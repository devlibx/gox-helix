package impl

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/devlibx/gox-base/v2"
)

// ChaosOperation represents a chaos operation type
type ChaosOperation int

const (
	ChaosOperationAddNode ChaosOperation = iota
	ChaosOperationRemoveNode
)

// ChaosEvent represents a single chaos event
type ChaosEvent struct {
	Operation ChaosOperation
	Timestamp time.Time
	Success   bool
	Error     error
}

// ChaosConfig holds configuration for chaos testing
type ChaosConfig struct {
	ChaosDuration      time.Duration // How long to run chaos (real time)
	OperationInterval  time.Duration // How often to perform operations (real time)
	AddProbability     float64       // Probability of adding vs removing nodes (0-1)
	MinNodesPerCluster int           // Minimum nodes to maintain per cluster
	MaxNodesPerCluster int           // Maximum nodes per cluster
}

// ChaosController manages chaos operations during testing
type ChaosController struct {
	config      ChaosConfig
	nodeManager *NodeManager
	cf          gox.CrossFunction
	
	// Statistics
	events       []ChaosEvent
	eventsMutex  sync.Mutex
	isRunning    bool
	runningMutex sync.RWMutex
}

// NewChaosController creates a new chaos controller
func NewChaosController(config ChaosConfig, nodeManager *NodeManager, cf gox.CrossFunction) *ChaosController {
	return &ChaosController{
		config:      config,
		nodeManager: nodeManager,
		cf:          cf,
		events:      make([]ChaosEvent, 0),
	}
}

// StartChaos begins the chaos phase with intensive node churn
func (cc *ChaosController) StartChaos(ctx context.Context, statusReporter *StatusReporter) error {
	cc.setRunning(true)
	defer cc.setRunning(false)
	
	fmt.Printf("🔥 Starting chaos phase for %v...\n", cc.config.ChaosDuration)
	fmt.Printf("⚡ Operations every %v (probability: %.1f%% add, %.1f%% remove)\n", 
		cc.config.OperationInterval, 
		cc.config.AddProbability*100, 
		(1-cc.config.AddProbability)*100)
	
	startTime := time.Now()
	ticker := time.NewTicker(cc.config.OperationInterval)
	defer ticker.Stop()
	
	// Track operation counts
	addCount := 0
	removeCount := 0
	failureCount := 0
	
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("🛑 Chaos phase cancelled\n")
			return ctx.Err()
			
		case <-ticker.C:
			// Check if chaos duration is complete
			if time.Since(startTime) >= cc.config.ChaosDuration {
				fmt.Printf("\n🏁 Chaos phase completed after %v\n", time.Since(startTime))
				cc.printChaosStatistics(addCount, removeCount, failureCount)
				return nil
			}
			
			// Perform chaos operation
			operation := cc.selectChaosOperation()
			event := cc.executeChaosOperation(ctx, operation)
			
			// Track statistics
			cc.recordEvent(event)
			if event.Success {
				switch operation {
				case ChaosOperationAddNode:
					addCount++
				case ChaosOperationRemoveNode:
					removeCount++
				}
			} else {
				failureCount++
			}
			
			// Print periodic progress
			elapsed := time.Since(startTime)
			if elapsed.Milliseconds()%10000 == 0 { // Every 10 seconds
				fmt.Printf("⏱️  Chaos progress: %v elapsed (add:%d, remove:%d, failures:%d)\n", 
					elapsed.Truncate(time.Second), addCount, removeCount, failureCount)
			}
		}
	}
}

// selectChaosOperation selects which chaos operation to perform
func (cc *ChaosController) selectChaosOperation() ChaosOperation {
	// Get current node statistics
	stats := cc.nodeManager.GetClusterStats()
	
	// Check if any cluster is at minimum or maximum
	hasMinNodes := false
	hasMaxNodes := false
	
	for _, count := range stats {
		if count <= cc.config.MinNodesPerCluster {
			hasMinNodes = true
		}
		if count >= cc.config.MaxNodesPerCluster {
			hasMaxNodes = true
		}
	}
	
	// Force add if any cluster is at minimum
	if hasMinNodes && !hasMaxNodes {
		return ChaosOperationAddNode
	}
	
	// Force remove if any cluster is at maximum
	if hasMaxNodes && !hasMinNodes {
		return ChaosOperationRemoveNode
	}
	
	// Random selection based on probability
	if rand.Float64() < cc.config.AddProbability {
		return ChaosOperationAddNode
	}
	return ChaosOperationRemoveNode
}

// executeChaosOperation executes a chaos operation and returns the result
func (cc *ChaosController) executeChaosOperation(ctx context.Context, operation ChaosOperation) ChaosEvent {
	event := ChaosEvent{
		Operation: operation,
		Timestamp: time.Now(),
	}
	
	switch operation {
	case ChaosOperationAddNode:
		event.Error = cc.nodeManager.AddRandomNode(ctx)
		event.Success = event.Error == nil
		
	case ChaosOperationRemoveNode:
		event.Error = cc.nodeManager.RemoveRandomNode(ctx)
		event.Success = event.Error == nil
		
	default:
		event.Error = fmt.Errorf("unknown chaos operation: %d", operation)
		event.Success = false
	}
	
	return event
}

// recordEvent records a chaos event for statistics
func (cc *ChaosController) recordEvent(event ChaosEvent) {
	cc.eventsMutex.Lock()
	defer cc.eventsMutex.Unlock()
	cc.events = append(cc.events, event)
}

// printChaosStatistics prints comprehensive chaos statistics
func (cc *ChaosController) printChaosStatistics(addCount, removeCount, failureCount int) {
	cc.eventsMutex.Lock()
	defer cc.eventsMutex.Unlock()
	
	fmt.Printf("\n📊 Chaos Phase Statistics:\n")
	fmt.Printf("═══════════════════════════\n")
	fmt.Printf("Total Operations: %d\n", len(cc.events))
	fmt.Printf("  - Node Additions: %d\n", addCount)
	fmt.Printf("  - Node Removals: %d\n", removeCount)
	fmt.Printf("  - Failures: %d\n", failureCount)
	
	if len(cc.events) > 0 {
		successRate := float64(addCount+removeCount) / float64(len(cc.events)) * 100
		fmt.Printf("Success Rate: %.1f%%\n", successRate)
		
		// Calculate operations per second
		duration := cc.events[len(cc.events)-1].Timestamp.Sub(cc.events[0].Timestamp)
		opsPerSecond := float64(len(cc.events)) / duration.Seconds()
		fmt.Printf("Operations per Second: %.2f\n", opsPerSecond)
	}
	
	// Print final node distribution
	fmt.Printf("\n🔢 Final Node Distribution:\n")
	stats := cc.nodeManager.GetClusterStats()
	totalNodes := 0
	for clusterName, count := range stats {
		fmt.Printf("  - %s: %d nodes\n", clusterName, count)
		totalNodes += count
	}
	fmt.Printf("  - Total: %d nodes\n", totalNodes)
	fmt.Printf("═══════════════════════════\n\n")
}

// IsRunning returns whether chaos is currently running
func (cc *ChaosController) IsRunning() bool {
	cc.runningMutex.RLock()
	defer cc.runningMutex.RUnlock()
	return cc.isRunning
}

// setRunning sets the running state thread-safely
func (cc *ChaosController) setRunning(running bool) {
	cc.runningMutex.Lock()
	defer cc.runningMutex.Unlock()
	cc.isRunning = running
}

// GetChaosEvents returns a copy of all chaos events for analysis
func (cc *ChaosController) GetChaosEvents() []ChaosEvent {
	cc.eventsMutex.Lock()
	defer cc.eventsMutex.Unlock()
	
	eventsCopy := make([]ChaosEvent, len(cc.events))
	copy(eventsCopy, cc.events)
	return eventsCopy
}

// StabilizationPhase runs the stabilization phase after chaos
func (cc *ChaosController) StabilizationPhase(ctx context.Context, duration time.Duration) error {
	fmt.Printf("🔄 Starting stabilization phase for %v...\n", duration)
	fmt.Printf("⏸️  No node changes during stabilization - letting clusters converge\n")
	
	startTime := time.Now()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	
	for {
		select {
		case <-ctx.Done():
			fmt.Printf("🛑 Stabilization phase cancelled\n")
			return ctx.Err()
			
		case <-ticker.C:
			elapsed := time.Since(startTime)
			if elapsed >= duration {
				fmt.Printf("✅ Stabilization phase completed after %v\n", elapsed)
				return nil
			}
			
			// Print progress every few seconds
			remaining := duration - elapsed
			fmt.Printf("⏳ Stabilization: %v remaining...\n", remaining.Truncate(time.Second))
		}
	}
}

// GetDefaultChaosConfig returns the default chaos configuration for the soak test
func GetDefaultChaosConfig() ChaosConfig {
	return ChaosConfig{
		ChaosDuration:      60 * time.Second,  // 1 minute of chaos
		OperationInterval:  2 * time.Second,   // Operation every 2s (0.5 ops/sec) - Much slower
		AddProbability:     0.8,               // 80% chance to add, 20% to remove - Favor adding
		MinNodesPerCluster: 15,                // Keep minimum 15 nodes per cluster - Higher minimum
		MaxNodesPerCluster: 80,                // Don't exceed 80 nodes per cluster
	}
}