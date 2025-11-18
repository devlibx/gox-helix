package coordinator

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/devlibx/gox-base/v2"
	helixDomainMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/domain/database"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

// chaosTestConfig defines the parameters for chaos testing
type chaosTestConfig struct {
	partitionCount     int
	initialWorkers     int
	maxWorkers         int
	minWorkers         int
	phase1Duration     time.Duration
	phase2Duration     time.Duration
	distributeInterval time.Duration
}

// chaosSimulator manages worker churn and validates distributions
type chaosSimulator struct {
	config               chaosTestConfig
	activeWorkers        []string
	workerPool           []string
	currentDistribution  *DistributionResponse
	previousDistribution *DistributionResponse
	iterationCount       int
	phase1Complete       bool
	stableWorkers        []string // For mixed stability pattern

	// Validation tracking
	partitionOwnership map[int]string
	duplicateDetected  bool
	duplicateDetails   string
	t                  *testing.T
}

func newChaosSimulator(t *testing.T, config chaosTestConfig) *chaosSimulator {
	// Create worker pool (worker-1 to worker-50)
	workerPool := make([]string, 50)
	for i := 0; i < 50; i++ {
		workerPool[i] = fmt.Sprintf("worker-%d", i+1)
	}

	// Initialize with initial workers
	activeWorkers := make([]string, config.initialWorkers)
	copy(activeWorkers, workerPool[:config.initialWorkers])

	return &chaosSimulator{
		config:             config,
		activeWorkers:      activeWorkers,
		workerPool:         workerPool,
		partitionOwnership: make(map[int]string),
		t:                  t,
	}
}

func (s *chaosSimulator) getActiveWorkers() []string {
	return append([]string{}, s.activeWorkers...)
}

func (s *chaosSimulator) getPreviousPartitionMappings() []WorkerPartitionMapping {
	if s.previousDistribution == nil {
		return []WorkerPartitionMapping{}
	}

	// Convert previous distribution to WorkerPartitionMapping format
	workerMappings := make(map[string]map[int]DistributionMapping)
	for partitionId, mapping := range s.previousDistribution.Mapping {
		if _, ok := workerMappings[mapping.OwnerId]; !ok {
			workerMappings[mapping.OwnerId] = make(map[int]DistributionMapping)
		}
		workerMappings[mapping.OwnerId][partitionId] = mapping
	}

	result := make([]WorkerPartitionMapping, 0, len(workerMappings))
	for ownerId, mappings := range workerMappings {
		result = append(result, WorkerPartitionMapping{
			OwnerID: ownerId,
			Mapping: mappings,
		})
	}

	return result
}

func (s *chaosSimulator) addWorkers(count int) {
	if len(s.activeWorkers) >= s.config.maxWorkers {
		return
	}

	// Find workers not currently active
	activeSet := make(map[string]bool)
	for _, w := range s.activeWorkers {
		activeSet[w] = true
	}

	// Add up to 'count' new workers
	added := 0
	for _, w := range s.workerPool {
		if !activeSet[w] && added < count && len(s.activeWorkers) < s.config.maxWorkers {
			s.activeWorkers = append(s.activeWorkers, w)
			added++
		}
	}
}

func (s *chaosSimulator) removeWorkers(count int, excludeStable bool) {
	if len(s.activeWorkers) <= s.config.minWorkers {
		return
	}

	// Build stable worker set for exclusion
	stableSet := make(map[string]bool)
	if excludeStable {
		for _, w := range s.stableWorkers {
			stableSet[w] = true
		}
	}

	// Remove up to 'count' workers (excluding stable workers if needed)
	removed := 0
	newActive := make([]string, 0, len(s.activeWorkers))
	for _, w := range s.activeWorkers {
		shouldKeep := removed >= count || len(s.activeWorkers)-removed <= s.config.minWorkers
		if excludeStable && stableSet[w] {
			shouldKeep = true
		}

		if shouldKeep {
			newActive = append(newActive, w)
		} else {
			removed++
		}
	}
	s.activeWorkers = newActive
}

func (s *chaosSimulator) freezeWorkers() {
	s.phase1Complete = true
}

func (s *chaosSimulator) updateDistribution(response *DistributionResponse) {
	s.previousDistribution = s.currentDistribution
	s.currentDistribution = response
	s.iterationCount++
}

func (s *chaosSimulator) validateNoDuplicates(response *DistributionResponse) {
	if s.duplicateDetected {
		return // Already detected, don't spam
	}

	// Build ownership map
	ownership := make(map[int][]string)
	for partitionId, mapping := range response.Mapping {
		ownership[partitionId] = append(ownership[partitionId], mapping.OwnerId)
	}

	// Check for duplicates
	for partitionId, owners := range ownership {
		if len(owners) > 1 {
			s.duplicateDetected = true
			s.duplicateDetails = fmt.Sprintf("Partition %d assigned to multiple workers: %v at iteration %d",
				partitionId, owners, s.iterationCount)
			assert.Fail(s.t, "Duplicate partition assignment detected", s.duplicateDetails)
			return
		}
	}

	// Update current ownership
	for partitionId, mapping := range response.Mapping {
		s.partitionOwnership[partitionId] = mapping.OwnerId
	}
}

func (s *chaosSimulator) validateAllPartitionsAssigned() {
	require.NotNil(s.t, s.currentDistribution, "No distribution available")

	for i := 0; i < s.config.partitionCount; i++ {
		mapping, exists := s.currentDistribution.Mapping[i]
		require.True(s.t, exists, "Partition %d is not assigned", i)
		require.NotEmpty(s.t, mapping.OwnerId, "Partition %d has empty OwnerId", i)
		require.Equal(s.t, databaseCommon.PartitionAssignmentStatusAssigned, mapping.Status,
			"Partition %d is not in ASSIGNED status", i)
	}
}

func (s *chaosSimulator) validateFairDistribution() {
	require.NotNil(s.t, s.currentDistribution, "No distribution available")

	// Count partitions per worker
	workerCounts := make(map[string]int)
	for _, mapping := range s.currentDistribution.Mapping {
		workerCounts[mapping.OwnerId]++
	}

	// Calculate expected distribution
	activeWorkerCount := len(s.activeWorkers)
	base := s.config.partitionCount / activeWorkerCount
	remainder := s.config.partitionCount % activeWorkerCount

	// Count workers with base+1 and base partitions
	workersWithBasePlus1 := 0
	workersWithBase := 0

	for _, count := range workerCounts {
		if count == base+1 {
			workersWithBasePlus1++
		} else if count == base {
			workersWithBase++
		} else {
			assert.Fail(s.t, "Unfair distribution",
				"Worker has %d partitions, expected %d or %d (base=%d, remainder=%d, workers=%d)",
				count, base, base+1, base, remainder, activeWorkerCount)
		}
	}

	// Verify exact mathematical fairness
	assert.Equal(s.t, remainder, workersWithBasePlus1,
		"Expected exactly %d workers with %d partitions, got %d", remainder, base+1, workersWithBasePlus1)
	assert.Equal(s.t, activeWorkerCount-remainder, workersWithBase,
		"Expected exactly %d workers with %d partitions, got %d", activeWorkerCount-remainder, base, workersWithBase)
}

func (s *chaosSimulator) setStableWorkers(workers []string) {
	s.stableWorkers = workers
}

// Test 1: Random Churn Pattern
// 100 partitions, 5-10 workers, random add/remove
func TestDistributeChaos_RandomChurn(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := chaosTestConfig{
		partitionCount:     100,
		initialWorkers:     5,
		maxWorkers:         10,
		minWorkers:         5,
		phase1Duration:     5 * time.Second,
		phase2Duration:     2 * time.Second,
		distributeInterval: 100 * time.Millisecond,
	}

	simulator := newChaosSimulator(t, config)
	mockWorkerService := NewMockWorkerService(ctrl)
	mockPartitionService := NewMockPartitionService(ctrl)
	mockDomainService := NewMockDomainService(ctrl)

	// Setup dynamic mocks
	mockDomainService.EXPECT().GetTaskListInfo(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&helixDomainMysql.HelixDomain{PartitionCount: uint32(config.partitionCount)}, nil).
		AnyTimes()

	mockWorkerService.EXPECT().GetActiveWorkers(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, domain string) ([]string, error) {
			return simulator.getActiveWorkers(), nil
		}).AnyTimes()

	mockPartitionService.EXPECT().GetActivePartitionMappings(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, domain, taskList string) ([]WorkerPartitionMapping, error) {
			return simulator.getPreviousPartitionMappings(), nil
		}).AnyTimes()

	distributor := distributorStrategyV1Impl{
		CrossFunction: gox.NewCrossFunction(),
		ws:            mockWorkerService,
		ps:            mockPartitionService,
		ds:            mockDomainService,
	}

	// Run chaos test
	ctx := context.Background()
	request := DistributionRequest{
		DomainName: "test-domain",
		TaskList:   "test-tasklist",
	}

	ticker := time.NewTicker(config.distributeInterval)
	defer ticker.Stop()

	phase1Timer := time.NewTimer(config.phase1Duration)
	phase2Timer := time.NewTimer(config.phase1Duration + config.phase2Duration)
	defer phase1Timer.Stop()
	defer phase2Timer.Stop()

	churnTicker := time.NewTicker(400 * time.Millisecond)
	defer churnTicker.Stop()

	rand.Seed(time.Now().UnixNano())

	for {
		select {
		case <-ticker.C:
			// Call Distribute
			response, err := distributor.Distribute(ctx, request)
			require.NoError(t, err)

			// Validate no duplicates immediately
			simulator.validateNoDuplicates(response)

			// Update state
			simulator.updateDistribution(response)

		case <-churnTicker.C:
			if !simulator.phase1Complete {
				// Random churn: add or remove workers
				if rand.Float32() < 0.5 {
					simulator.addWorkers(rand.Intn(2) + 1) // Add 1-2 workers
				} else {
					simulator.removeWorkers(rand.Intn(2)+1, false) // Remove 1-2 workers
				}
			}

		case <-phase1Timer.C:
			t.Logf("Phase 1 complete. Freezing workers at: %v", simulator.getActiveWorkers())
			simulator.freezeWorkers()

		case <-phase2Timer.C:
			t.Logf("Phase 2 complete. Total iterations: %d", simulator.iterationCount)
			// Final validations
			simulator.validateAllPartitionsAssigned()
			simulator.validateFairDistribution()
			require.False(t, simulator.duplicateDetected, "Duplicate detected: %s", simulator.duplicateDetails)
			return
		}
	}
}

// Test 2: Structured Scenario Pattern
// 50 partitions, 3-8 workers, predefined lifecycle
func TestDistributeChaos_StructuredScenario(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := chaosTestConfig{
		partitionCount:     50,
		initialWorkers:     3,
		maxWorkers:         8,
		minWorkers:         3,
		phase1Duration:     5 * time.Second,
		phase2Duration:     2 * time.Second,
		distributeInterval: 100 * time.Millisecond,
	}

	simulator := newChaosSimulator(t, config)
	mockWorkerService := NewMockWorkerService(ctrl)
	mockPartitionService := NewMockPartitionService(ctrl)
	mockDomainService := NewMockDomainService(ctrl)

	// Setup dynamic mocks
	mockDomainService.EXPECT().GetTaskListInfo(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&helixDomainMysql.HelixDomain{PartitionCount: uint32(config.partitionCount)}, nil).
		AnyTimes()

	mockWorkerService.EXPECT().GetActiveWorkers(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, domain string) ([]string, error) {
			return simulator.getActiveWorkers(), nil
		}).AnyTimes()

	mockPartitionService.EXPECT().GetActivePartitionMappings(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, domain, taskList string) ([]WorkerPartitionMapping, error) {
			return simulator.getPreviousPartitionMappings(), nil
		}).AnyTimes()

	distributor := distributorStrategyV1Impl{
		CrossFunction: gox.NewCrossFunction(),
		ws:            mockWorkerService,
		ps:            mockPartitionService,
		ds:            mockDomainService,
	}

	// Structured scenario timeline
	scenarioTimers := []*time.Timer{
		time.NewTimer(1 * time.Second),  // Add 5 workers → 8 total
		time.NewTimer(2 * time.Second),  // Remove 3 workers → 5 total
		time.NewTimer(3 * time.Second),  // Add 2 workers → 7 total
		time.NewTimer(4 * time.Second),  // Remove 4 workers → 3 total
		time.NewTimer(5 * time.Second),  // Freeze (Phase 1 end)
		time.NewTimer(7 * time.Second),  // Test complete (Phase 2 end)
	}
	defer func() {
		for _, timer := range scenarioTimers {
			timer.Stop()
		}
	}()

	ctx := context.Background()
	request := DistributionRequest{
		DomainName: "test-domain",
		TaskList:   "test-tasklist",
	}

	ticker := time.NewTicker(config.distributeInterval)
	defer ticker.Stop()

	scenarioStep := 0

	for {
		select {
		case <-ticker.C:
			// Call Distribute
			response, err := distributor.Distribute(ctx, request)
			require.NoError(t, err)

			// Validate no duplicates immediately
			simulator.validateNoDuplicates(response)

			// Update state
			simulator.updateDistribution(response)

		case <-scenarioTimers[scenarioStep].C:
			switch scenarioStep {
			case 0: // 1s: Add 5 workers → 8 total
				simulator.addWorkers(5)
				t.Logf("1s: Added 5 workers, total: %d", len(simulator.getActiveWorkers()))
			case 1: // 2s: Remove 3 workers → 5 total
				simulator.removeWorkers(3, false)
				t.Logf("2s: Removed 3 workers, total: %d", len(simulator.getActiveWorkers()))
			case 2: // 3s: Add 2 workers → 7 total
				simulator.addWorkers(2)
				t.Logf("3s: Added 2 workers, total: %d", len(simulator.getActiveWorkers()))
			case 3: // 4s: Remove 4 workers → 3 total
				simulator.removeWorkers(4, false)
				t.Logf("4s: Removed 4 workers, total: %d", len(simulator.getActiveWorkers()))
			case 4: // 5s: Freeze
				simulator.freezeWorkers()
				t.Logf("5s: Phase 1 complete. Frozen at: %v", simulator.getActiveWorkers())
			case 5: // 7s: Complete
				t.Logf("7s: Phase 2 complete. Total iterations: %d", simulator.iterationCount)
				// Final validations
				simulator.validateAllPartitionsAssigned()
				simulator.validateFairDistribution()
				require.False(t, simulator.duplicateDetected, "Duplicate detected: %s", simulator.duplicateDetails)
				return
			}
			scenarioStep++
		}
	}
}

// Test 3: Mixed Stability Pattern
// 105 partitions, 10-15 workers, stable base + transient workers
func TestDistributeChaos_MixedStability(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping chaos test in short mode")
	}

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := chaosTestConfig{
		partitionCount:     105,
		initialWorkers:     10,
		maxWorkers:         15,
		minWorkers:         10,
		phase1Duration:     5 * time.Second,
		phase2Duration:     2 * time.Second,
		distributeInterval: 100 * time.Millisecond,
	}

	simulator := newChaosSimulator(t, config)

	// Set first 10 workers as stable (never removed)
	stableWorkers := simulator.getActiveWorkers()
	simulator.setStableWorkers(stableWorkers)

	mockWorkerService := NewMockWorkerService(ctrl)
	mockPartitionService := NewMockPartitionService(ctrl)
	mockDomainService := NewMockDomainService(ctrl)

	// Setup dynamic mocks
	mockDomainService.EXPECT().GetTaskListInfo(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(&helixDomainMysql.HelixDomain{PartitionCount: uint32(config.partitionCount)}, nil).
		AnyTimes()

	mockWorkerService.EXPECT().GetActiveWorkers(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, domain string) ([]string, error) {
			return simulator.getActiveWorkers(), nil
		}).AnyTimes()

	mockPartitionService.EXPECT().GetActivePartitionMappings(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, domain, taskList string) ([]WorkerPartitionMapping, error) {
			return simulator.getPreviousPartitionMappings(), nil
		}).AnyTimes()

	distributor := distributorStrategyV1Impl{
		CrossFunction: gox.NewCrossFunction(),
		ws:            mockWorkerService,
		ps:            mockPartitionService,
		ds:            mockDomainService,
	}

	ctx := context.Background()
	request := DistributionRequest{
		DomainName: "test-domain",
		TaskList:   "test-tasklist",
	}

	ticker := time.NewTicker(config.distributeInterval)
	defer ticker.Stop()

	phase1Timer := time.NewTimer(config.phase1Duration)
	phase2Timer := time.NewTimer(config.phase1Duration + config.phase2Duration)
	defer phase1Timer.Stop()
	defer phase2Timer.Stop()

	churnTicker := time.NewTicker(300 * time.Millisecond)
	defer churnTicker.Stop()

	rand.Seed(time.Now().UnixNano())

	for {
		select {
		case <-ticker.C:
			// Call Distribute
			response, err := distributor.Distribute(ctx, request)
			require.NoError(t, err)

			// Validate no duplicates immediately
			simulator.validateNoDuplicates(response)

			// Update state
			simulator.updateDistribution(response)

		case <-churnTicker.C:
			if !simulator.phase1Complete {
				// Mixed churn: add or remove transient workers (never remove stable workers)
				if rand.Float32() < 0.5 {
					simulator.addWorkers(rand.Intn(3) + 1) // Add 1-3 workers
				} else {
					simulator.removeWorkers(rand.Intn(3)+1, true) // Remove 1-3 transient workers
				}
			}

		case <-phase1Timer.C:
			t.Logf("Phase 1 complete. Freezing workers at: %v", simulator.getActiveWorkers())
			simulator.freezeWorkers()

		case <-phase2Timer.C:
			t.Logf("Phase 2 complete. Total iterations: %d", simulator.iterationCount)
			// Final validations
			simulator.validateAllPartitionsAssigned()
			simulator.validateFairDistribution()
			require.False(t, simulator.duplicateDetected, "Duplicate detected: %s", simulator.duplicateDetails)
			return
		}
	}
}
