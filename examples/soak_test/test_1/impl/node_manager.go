package impl

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/devlibx/gox-base/v2"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
)

// NodeInfo tracks information about a registered node
type NodeInfo struct {
	ID          string
	ClusterName string
	RegisteredAt time.Time
	Active      bool
}

// ClusterNodes tracks all nodes for a specific cluster
type ClusterNodes struct {
	clusterManager managment.ClusterManager
	nodes          map[string]*NodeInfo
	mutex          sync.RWMutex
}

// NodeManager manages node lifecycle across multiple clusters
type NodeManager struct {
	clusters map[string]*ClusterNodes
	mutex    sync.RWMutex
	cf       gox.CrossFunction
}

// NewNodeManager creates a new node manager
func NewNodeManager(cf gox.CrossFunction) *NodeManager {
	return &NodeManager{
		clusters: make(map[string]*ClusterNodes),
		cf:       cf,
	}
}

// RegisterCluster registers a cluster manager for node operations
func (nm *NodeManager) RegisterCluster(clusterName string, clusterManager managment.ClusterManager) {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()
	
	nm.clusters[clusterName] = &ClusterNodes{
		clusterManager: clusterManager,
		nodes:         make(map[string]*NodeInfo),
	}
}

// RegisterInitialNodes registers the specified number of initial nodes for all clusters
func (nm *NodeManager) RegisterInitialNodes(ctx context.Context, nodeCount int) error {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	
	fmt.Printf("🚀 Registering %d initial nodes per cluster...\n", nodeCount)
	
	for clusterName, clusterNodes := range nm.clusters {
		if err := nm.registerNodesForCluster(ctx, clusterName, clusterNodes, nodeCount); err != nil {
			return fmt.Errorf("failed to register initial nodes for cluster %s: %w", clusterName, err)
		}
		fmt.Printf("✅ Registered %d nodes for cluster: %s\n", nodeCount, clusterName)
	}
	
	return nil
}

// registerNodesForCluster registers nodes for a specific cluster
func (nm *NodeManager) registerNodesForCluster(ctx context.Context, clusterName string, clusterNodes *ClusterNodes, count int) error {
	clusterNodes.mutex.Lock()
	defer clusterNodes.mutex.Unlock()
	
	for i := 0; i < count; i++ {
		request := managment.NodeRegisterRequest{
			Cluster: clusterName,
		}
		
		response, err := clusterNodes.clusterManager.RegisterNode(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to register node %d: %w", i, err)
		}
		
		nodeInfo := &NodeInfo{
			ID:          response.NodeId,
			ClusterName: clusterName,
			RegisteredAt: time.Now(),
			Active:      true,
		}
		
		clusterNodes.nodes[response.NodeId] = nodeInfo
	}
	
	return nil
}

// AddRandomNode adds a random node to a randomly selected cluster
func (nm *NodeManager) AddRandomNode(ctx context.Context) error {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	
	if len(nm.clusters) == 0 {
		return fmt.Errorf("no clusters registered")
	}
	
	// Select random cluster
	clusterNames := make([]string, 0, len(nm.clusters))
	for name := range nm.clusters {
		clusterNames = append(clusterNames, name)
	}
	selectedCluster := clusterNames[rand.Intn(len(clusterNames))]
	clusterNodes := nm.clusters[selectedCluster]
	
	// Register new node
	request := managment.NodeRegisterRequest{
		Cluster: selectedCluster,
	}
	
	response, err := clusterNodes.clusterManager.RegisterNode(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to add random node to cluster %s: %w", selectedCluster, err)
	}
	
	// Track the new node
	clusterNodes.mutex.Lock()
	defer clusterNodes.mutex.Unlock()
	
	nodeInfo := &NodeInfo{
		ID:          response.NodeId,
		ClusterName: selectedCluster,
		RegisteredAt: time.Now(),
		Active:      true,
	}
	
	clusterNodes.nodes[response.NodeId] = nodeInfo
	
	fmt.Printf("➕ Added node %s to cluster %s (total: %d)\n", 
		response.NodeId[:8], selectedCluster, len(clusterNodes.nodes))
	
	return nil
}

// RemoveRandomNode removes a random active node from a randomly selected cluster
func (nm *NodeManager) RemoveRandomNode(ctx context.Context) error {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	
	if len(nm.clusters) == 0 {
		return fmt.Errorf("no clusters registered")
	}
	
	// Find clusters with active nodes
	clustersWithNodes := make([]string, 0)
	for clusterName, clusterNodes := range nm.clusters {
		clusterNodes.mutex.RLock()
		activeCount := nm.countActiveNodes(clusterNodes.nodes)
		clusterNodes.mutex.RUnlock()
		
		// Keep minimum of 5 nodes per cluster to avoid complete emptiness
		if activeCount > 5 {
			clustersWithNodes = append(clustersWithNodes, clusterName)
		}
	}
	
	if len(clustersWithNodes) == 0 {
		return fmt.Errorf("no clusters have removable nodes (keeping minimum 5 per cluster)")
	}
	
	// Select random cluster with removable nodes
	selectedCluster := clustersWithNodes[rand.Intn(len(clustersWithNodes))]
	clusterNodes := nm.clusters[selectedCluster]
	
	clusterNodes.mutex.Lock()
	defer clusterNodes.mutex.Unlock()
	
	// Find random active node
	activeNodes := make([]*NodeInfo, 0)
	for _, node := range clusterNodes.nodes {
		if node.Active {
			activeNodes = append(activeNodes, node)
		}
	}
	
	if len(activeNodes) == 0 {
		return fmt.Errorf("no active nodes to remove from cluster %s", selectedCluster)
	}
	
	// Select random node to remove
	nodeToRemove := activeNodes[rand.Intn(len(activeNodes))]
	
	// Deregister the node (mark as inactive in database)
	// Note: We simulate node removal by stopping the node's heartbeat
	// The cluster manager's background process will mark it inactive
	nodeToRemove.Active = false
	
	fmt.Printf("➖ Removed node %s from cluster %s (remaining: %d)\n", 
		nodeToRemove.ID[:8], selectedCluster, nm.countActiveNodes(clusterNodes.nodes)-1)
	
	return nil
}

// GetClusterStats returns statistics for all clusters
func (nm *NodeManager) GetClusterStats() map[string]int {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	
	stats := make(map[string]int)
	for clusterName, clusterNodes := range nm.clusters {
		clusterNodes.mutex.RLock()
		stats[clusterName] = nm.countActiveNodes(clusterNodes.nodes)
		clusterNodes.mutex.RUnlock()
	}
	
	return stats
}

// countActiveNodes counts active nodes in a node map (must hold mutex)
func (nm *NodeManager) countActiveNodes(nodes map[string]*NodeInfo) int {
	count := 0
	for _, node := range nodes {
		if node.Active {
			count++
		}
	}
	return count
}

// GetTotalNodes returns total active nodes across all clusters
func (nm *NodeManager) GetTotalNodes() int {
	stats := nm.GetClusterStats()
	total := 0
	for _, count := range stats {
		total += count
	}
	return total
}

// PrintNodeStats prints current node statistics
func (nm *NodeManager) PrintNodeStats() {
	stats := nm.GetClusterStats()
	total := 0
	
	fmt.Printf("🔢 Node Statistics:\n")
	for clusterName, count := range stats {
		fmt.Printf("  - %s: %d nodes\n", clusterName, count)
		total += count
	}
	fmt.Printf("  - Total: %d nodes\n", total)
}

// CleanupInactiveNodes removes inactive node records (for cleanup)
func (nm *NodeManager) CleanupInactiveNodes() {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()
	
	for clusterName, clusterNodes := range nm.clusters {
		clusterNodes.mutex.Lock()
		for nodeID, node := range clusterNodes.nodes {
			if !node.Active {
				delete(clusterNodes.nodes, nodeID)
			}
		}
		clusterNodes.mutex.Unlock()
		
		fmt.Printf("🧹 Cleaned up inactive nodes for cluster: %s\n", clusterName)
	}
}

// Shutdown gracefully shuts down all node operations
func (nm *NodeManager) Shutdown() {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()
	
	fmt.Printf("🛑 Shutting down node manager...\n")
	
	for clusterName, clusterNodes := range nm.clusters {
		clusterNodes.mutex.Lock()
		
		// Mark all nodes as inactive
		for _, node := range clusterNodes.nodes {
			node.Active = false
		}
		
		clusterNodes.mutex.Unlock()
		fmt.Printf("✅ Shutdown nodes for cluster: %s\n", clusterName)
	}
}