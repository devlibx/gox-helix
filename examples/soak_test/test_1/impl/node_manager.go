package impl

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"github.com/devlibx/gox-base/v2"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
)

// ClusterInfo holds cluster manager and database connection
type ClusterInfo struct {
	clusterManager managment.ClusterManager
	clusterName    string
}

// NodeManager manages node lifecycle across multiple clusters using database as source of truth
type NodeManager struct {
	clusters map[string]*ClusterInfo
	mutex    sync.RWMutex
	cf       gox.CrossFunction
	db       *helixClusterMysql.Queries // Database connection for querying active nodes
}

// NewNodeManager creates a new node manager
func NewNodeManager(cf gox.CrossFunction, db *helixClusterMysql.Queries) *NodeManager {
	return &NodeManager{
		clusters: make(map[string]*ClusterInfo),
		cf:       cf,
		db:       db,
	}
}

// RegisterCluster registers a cluster manager for node operations
func (nm *NodeManager) RegisterCluster(clusterName string, clusterManager managment.ClusterManager) {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()
	
	nm.clusters[clusterName] = &ClusterInfo{
		clusterManager: clusterManager,
		clusterName:    clusterName,
	}
}

// RegisterInitialNodes registers the specified number of initial nodes for all clusters
func (nm *NodeManager) RegisterInitialNodes(ctx context.Context, nodeCount int) error {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	
	fmt.Printf("🚀 Registering %d initial nodes per cluster...\n", nodeCount)
	
	for clusterName, clusterInfo := range nm.clusters {
		if err := nm.registerNodesForCluster(ctx, clusterName, clusterInfo, nodeCount); err != nil {
			return fmt.Errorf("failed to register initial nodes for cluster %s: %w", clusterName, err)
		}
		fmt.Printf("✅ Registered %d nodes for cluster: %s\n", nodeCount, clusterName)
	}
	
	return nil
}

// registerNodesForCluster registers nodes for a specific cluster
func (nm *NodeManager) registerNodesForCluster(ctx context.Context, clusterName string, clusterInfo *ClusterInfo, count int) error {
	for i := 0; i < count; i++ {
		request := managment.NodeRegisterRequest{
			Cluster: clusterName,
		}
		
		_, err := clusterInfo.clusterManager.RegisterNode(ctx, request)
		if err != nil {
			return fmt.Errorf("failed to register node %d: %w", i, err)
		}
		// No local tracking - database is the source of truth
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
	clusterInfo := nm.clusters[selectedCluster]
	
	// Register new node
	request := managment.NodeRegisterRequest{
		Cluster: selectedCluster,
	}
	
	response, err := clusterInfo.clusterManager.RegisterNode(ctx, request)
	if err != nil {
		return fmt.Errorf("failed to add random node to cluster %s: %w", selectedCluster, err)
	}
	
	// Query current active node count from database for logging
	activeNodes, err := nm.db.GetActiveNodes(ctx, selectedCluster)
	if err != nil {
		fmt.Printf("➕ Added node %s to cluster %s (count query failed: %v)\n", 
			response.NodeId[:8], selectedCluster, err)
	} else {
		fmt.Printf("➕ Added node %s to cluster %s (total: %d)\n", 
			response.NodeId[:8], selectedCluster, len(activeNodes))
	}
	
	return nil
}

// RemoveRandomNode removes a random active node from a randomly selected cluster
func (nm *NodeManager) RemoveRandomNode(ctx context.Context) error {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	
	if len(nm.clusters) == 0 {
		return fmt.Errorf("no clusters registered")
	}
	
	// Find clusters with active nodes by querying database
	clustersWithNodes := make([]string, 0)
	for clusterName := range nm.clusters {
		activeNodes, err := nm.db.GetActiveNodes(ctx, clusterName)
		if err != nil {
			continue // Skip cluster if can't query active nodes
		}
		
		// Keep minimum of 5 nodes per cluster to avoid complete emptiness
		if len(activeNodes) > 5 {
			clustersWithNodes = append(clustersWithNodes, clusterName)
		}
	}
	
	if len(clustersWithNodes) == 0 {
		return fmt.Errorf("no clusters have removable nodes (keeping minimum 5 per cluster)")
	}
	
	// Select random cluster with removable nodes
	selectedCluster := clustersWithNodes[rand.Intn(len(clustersWithNodes))]
	
	// Get active nodes from database
	activeNodes, err := nm.db.GetActiveNodes(ctx, selectedCluster)
	if err != nil {
		return fmt.Errorf("failed to get active nodes for cluster %s: %w", selectedCluster, err)
	}
	
	if len(activeNodes) == 0 {
		return fmt.Errorf("no active nodes to remove from cluster %s", selectedCluster)
	}
	
	// Select random node to remove
	nodeToRemove := activeNodes[rand.Intn(len(activeNodes))]
	
	// Deregister the node directly via database (marks inactive)
	err = nm.db.DeregisterNode(ctx, helixClusterMysql.DeregisterNodeParams{
		ClusterName: selectedCluster,
		NodeUuid:    nodeToRemove.NodeUuid,
	})
	if err != nil {
		return fmt.Errorf("failed to deregister node %s from cluster %s: %w", nodeToRemove.NodeUuid[:8], selectedCluster, err)
	}
	
	fmt.Printf("➖ Removed node %s from cluster %s (remaining: %d)\n", 
		nodeToRemove.NodeUuid[:8], selectedCluster, len(activeNodes)-1)
	
	return nil
}

// GetClusterStats returns statistics for all clusters by querying database
func (nm *NodeManager) GetClusterStats() map[string]int {
	nm.mutex.RLock()
	defer nm.mutex.RUnlock()
	
	stats := make(map[string]int)
	for clusterName := range nm.clusters {
		// Query database for active nodes count
		activeNodes, err := nm.db.GetActiveNodes(context.Background(), clusterName)
		if err != nil {
			stats[clusterName] = 0 // Default to 0 if query fails
		} else {
			stats[clusterName] = len(activeNodes)
		}
	}
	
	return stats
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

// CleanupInactiveNodes is no longer needed since we don't maintain local state
// Database cleanup is handled by cluster manager background processes
func (nm *NodeManager) CleanupInactiveNodes() {
	fmt.Printf("🧹 Cleanup inactive nodes - delegated to cluster manager background processes\n")
}

// Shutdown gracefully shuts down all node operations
func (nm *NodeManager) Shutdown() {
	nm.mutex.Lock()
	defer nm.mutex.Unlock()
	
	fmt.Printf("🛑 Shutting down node manager...\n")
	
	// No local state to clean up - cluster managers handle node lifecycle
	for clusterName := range nm.clusters {
		fmt.Printf("✅ Shutdown nodes for cluster: %s\n", clusterName)
	}
}