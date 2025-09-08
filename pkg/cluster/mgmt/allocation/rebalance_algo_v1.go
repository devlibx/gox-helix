package allocation

import (
	"sort"

	"github.com/devlibx/gox-base/v2"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
)

// NodeAllocationState tracks comprehensive state for all nodes during rebalancing
type NodeAllocationState struct {
	// Basic configuration
	ActiveNodeIds   []string
	TargetPerNode   int
	ExtraPartitions int

	// Current partition distribution
	NodeCurrentCount     map[string]int
	NodePartitions       map[string][]*nodePartitionMapping
	UnassignedPartitions []*nodePartitionMapping

	// Node categories for different rebalancing strategies
	NewNodes            []string // nodes with 0 partitions (likely new nodes)
	OverAllocatedNodes  []string // nodes exceeding target
	UnderAllocatedNodes []string // nodes below target
}

// RebalanceAlgoV1 implements a clean, step-by-step partition rebalancing algorithm
type RebalanceAlgoV1 struct {
	gox.CrossFunction
	AlgorithmConfig *managment.AlgorithmConfig
}

// NewRebalanceAlgoV1 creates a new instance of the v1 rebalancing algorithm
func NewRebalanceAlgoV1(cf gox.CrossFunction, config *managment.AlgorithmConfig) *RebalanceAlgoV1 {
	return &RebalanceAlgoV1{
		CrossFunction:   cf,
		AlgorithmConfig: config,
	}
}

// DistributeWork orchestrates the 4-step rebalancing algorithm
func (r *RebalanceAlgoV1) DistributeWork(
	taskListInfo managment.TaskListInfo,
	nodePartitionMappings map[string]*nodePartitionMapping,
	nodes []*helixClusterMysql.GetActiveNodesRow,
) map[string]*nodePartitionMapping {

	// Step 1: Build comprehensive state analysis
	state := r.buildAllocationState(taskListInfo, nodePartitionMappings, nodes)

	// Step 2: Execute graceful migration if needed (two-phase)
	r.executeGracefulMigration(nodePartitionMappings, state)

	// Step 3: Assign unassigned partitions to lowest-count nodes
	r.assignUnassignedPartitions(nodePartitionMappings, state)

	// Step 4: Balance remaining imbalances by moving active partitions
	r.balanceRemainingImbalances(nodePartitionMappings, state)

	return nodePartitionMappings
}

// buildAllocationState analyzes current partition distribution and creates comprehensive state
func (r *RebalanceAlgoV1) buildAllocationState(
	taskListInfo managment.TaskListInfo,
	nodePartitionMappings map[string]*nodePartitionMapping,
	nodes []*helixClusterMysql.GetActiveNodesRow,
) *NodeAllocationState {

	// Get active node IDs
	activeNodeIds := make([]string, 0)
	for _, node := range nodes {
		if node.Status == helixClusterMysql.NodeStatusActive {
			activeNodeIds = append(activeNodeIds, node.NodeUuid)
		}
	}
	sort.Strings(activeNodeIds) // Ensure consistent ordering

	if len(activeNodeIds) == 0 {
		return &NodeAllocationState{} // No active nodes, return empty state
	}

	// Calculate target distribution
	targetPerNode := taskListInfo.PartitionCount / len(activeNodeIds)
	extraPartitions := taskListInfo.PartitionCount % len(activeNodeIds)

	// Initialize state tracking
	state := &NodeAllocationState{
		ActiveNodeIds:        activeNodeIds,
		TargetPerNode:        targetPerNode,
		ExtraPartitions:      extraPartitions,
		NodeCurrentCount:     make(map[string]int),
		NodePartitions:       make(map[string][]*nodePartitionMapping),
		UnassignedPartitions: make([]*nodePartitionMapping, 0),
		NewNodes:             make([]string, 0),
		OverAllocatedNodes:   make([]string, 0),
		UnderAllocatedNodes:  make([]string, 0),
	}

	// Initialize maps for all active nodes
	for _, nodeId := range activeNodeIds {
		state.NodePartitions[nodeId] = make([]*nodePartitionMapping, 0)
		state.NodeCurrentCount[nodeId] = 0
	}

	// Categorize partitions by status
	for _, partition := range nodePartitionMappings {
		switch partition.Status {
		case managment.PartitionAllocationAssigned:
			// Only count if node is active
			if _, exists := state.NodeCurrentCount[partition.OwnerNode]; exists {
				state.NodePartitions[partition.OwnerNode] = append(state.NodePartitions[partition.OwnerNode], partition)
				state.NodeCurrentCount[partition.OwnerNode]++
			} else {
				// Node is inactive, treat partition as unassigned
				partition.Status = managment.PartitionAllocationUnassigned
				partition.OwnerNode = ""
				state.UnassignedPartitions = append(state.UnassignedPartitions, partition)
			}

		case managment.PartitionAllocationPlaceholder:
			// Placeholders count toward active load for rebalancing calculations
			if _, exists := state.NodeCurrentCount[partition.OwnerNode]; exists {
				state.NodePartitions[partition.OwnerNode] = append(state.NodePartitions[partition.OwnerNode], partition)
				state.NodeCurrentCount[partition.OwnerNode]++
			}

		case managment.PartitionAllocationRequestedRelease, managment.PartitionAllocationPendingRelease:
			// Keep with current owner but don't count as active
			// These will be handled by graceful migration logic
			continue

		case managment.PartitionAllocationUnassigned:
			state.UnassignedPartitions = append(state.UnassignedPartitions, partition)
		}
	}

	// Categorize nodes based on their current allocation vs target
	for i, nodeId := range activeNodeIds {
		target := r.getTargetForNode(i, targetPerNode, extraPartitions)
		currentCount := state.NodeCurrentCount[nodeId]

		if currentCount == 0 && target > 0 {
			state.NewNodes = append(state.NewNodes, nodeId)
		} else if currentCount > target {
			state.OverAllocatedNodes = append(state.OverAllocatedNodes, nodeId)
		} else if currentCount < target {
			state.UnderAllocatedNodes = append(state.UnderAllocatedNodes, nodeId)
		}
	}

	return state
}

// executeGracefulMigration implements two-phase partition migration
// Creates placeholders for graceful rebalancing when adding new nodes to imbalanced clusters
func (r *RebalanceAlgoV1) executeGracefulMigration(
	nodePartitionMappings map[string]*nodePartitionMapping,
	state *NodeAllocationState,
) {
	// Only proceed if we have both new nodes and over-allocated nodes
	// This indicates a scenario where graceful migration would be beneficial
	if len(state.NewNodes) == 0 || len(state.OverAllocatedNodes) == 0 {
		return
	}

	// Process each over-allocated node
	newNodeIndex := 0
	for _, overNodeId := range state.OverAllocatedNodes {
		if newNodeIndex >= len(state.NewNodes) {
			break // No more new nodes to assign to
		}

		// Find target count for this over-allocated node
		overNodeTarget := 0
		for i, nodeId := range state.ActiveNodeIds {
			if nodeId == overNodeId {
				overNodeTarget = r.getTargetForNode(i, state.TargetPerNode, state.ExtraPartitions)
				break
			}
		}

		currentCount := state.NodeCurrentCount[overNodeId]
		excessCount := currentCount - overNodeTarget

		if excessCount <= 0 {
			continue // Node is not actually over-allocated
		}

		// Mark excess partitions for release and create placeholders
		partitionsToRelease := 0
		for partitionId, partition := range nodePartitionMappings {
			if partition.OwnerNode == overNodeId &&
				partition.Status == managment.PartitionAllocationAssigned &&
				partitionsToRelease < excessCount {

				// Mark partition for release
				partition.Status = managment.PartitionAllocationRequestedRelease
				partition.UpdatedTime = r.Now()

				// Create placeholder on new node
				newNodeId := state.NewNodes[newNodeIndex]
				placeholderKey := partitionId + "_placeholder_" + newNodeId
				nodePartitionMappings[placeholderKey] = &nodePartitionMapping{
					OwnerNode:   newNodeId,
					Partition:   partition.Partition,
					Status:      managment.PartitionAllocationPlaceholder,
					UpdatedTime: r.Now(),
				}

				// Update state counts for load balancing calculations
				state.NodeCurrentCount[overNodeId]--  // Reduce over-allocated node count
				state.NodeCurrentCount[newNodeId]++   // Increase new node count

				partitionsToRelease++

				// Check if current new node has reached its target
				newNodeTarget := 0
				for i, nodeId := range state.ActiveNodeIds {
					if nodeId == newNodeId {
						newNodeTarget = r.getTargetForNode(i, state.TargetPerNode, state.ExtraPartitions)
						break
					}
				}

				// Move to next new node when current one reaches target
				if state.NodeCurrentCount[newNodeId] >= newNodeTarget {
					newNodeIndex++
					if newNodeIndex >= len(state.NewNodes) {
						break // No more new nodes available
					}
				}
			}
		}
	}

	// Update state.OverAllocatedNodes and state.NewNodes lists after changes
	r.updateNodeCategories(state)
}

// updateNodeCategories refreshes node categorization after state changes
func (r *RebalanceAlgoV1) updateNodeCategories(state *NodeAllocationState) {
	state.NewNodes = state.NewNodes[:0]            // Clear
	state.OverAllocatedNodes = state.OverAllocatedNodes[:0] // Clear
	state.UnderAllocatedNodes = state.UnderAllocatedNodes[:0] // Clear

	// Recategorize nodes based on updated counts
	for i, nodeId := range state.ActiveNodeIds {
		target := r.getTargetForNode(i, state.TargetPerNode, state.ExtraPartitions)
		currentCount := state.NodeCurrentCount[nodeId]

		if currentCount == 0 && target > 0 {
			state.NewNodes = append(state.NewNodes, nodeId)
		} else if currentCount > target {
			state.OverAllocatedNodes = append(state.OverAllocatedNodes, nodeId)
		} else if currentCount < target {
			state.UnderAllocatedNodes = append(state.UnderAllocatedNodes, nodeId)
		}
	}
}

// assignUnassignedPartitions assigns truly unassigned partitions to nodes with lowest counts
func (r *RebalanceAlgoV1) assignUnassignedPartitions(
	nodePartitionMappings map[string]*nodePartitionMapping,
	state *NodeAllocationState,
) {
	// Assign each unassigned partition to the node with lowest count
	for _, partition := range state.UnassignedPartitions {
		// Find node with lowest current count that hasn't reached its target
		minNodeId := r.findMinimumCountNode(state, true)  // preferUnderTarget = true
		
		// If no node is under target, assign to node with lowest count overall
		if minNodeId == "" {
			minNodeId = r.findMinimumCountNode(state, false) // preferUnderTarget = false
		}
		
		if minNodeId != "" {
			// Assign partition to this node
			partition.OwnerNode = minNodeId
			partition.Status = managment.PartitionAllocationAssigned
			partition.UpdatedTime = r.Now()
			
			// Update state tracking
			state.NodePartitions[minNodeId] = append(state.NodePartitions[minNodeId], partition)
			state.NodeCurrentCount[minNodeId]++
		}
	}
	
	// Clear unassigned partitions since they've all been assigned
	state.UnassignedPartitions = state.UnassignedPartitions[:0]
	
	// Update node categories after assignments
	r.updateNodeCategories(state)
}

// balanceRemainingImbalances moves active partitions between nodes for final perfect balance
func (r *RebalanceAlgoV1) balanceRemainingImbalances(
	nodePartitionMappings map[string]*nodePartitionMapping,
	state *NodeAllocationState,
) {
	// Only proceed if no graceful migration is in progress
	// (i.e., no partitions are in RequestedRelease or PendingRelease state)
	if r.hasPartitionsInReleaseState(nodePartitionMappings) {
		return // Skip final balancing during graceful migration
	}
	
	// Rebalance over-allocated nodes by moving excess active partitions
	for _, overNodeId := range state.OverAllocatedNodes {
		// Find target for this node
		overNodeTarget := 0
		for i, nodeId := range state.ActiveNodeIds {
			if nodeId == overNodeId {
				overNodeTarget = r.getTargetForNode(i, state.TargetPerNode, state.ExtraPartitions)
				break
			}
		}
		
		currentCount := state.NodeCurrentCount[overNodeId]
		if currentCount <= overNodeTarget {
			continue // Node is not actually over-allocated
		}
		
		// Get partitions to move (prefer placeholders first, then regular assigned partitions)
		partitionsToMove := r.selectPartitionsToMove(state.NodePartitions[overNodeId], currentCount - overNodeTarget)
		
		// Move each partition to an under-allocated node
		for _, partitionToMove := range partitionsToMove {
			targetNodeId := r.findMinimumCountNode(state, true) // preferUnderTarget = true
			
			if targetNodeId != "" && targetNodeId != overNodeId {
				// Move partition to target node
				partitionToMove.OwnerNode = targetNodeId
				partitionToMove.UpdatedTime = r.Now()
				
				// Update state tracking
				state.NodeCurrentCount[overNodeId]--
				state.NodeCurrentCount[targetNodeId]++
				
				// Update node partition lists
				r.removePartitionFromNode(state.NodePartitions[overNodeId], partitionToMove)
				state.NodePartitions[targetNodeId] = append(state.NodePartitions[targetNodeId], partitionToMove)
			}
		}
	}
	
	// Final update of node categories
	r.updateNodeCategories(state)
}

// findMinimumCountNode finds the node with the lowest partition count
func (r *RebalanceAlgoV1) findMinimumCountNode(state *NodeAllocationState, preferUnderTarget bool) string {
	var minNodeId string
	minCount := len(state.ActiveNodeIds) * state.TargetPerNode + state.ExtraPartitions + 1 // Start with impossible high value
	
	for i, nodeId := range state.ActiveNodeIds {
		currentCount := state.NodeCurrentCount[nodeId]
		target := r.getTargetForNode(i, state.TargetPerNode, state.ExtraPartitions)
		
		if preferUnderTarget {
			// Only consider nodes under their target
			if currentCount < target && currentCount < minCount {
				minCount = currentCount
				minNodeId = nodeId
			}
		} else {
			// Consider all nodes
			if currentCount < minCount {
				minCount = currentCount
				minNodeId = nodeId
			}
		}
	}
	
	return minNodeId
}

// hasPartitionsInReleaseState checks if any partitions are currently in release state
func (r *RebalanceAlgoV1) hasPartitionsInReleaseState(nodePartitionMappings map[string]*nodePartitionMapping) bool {
	for _, partition := range nodePartitionMappings {
		if partition.Status == managment.PartitionAllocationRequestedRelease ||
			partition.Status == managment.PartitionAllocationPendingRelease {
			return true
		}
	}
	return false
}

// selectPartitionsToMove selects which partitions to move from an over-allocated node
func (r *RebalanceAlgoV1) selectPartitionsToMove(nodePartitions []*nodePartitionMapping, count int) []*nodePartitionMapping {
	if count >= len(nodePartitions) {
		return nodePartitions // Move all partitions
	}
	
	// Prefer moving placeholders first (they're easier to move), then regular assigned partitions
	placeholders := make([]*nodePartitionMapping, 0)
	assigned := make([]*nodePartitionMapping, 0)
	
	for _, partition := range nodePartitions {
		if partition.Status == managment.PartitionAllocationPlaceholder {
			placeholders = append(placeholders, partition)
		} else if partition.Status == managment.PartitionAllocationAssigned {
			assigned = append(assigned, partition)
		}
	}
	
	result := make([]*nodePartitionMapping, 0, count)
	
	// Add placeholders first
	for i, partition := range placeholders {
		if len(result) >= count {
			break
		}
		result = append(result, partition)
		_ = i // avoid unused variable
	}
	
	// Add assigned partitions if needed
	for i, partition := range assigned {
		if len(result) >= count {
			break
		}
		result = append(result, partition)
		_ = i // avoid unused variable
	}
	
	return result
}

// removePartitionFromNode removes a partition from a node's partition list
func (r *RebalanceAlgoV1) removePartitionFromNode(nodePartitions []*nodePartitionMapping, partitionToRemove *nodePartitionMapping) []*nodePartitionMapping {
	for i, partition := range nodePartitions {
		if partition == partitionToRemove {
			// Remove by swapping with last element and truncating
			nodePartitions[i] = nodePartitions[len(nodePartitions)-1]
			return nodePartitions[:len(nodePartitions)-1]
		}
	}
	return nodePartitions // Not found, return unchanged
}

// Helper function to get target partition count for a node (handles remainder distribution)
func (r *RebalanceAlgoV1) getTargetForNode(nodeIndex int, targetPerNode, extraPartitions int) int {
	if nodeIndex < extraPartitions {
		return targetPerNode + 1
	}
	return targetPerNode
}