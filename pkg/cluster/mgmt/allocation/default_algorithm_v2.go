package allocation

import (
	"context"
	"database/sql"
	"fmt"
	"sort"

	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	goxJsonUtils "github.com/devlibx/gox-base/v2/serialization/utils/json"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
)

// SimpleAllocationAlgorithm implements a much simpler and cleaner partition allocation algorithm
// Key principles:
// 1. Even distribution across active nodes
// 2. Deterministic conflict resolution
// 3. Minimal partition movement
// 4. Direct assignment without complex state machines
type SimpleAllocationAlgorithm struct {
	gox.CrossFunction
	dbInterface     helixClusterMysql.Querier
	AlgorithmConfig *managment.AlgorithmConfig
}

// NewSimpleAllocationAlgorithm creates a new simplified allocation algorithm
func NewSimpleAllocationAlgorithm(
	cf gox.CrossFunction,
	clusterDbInterface helixClusterMysql.Querier,
	algorithmConfig *managment.AlgorithmConfig,
) (managment.Algorithm, error) {
	return &SimpleAllocationAlgorithm{
		CrossFunction:   cf,
		dbInterface:     clusterDbInterface,
		AlgorithmConfig: algorithmConfig,
	}, nil
}

// PartitionState represents the current state of a partition
type PartitionState struct {
	PartitionID string
	NodeID      string                            // empty if unassigned
	Status      managment.PartitionAllocationStatus
}

// NodeState represents the current state of a node
type NodeState struct {
	NodeID       string
	IsActive     bool
	PartitionIDs []string
}

// CalculateAllocation implements the core allocation logic with 5 simple steps
func (s *SimpleAllocationAlgorithm) CalculateAllocation(ctx context.Context, taskListInfo managment.TaskListInfo) (*managment.AllocationResponse, error) {
	
	// Step 1: Get current state from database
	partitionStates, nodeStates, err := s.getCurrentState(ctx, taskListInfo)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get current state")
	}
	
	// Step 2: Resolve conflicts (deterministic winner selection)
	s.resolveConflicts(partitionStates)
	
	// Step 3: Calculate target distribution
	targetAssignments := s.calculateTargetDistribution(taskListInfo, nodeStates)
	
	// Step 4: Perform minimal rebalancing
	finalAssignments := s.performRebalancing(partitionStates, targetAssignments, nodeStates)
	
	// Step 5: Update database with new assignments
	err = s.updateDatabase(ctx, taskListInfo, finalAssignments)
	if err != nil {
		return nil, errors.Wrap(err, "failed to update database")
	}
	
	return &managment.AllocationResponse{}, nil
}

// getCurrentState extracts current partition and node states from the database
func (s *SimpleAllocationAlgorithm) getCurrentState(ctx context.Context, taskListInfo managment.TaskListInfo) (map[string]*PartitionState, map[string]*NodeState, error) {
	
	// Get current allocations
	allocations, err := s.dbInterface.GetAllocationsForTasklist(ctx, helixClusterMysql.GetAllocationsForTasklistParams{
		Cluster:  taskListInfo.Cluster,
		Domain:   taskListInfo.Domain,
		Tasklist: taskListInfo.TaskList,
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get allocations")
	}
	
	// Get active nodes
	nodes, err := s.dbInterface.GetActiveNodes(ctx, taskListInfo.Cluster)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get active nodes")
	}
	
	// Initialize node states
	nodeStates := make(map[string]*NodeState)
	for _, node := range nodes {
		nodeStates[node.NodeUuid] = &NodeState{
			NodeID:       node.NodeUuid,
			IsActive:     node.Status == helixClusterMysql.NodeStatusActive,
			PartitionIDs: make([]string, 0),
		}
	}
	
	// Initialize partition states (all partitions start as unassigned)
	partitionStates := make(map[string]*PartitionState)
	for i := 0; i < taskListInfo.PartitionCount; i++ {
		partitionID := fmt.Sprintf("%d", i)
		partitionStates[partitionID] = &PartitionState{
			PartitionID: partitionID,
			NodeID:      "",
			Status:      managment.PartitionAllocationUnassigned,
		}
	}
	
	// Parse existing allocations
	for _, allocation := range allocations {
		allocInfo, err := goxJsonUtils.BytesToObject[dbPartitionAllocationInfos]([]byte(allocation.PartitionInfo))
		if err != nil {
			continue // Skip malformed allocation data
		}
		
		for _, partitionInfo := range allocInfo.PartitionAllocationInfos {
			partitionID := partitionInfo.PartitionId
			
			// Only process known partitions
			if partition, exists := partitionStates[partitionID]; exists {
				
				// Handle partitions stuck in release state (force reassign)
				if partitionInfo.AllocationStatus == managment.PartitionAllocationRequestedRelease ||
					partitionInfo.AllocationStatus == managment.PartitionAllocationPendingRelease {
					if partitionInfo.UpdatedTime.Add(s.AlgorithmConfig.TimeToWaitForPartitionReleaseBeforeForceRelease).Before(s.Now()) {
						partition.Status = managment.PartitionAllocationUnassigned
						partition.NodeID = ""
						continue
					}
				}
				
				// Only assign to active nodes
				if nodeState, nodeExists := nodeStates[allocation.NodeID]; nodeExists && nodeState.IsActive {
					if partitionInfo.AllocationStatus == managment.PartitionAllocationAssigned {
						partition.NodeID = allocation.NodeID
						partition.Status = managment.PartitionAllocationAssigned
						nodeState.PartitionIDs = append(nodeState.PartitionIDs, partitionID)
					}
				}
			}
		}
	}
	
	return partitionStates, nodeStates, nil
}

// resolveConflicts handles duplicate partition assignments deterministically
func (s *SimpleAllocationAlgorithm) resolveConflicts(partitionStates map[string]*PartitionState) {
	// Track which partitions have multiple assignments
	partitionToNodes := make(map[string][]string)
	
	// Build conflict map
	for partitionID, state := range partitionStates {
		if state.NodeID != "" && state.Status == managment.PartitionAllocationAssigned {
			partitionToNodes[partitionID] = append(partitionToNodes[partitionID], state.NodeID)
		}
	}
	
	// Resolve conflicts by choosing lexicographically smallest node ID (deterministic)
	for partitionID, nodeIDs := range partitionToNodes {
		if len(nodeIDs) > 1 {
			sort.Strings(nodeIDs) // Deterministic ordering
			winnerNodeID := nodeIDs[0]
			
			// Update partition state to keep only the winner
			partitionStates[partitionID].NodeID = winnerNodeID
			partitionStates[partitionID].Status = managment.PartitionAllocationAssigned
		}
	}
}

// calculateTargetDistribution computes optimal partition distribution across active nodes
func (s *SimpleAllocationAlgorithm) calculateTargetDistribution(taskListInfo managment.TaskListInfo, nodeStates map[string]*NodeState) map[string][]string {
	
	// Get list of active nodes
	activeNodes := make([]string, 0)
	for nodeID, nodeState := range nodeStates {
		if nodeState.IsActive {
			activeNodes = append(activeNodes, nodeID)
		}
	}
	sort.Strings(activeNodes) // Consistent ordering
	
	targetAssignments := make(map[string][]string)
	
	// If no active nodes, return empty assignments
	if len(activeNodes) == 0 {
		return targetAssignments
	}
	
	// Calculate target distribution
	basePartitionsPerNode := taskListInfo.PartitionCount / len(activeNodes)
	remainder := taskListInfo.PartitionCount % len(activeNodes)
	
	// Distribute partitions evenly
	partitionIndex := 0
	for i, nodeID := range activeNodes {
		targetAssignments[nodeID] = make([]string, 0)
		
		// First 'remainder' nodes get one extra partition
		targetCount := basePartitionsPerNode
		if i < remainder {
			targetCount++
		}
		
		// Assign partitions to this node
		for j := 0; j < targetCount && partitionIndex < taskListInfo.PartitionCount; j++ {
			partitionID := fmt.Sprintf("%d", partitionIndex)
			targetAssignments[nodeID] = append(targetAssignments[nodeID], partitionID)
			partitionIndex++
		}
	}
	
	return targetAssignments
}

// performRebalancing moves partitions to achieve target distribution with minimal movement
func (s *SimpleAllocationAlgorithm) performRebalancing(partitionStates map[string]*PartitionState, targetAssignments map[string][]string, nodeStates map[string]*NodeState) map[string][]string {
	
	// Create final assignment map
	finalAssignments := make(map[string][]string)
	for nodeID := range nodeStates {
		if nodeStates[nodeID].IsActive {
			finalAssignments[nodeID] = make([]string, 0)
		}
	}
	
	// Track which partitions need new assignments
	unassignedPartitions := make([]string, 0)
	
	// First pass: keep partitions that are already correctly assigned
	for nodeID, targetPartitions := range targetAssignments {
		for _, partitionID := range targetPartitions {
			partition := partitionStates[partitionID]
			
			// If partition is already assigned to this node, keep it
			if partition.NodeID == nodeID && partition.Status == managment.PartitionAllocationAssigned {
				finalAssignments[nodeID] = append(finalAssignments[nodeID], partitionID)
			} else {
				// Mark partition for reassignment
				unassignedPartitions = append(unassignedPartitions, partitionID)
			}
		}
	}
	
	// Second pass: assign unassigned partitions to nodes with capacity
	unassignedIndex := 0
	for nodeID, targetPartitions := range targetAssignments {
		currentCount := len(finalAssignments[nodeID])
		targetCount := len(targetPartitions)
		
		// Fill remaining capacity
		for currentCount < targetCount && unassignedIndex < len(unassignedPartitions) {
			partitionID := unassignedPartitions[unassignedIndex]
			finalAssignments[nodeID] = append(finalAssignments[nodeID], partitionID)
			currentCount++
			unassignedIndex++
		}
	}
	
	return finalAssignments
}

// updateDatabase writes the final assignments to the database
func (s *SimpleAllocationAlgorithm) updateDatabase(ctx context.Context, taskListInfo managment.TaskListInfo, finalAssignments map[string][]string) error {
	
	for nodeID, partitionIDs := range finalAssignments {
		if len(partitionIDs) == 0 {
			continue // Skip nodes with no assignments
		}
		
		// Create allocation object
		allocation := &managment.Allocation{
			Cluster:                  taskListInfo.Cluster,
			Domain:                   taskListInfo.Domain,
			TaskList:                 taskListInfo.TaskList,
			NodeId:                   nodeID,
			PartitionAllocationInfos: make([]managment.PartitionAllocationInfo, 0, len(partitionIDs)),
		}
		
		// Add partition allocation info
		for _, partitionID := range partitionIDs {
			allocation.PartitionAllocationInfos = append(allocation.PartitionAllocationInfos, managment.PartitionAllocationInfo{
				PartitionId:      partitionID,
				AllocationStatus: managment.PartitionAllocationAssigned,
				UpdatedTime:      s.Now(),
			})
		}
		
		// Serialize allocation data
		allocationJSON, err := goxJsonUtils.ObjectToString(allocation)
		if err != nil {
			return errors.Wrap(err, "failed to serialize allocation")
		}
		
		// Upsert allocation in database
		err = s.dbInterface.UpsertAllocation(ctx, helixClusterMysql.UpsertAllocationParams{
			Cluster:       taskListInfo.Cluster,
			Domain:        taskListInfo.Domain,
			Tasklist:      taskListInfo.TaskList,
			NodeID:        nodeID,
			PartitionInfo: allocationJSON,
			Metadata:      sql.NullString{Valid: true, String: "{}"},
		})
		if err != nil {
			return errors.Wrap(err, "failed to upsert allocation for node "+nodeID)
		}
	}
	
	return nil
}