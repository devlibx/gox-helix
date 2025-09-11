package allocation

import (
	"fmt"
	"sort"
	"testing"

	"github.com/devlibx/gox-base/v2"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	"github.com/stretchr/testify/assert"
)

// Unit tests for V3 algorithm - test algorithm logic without database dependencies

// TestActiveNodeCapacityCalculation tests that only active nodes count for capacity calculation
func TestActiveNodeCapacityCalculation(t *testing.T) {
	mockCF := gox.NewNoOpCrossFunction()
	algorithm := &AlgorithmV1{
		CrossFunction: mockCF,
		algorithmConfig: &managment.AlgorithmConfig{},
	}

	t.Run("OnlyActiveNodesCountForCapacity", func(t *testing.T) {
		// 16 partitions: 3 active nodes, 2 inactive nodes
		// Should calculate: 16/3 = 5 remainder 1 -> [6, 5, 5]
		taskListInfo := managment.TaskListInfo{PartitionCount: 16}
		
		partitionInfos := make(map[string]*PartitionState)
		for i := 0; i < 16; i++ {
			partitionID := fmt.Sprintf("%d", i)
			partitionInfos[partitionID] = &PartitionState{
				PartitionID: partitionID,
				NodeID:      "",
				Status:      managment.PartitionAllocationUnassigned,
				UpdatedTime: mockCF.Now(),
			}
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true}, 
			"node3": {NodeID: "node3", IsActive: true},
			"node4": {NodeID: "node4", IsActive: false}, // inactive
			"node5": {NodeID: "node5", IsActive: false}, // inactive
		}

		distributions, _ := algorithm.calculateTargetDistribution(taskListInfo, partitionInfos, nodeStates)

		// Should only have distributions for active nodes
		assert.Len(t, distributions, 3, "Should only create distributions for active nodes")
		assert.Contains(t, distributions, "node1")
		assert.Contains(t, distributions, "node2")  
		assert.Contains(t, distributions, "node3")
		assert.NotContains(t, distributions, "node4", "Should not create distribution for inactive node")
		assert.NotContains(t, distributions, "node5", "Should not create distribution for inactive node")

		// Validate capacity distribution: one node gets 6, two nodes get 5 (16 total / 3 nodes = 5 remainder 1)
		capacities := make([]int, 0)
		for _, dist := range distributions {
			capacities = append(capacities, dist.maxAllowed)
		}
		
		// Sort to get consistent ordering
		sort.Ints(capacities)
		assert.Equal(t, []int{5, 5, 6}, capacities, "Should have two nodes with 5 partitions and one with 6")
	})

	t.Run("NoActiveNodesEdgeCase", func(t *testing.T) {
		taskListInfo := managment.TaskListInfo{PartitionCount: 10}
		partitionInfos := make(map[string]*PartitionState)
		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: false},
			"node2": {NodeID: "node2", IsActive: false},
		}

		distributions, _ := algorithm.calculateTargetDistribution(taskListInfo, partitionInfos, nodeStates)
		assert.Empty(t, distributions, "Should return empty distributions when no active nodes")
	})
}

// TestPhase1StickyAllocation tests Phase 1 sticky allocation logic
func TestPhase1StickyAllocation(t *testing.T) {
	mockCF := gox.NewNoOpCrossFunction()
	
	t.Run("AssignedPartitionStaysWithinCapacity", func(t *testing.T) {
		// Create node distribution with capacity for 3 partitions
		nd := newNodeDistribution(mockCF, 3, "node1")
		
		partition := &PartitionState{
			PartitionID: "0",
			NodeID:      "node1",
			Status:      managment.PartitionAllocationAssigned,
			UpdatedTime: mockCF.Now(),
		}
		
		result := nd.tryToAllocatePhase1(partition)
		
		assert.True(t, result, "Should return true when partition allocated successfully")
		assert.Contains(t, nd.partitionWithAssignedState, "0", "Partition should be in assigned state")
		assert.Equal(t, 1, nd.getTotalCapacityUsed(), "Capacity should be used")
	})
	
	t.Run("AssignedPartitionMarkedForReleaseWhenOverCapacity", func(t *testing.T) {
		// Create node distribution with no capacity left
		nd := newNodeDistribution(mockCF, 2, "node1")
		
		// Fill up capacity first
		nd.partitionWithAssignedState["1"] = &PartitionState{PartitionID: "1"}
		nd.partitionWithAssignedState["2"] = &PartitionState{PartitionID: "2"}
		
		partition := &PartitionState{
			PartitionID: "0",
			NodeID:      "node1",
			Status:      managment.PartitionAllocationAssigned,
			UpdatedTime: mockCF.Now(),
		}
		
		result := nd.tryToAllocatePhase1(partition)
		
		assert.False(t, result, "Should return false when over capacity (needs Phase 2)")
		assert.Contains(t, nd.partitionWithReleaseState, "0", "Partition should be marked for release")
		
		releasePartition := nd.partitionWithReleaseState["0"]
		assert.Equal(t, managment.PartitionAllocationRequestedRelease, releasePartition.Status)
	})
	
	t.Run("ReleaseStatesPassThrough", func(t *testing.T) {
		nd := newNodeDistribution(mockCF, 2, "node1")
		
		partition := &PartitionState{
			PartitionID: "0",
			NodeID:      "node1", 
			Status:      managment.PartitionAllocationRequestedRelease,
			UpdatedTime: mockCF.Now(),
		}
		
		result := nd.tryToAllocatePhase1(partition)
		
		assert.False(t, result, "Should return false to allow Phase 2 processing")
		assert.Contains(t, nd.partitionWithReleaseState, "0", "Release partition should be tracked")
		
		releasePartition := nd.partitionWithReleaseState["0"]
		assert.Equal(t, managment.PartitionAllocationRequestedRelease, releasePartition.Status)
		assert.Equal(t, "node1", releasePartition.NodeID)
	})
	
	t.Run("WrongNodeIgnored", func(t *testing.T) {
		nd := newNodeDistribution(mockCF, 3, "node1")
		
		partition := &PartitionState{
			PartitionID: "0",
			NodeID:      "node2", // Different node
			Status:      managment.PartitionAllocationAssigned,
			UpdatedTime: mockCF.Now(),
		}
		
		result := nd.tryToAllocatePhase1(partition)
		
		assert.False(t, result, "Should ignore partitions for other nodes")
		assert.Empty(t, nd.partitionWithAssignedState, "Should not allocate partition")
	})
}

// TestPhase2PlaceholderCreation tests Phase 2 cross-node placeholder logic
func TestPhase2PlaceholderCreation(t *testing.T) {
	mockCF := gox.NewNoOpCrossFunction()
	
	t.Run("ReleasePartitionGetsPlaceholderOnOtherNode", func(t *testing.T) {
		nd := newNodeDistribution(mockCF, 3, "node2") // Target node
		
		partition := &PartitionState{
			PartitionID: "0",
			NodeID:      "node1", // Original node
			Status:      managment.PartitionAllocationRequestedRelease,
			UpdatedTime: mockCF.Now(),
		}
		
		result := nd.tryToAllocatePhase2(partition, "node2")
		
		assert.True(t, result, "Should create placeholder on other node")
		assert.Contains(t, nd.partitionWithPlaceholderState, "0", "Should have placeholder")
		
		placeholder := nd.partitionWithPlaceholderState["0"]
		assert.Equal(t, "node2", placeholder.NodeID, "Placeholder should be for target node")
		assert.Equal(t, managment.PartitionAllocationPlaceholder, placeholder.Status)
	})
	
	t.Run("SameNodeGuardPreventsPlaceholder", func(t *testing.T) {
		nd := newNodeDistribution(mockCF, 3, "node1")
		
		partition := &PartitionState{
			PartitionID: "0",
			NodeID:      "node1", // Same node
			Status:      managment.PartitionAllocationRequestedRelease,
			UpdatedTime: mockCF.Now(),
		}
		
		result := nd.tryToAllocatePhase2(partition, "node1")
		
		assert.False(t, result, "Should not create placeholder on same node")
		assert.Empty(t, nd.partitionWithPlaceholderState, "Should not have placeholder")
	})
	
	t.Run("UnassignedPartitionGetsDirectAssignment", func(t *testing.T) {
		nd := newNodeDistribution(mockCF, 3, "node2")
		
		partition := &PartitionState{
			PartitionID: "0",
			NodeID:      "",
			Status:      managment.PartitionAllocationUnassigned,
			UpdatedTime: mockCF.Now(),
		}
		
		result := nd.tryToAllocatePhase2(partition, "node2")
		
		assert.True(t, result, "Should assign unassigned partition")
		assert.Contains(t, nd.partitionWithAssignedState, "0", "Should have assigned partition")
		
		assignedPartition := nd.partitionWithAssignedState["0"]
		assert.Equal(t, "node2", assignedPartition.NodeID)
		assert.Equal(t, managment.PartitionAllocationAssigned, assignedPartition.Status)
	})
	
	t.Run("CapacityRespected", func(t *testing.T) {
		nd := newNodeDistribution(mockCF, 1, "node2")
		
		// Fill capacity
		nd.partitionWithAssignedState["1"] = &PartitionState{PartitionID: "1"}
		
		partition := &PartitionState{
			PartitionID: "0",
			NodeID:      "node1",
			Status:      managment.PartitionAllocationRequestedRelease,
			UpdatedTime: mockCF.Now(),
		}
		
		result := nd.tryToAllocatePhase2(partition, "node2")
		
		assert.False(t, result, "Should respect capacity limits")
		assert.Empty(t, nd.partitionWithPlaceholderState, "Should not create placeholder when over capacity")
	})
}

// TestStableClusterValidation tests stable cluster requirements
func TestStableClusterValidation(t *testing.T) {
	mockCF := gox.NewNoOpCrossFunction()
	algorithm := &AlgorithmV1{
		CrossFunction:   mockCF,
		algorithmConfig: &managment.AlgorithmConfig{},
	}
	
	t.Run("AllPartitionsAllocated", func(t *testing.T) {
		taskListInfo := managment.TaskListInfo{PartitionCount: 6}
		
		partitionInfos := make(map[string]*PartitionState)
		for i := 0; i < 6; i++ {
			partitionID := fmt.Sprintf("%d", i)
			partitionInfos[partitionID] = &PartitionState{
				PartitionID: partitionID,
				NodeID:      "",
				Status:      managment.PartitionAllocationUnassigned,
				UpdatedTime: mockCF.Now(),
			}
		}
		
		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
		}
		
		_, result := algorithm.calculateTargetDistribution(taskListInfo, partitionInfos, nodeStates)
		
		// Count total partitions in result
		totalPartitions := 0
		for _, partitions := range result {
			totalPartitions += len(partitions)
		}
		
		assert.Equal(t, 6, totalPartitions, "All partitions should be allocated")
		
		// Verify balanced distribution (6/2 = 3 each)
		assert.Len(t, result["node1"], 3, "Node1 should have 3 partitions")
		assert.Len(t, result["node2"], 3, "Node2 should have 3 partitions")
	})
	
	t.Run("ReleasePartitionsHavePlaceholdersElsewhere", func(t *testing.T) {
		taskListInfo := managment.TaskListInfo{PartitionCount: 4}
		
		// Create scenario where node1 is over-capacity and needs to release partitions
		partitionInfos := map[string]*PartitionState{
			"0": {PartitionID: "0", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"1": {PartitionID: "1", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"2": {PartitionID: "2", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"3": {PartitionID: "3", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
		}
		
		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true}, // New node added
		}
		
		distributions, result := algorithm.calculateTargetDistribution(taskListInfo, partitionInfos, nodeStates)
		
		// In V3 algorithm, node1 will have:
		// - 2 assigned partitions (within capacity)
		// - 2 release partitions (marked for release but still on node1)
		// Node2 will have:
		// - 2 placeholder partitions (for the ones being released by node1)
		assert.Len(t, result["node1"], 4, "Node1 should have 4 partitions (2 assigned + 2 release)")
		assert.Len(t, result["node2"], 2, "Node2 should have 2 partitions (placeholders)") 
		
		// Verify that some partitions are marked for release on node1
		node1Distribution := distributions["node1"]
		hasReleasePartitions := len(node1Distribution.partitionWithReleaseState) > 0
		
		if hasReleasePartitions {
			// If there are release partitions, verify placeholders exist elsewhere
			node2Distribution := distributions["node2"]
			assert.True(t, len(node2Distribution.partitionWithPlaceholderState) > 0, 
				"Node2 should have placeholders for partitions being released by node1")
		}
	})
}

// TestMixedStateHandling tests handling of multiple partition states in one run
func TestMixedStateHandling(t *testing.T) {
	mockCF := gox.NewNoOpCrossFunction()
	algorithm := &AlgorithmV1{
		CrossFunction:   mockCF,
		algorithmConfig: &managment.AlgorithmConfig{},
	}
	
	t.Run("MixedPartitionStates", func(t *testing.T) {
		taskListInfo := managment.TaskListInfo{PartitionCount: 6}
		
		// Mix of different partition states
		partitionInfos := map[string]*PartitionState{
			"0": {PartitionID: "0", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"1": {PartitionID: "1", NodeID: "node1", Status: managment.PartitionAllocationRequestedRelease, UpdatedTime: mockCF.Now()},
			"2": {PartitionID: "2", NodeID: "node2", Status: managment.PartitionAllocationPendingRelease, UpdatedTime: mockCF.Now()},
			"3": {PartitionID: "3", NodeID: "", Status: managment.PartitionAllocationUnassigned, UpdatedTime: mockCF.Now()},
			"4": {PartitionID: "4", NodeID: "", Status: managment.PartitionAllocationUnassigned, UpdatedTime: mockCF.Now()},
			"5": {PartitionID: "5", NodeID: "node2", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
		}
		
		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
			"node3": {NodeID: "node3", IsActive: true},
		}
		
		_, result := algorithm.calculateTargetDistribution(taskListInfo, partitionInfos, nodeStates)
		
		// In V3 algorithm, release partitions appear on both original node and placeholder node
		// So total partition entries will be > 6 due to placeholders
		// But we should have exactly 6 unique partition IDs
		allPartitionIds := make(map[string]bool)
		for _, partitions := range result {
			for _, partition := range partitions {
				allPartitionIds[partition.PartitionID] = true
			}
		}
		
		assert.Len(t, allPartitionIds, 6, "All unique partitions should be present")
		
		// Verify each node has some partitions (exact count varies due to mixed states)
		for nodeId, partitions := range result {
			assert.True(t, len(partitions) > 0, "Node %s should have some partitions", nodeId)
		}
	})
}