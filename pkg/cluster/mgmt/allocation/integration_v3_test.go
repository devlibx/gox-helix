package allocation

import (
	"fmt"
	"sort"
	"testing"

	"github.com/devlibx/gox-base/v2"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	"github.com/stretchr/testify/assert"
)

// TestV3AlgorithmIntegration tests the V3 algorithm against complex scenarios similar to E2E tests
func TestV3AlgorithmIntegration(t *testing.T) {
	mockCF := gox.NewNoOpCrossFunction()
	algorithm := &AlgorithmV1{
		CrossFunction:   mockCF,
		algorithmConfig: &managment.AlgorithmConfig{},
	}

	t.Run("BasicEvenDistribution_15_Partitions_5_Nodes", func(t *testing.T) {
		// Scenario: 15 partitions, 5 nodes (3 per node)
		partitionStates := make(map[string]*PartitionState)
		for i := 0; i < 15; i++ {
			partitionStates[fmt.Sprintf("%d", i)] = &PartitionState{
				PartitionID: fmt.Sprintf("%d", i),
				NodeID:      "",
				Status:      managment.PartitionAllocationUnassigned,
				UpdatedTime: mockCF.Now(),
			}
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
			"node3": {NodeID: "node3", IsActive: true},
			"node4": {NodeID: "node4", IsActive: true},
			"node5": {NodeID: "node5", IsActive: true},
		}

		taskListInfo := managment.TaskListInfo{PartitionCount: 15}
		_, targetAssignments := algorithm.calculateTargetDistribution(taskListInfo, partitionStates, nodeStates)

		// Validate even distribution (15/5 = 3 per node)
		assert.Len(t, targetAssignments, 5)
		for nodeID := range nodeStates {
			assert.Len(t, targetAssignments[nodeID], 3, "Each node should have 3 partitions")
		}

		// Validate all partitions are assigned
		allAssignedPartitions := make(map[string]bool)
		for _, partitions := range targetAssignments {
			for _, partition := range partitions {
				allAssignedPartitions[partition.PartitionID] = true
			}
		}
		assert.Len(t, allAssignedPartitions, 15, "All 15 partitions should be assigned")
	})

	t.Run("UnevenDistribution_16_Partitions_5_Nodes", func(t *testing.T) {
		// Scenario: 16 partitions, 5 nodes (3 per node + 1 extra)
		partitionStates := make(map[string]*PartitionState)
		for i := 0; i < 16; i++ {
			partitionStates[fmt.Sprintf("%d", i)] = &PartitionState{
				PartitionID: fmt.Sprintf("%d", i),
				NodeID:      "",
				Status:      managment.PartitionAllocationUnassigned,
				UpdatedTime: mockCF.Now(),
			}
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
			"node3": {NodeID: "node3", IsActive: true},
			"node4": {NodeID: "node4", IsActive: true},
			"node5": {NodeID: "node5", IsActive: true},
		}

		taskListInfo := managment.TaskListInfo{PartitionCount: 16}
		_, targetAssignments := algorithm.calculateTargetDistribution(taskListInfo, partitionStates, nodeStates)

		// Count partitions per node and sort for consistent validation
		nodeCounts := make([]int, 0)
		for _, partitions := range targetAssignments {
			nodeCounts = append(nodeCounts, len(partitions))
		}
		sort.Ints(nodeCounts)

		// Should have 4 nodes with 3 partitions, 1 node with 4 partitions
		assert.Equal(t, []int{3, 3, 3, 3, 4}, nodeCounts, "Distribution should be balanced with one extra")

		// Validate all partitions are assigned
		totalAssigned := 0
		for _, partitions := range targetAssignments {
			totalAssigned += len(partitions)
		}
		assert.Equal(t, 16, totalAssigned, "All 16 partitions should be assigned")
	})

	t.Run("RebalanceOverAllocatedNodes", func(t *testing.T) {
		// Scenario: Imbalanced start (node1: 5, node2: 4, node3: 4, others: 0) -> balanced with releases
		partitionStates := map[string]*PartitionState{
			"0":  {PartitionID: "0", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"1":  {PartitionID: "1", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"2":  {PartitionID: "2", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"3":  {PartitionID: "3", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"4":  {PartitionID: "4", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"5":  {PartitionID: "5", NodeID: "node2", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"6":  {PartitionID: "6", NodeID: "node2", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"7":  {PartitionID: "7", NodeID: "node2", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"8":  {PartitionID: "8", NodeID: "node2", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"9":  {PartitionID: "9", NodeID: "node3", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"10": {PartitionID: "10", NodeID: "node3", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"11": {PartitionID: "11", NodeID: "node3", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"12": {PartitionID: "12", NodeID: "node3", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"13": {PartitionID: "13", NodeID: "", Status: managment.PartitionAllocationUnassigned, UpdatedTime: mockCF.Now()},
			"14": {PartitionID: "14", NodeID: "", Status: managment.PartitionAllocationUnassigned, UpdatedTime: mockCF.Now()},
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
			"node3": {NodeID: "node3", IsActive: true},
			"node4": {NodeID: "node4", IsActive: true},
			"node5": {NodeID: "node5", IsActive: true},
		}

		taskListInfo := managment.TaskListInfo{PartitionCount: 15}
		distributions, targetAssignments := algorithm.calculateTargetDistribution(taskListInfo, partitionStates, nodeStates)

		// Validate that over-allocated nodes mark partitions for release
		node1Dist := distributions["node1"]
		assert.True(t, len(node1Dist.partitionWithReleaseState) > 0, "Node1 should have release partitions")
		
		node2Dist := distributions["node2"]
		assert.True(t, len(node2Dist.partitionWithReleaseState) > 0, "Node2 should have release partitions")

		// Validate that other nodes get placeholders
		placeholderCount := 0
		for nodeId, dist := range distributions {
			if nodeId == "node4" || nodeId == "node5" {
				placeholderCount += len(dist.partitionWithPlaceholderState)
			}
		}
		assert.True(t, placeholderCount > 0, "Some nodes should have placeholder partitions")

		// Validate all partitions are allocated somewhere
		allPartitionIds := make(map[string]bool)
		for _, partitions := range targetAssignments {
			for _, partition := range partitions {
				allPartitionIds[partition.PartitionID] = true
			}
		}
		assert.Len(t, allPartitionIds, 15, "All 15 partitions should be allocated")
	})

	t.Run("SingleNode", func(t *testing.T) {
		// Scenario: 10 partitions, 1 node (all on single node)
		partitionStates := make(map[string]*PartitionState)
		for i := 0; i < 10; i++ {
			partitionStates[fmt.Sprintf("%d", i)] = &PartitionState{
				PartitionID: fmt.Sprintf("%d", i),
				NodeID:      "",
				Status:      managment.PartitionAllocationUnassigned,
				UpdatedTime: mockCF.Now(),
			}
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
		}

		taskListInfo := managment.TaskListInfo{PartitionCount: 10}
		_, targetAssignments := algorithm.calculateTargetDistribution(taskListInfo, partitionStates, nodeStates)

		// Validate single node gets all partitions
		assert.Len(t, targetAssignments, 1)
		assert.Len(t, targetAssignments["node1"], 10, "Single node should get all 10 partitions")
	})

	t.Run("InactiveNodePartitions", func(t *testing.T) {
		// Scenario: Partitions assigned to inactive nodes should be reassigned
		partitionStates := map[string]*PartitionState{
			"0": {PartitionID: "0", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"1": {PartitionID: "1", NodeID: "node2", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()}, // inactive node
			"2": {PartitionID: "2", NodeID: "node3", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: false}, // inactive
			"node3": {NodeID: "node3", IsActive: true},
		}

		// Simulate getCurrentState behavior - inactive node assignments become unassigned
		for _, state := range partitionStates {
			if state.NodeID == "node2" { // inactive node
				state.NodeID = ""
				state.Status = managment.PartitionAllocationUnassigned
			}
		}

		taskListInfo := managment.TaskListInfo{PartitionCount: 3}
		_, targetAssignments := algorithm.calculateTargetDistribution(taskListInfo, partitionStates, nodeStates)

		// Validate only active nodes get assignments
		assert.Empty(t, targetAssignments["node2"], "Inactive node should have no assignments")
		assert.True(t, len(targetAssignments["node1"]) > 0, "Active node1 should have assignments")
		assert.True(t, len(targetAssignments["node3"]) > 0, "Active node3 should have assignments")

		// Validate all partitions are assigned to active nodes
		totalAssigned := len(targetAssignments["node1"]) + len(targetAssignments["node3"])
		assert.Equal(t, 3, totalAssigned, "All 3 partitions should be assigned to active nodes")
	})

	t.Run("ComplexMixedStateScenario", func(t *testing.T) {
		// Scenario: Complex mix of partition states with multiple nodes
		partitionStates := map[string]*PartitionState{
			"0": {PartitionID: "0", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"1": {PartitionID: "1", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"2": {PartitionID: "2", NodeID: "node1", Status: managment.PartitionAllocationRequestedRelease, UpdatedTime: mockCF.Now()},
			"3": {PartitionID: "3", NodeID: "node2", Status: managment.PartitionAllocationPendingRelease, UpdatedTime: mockCF.Now()},
			"4": {PartitionID: "4", NodeID: "", Status: managment.PartitionAllocationUnassigned, UpdatedTime: mockCF.Now()},
			"5": {PartitionID: "5", NodeID: "", Status: managment.PartitionAllocationUnassigned, UpdatedTime: mockCF.Now()},
			"6": {PartitionID: "6", NodeID: "node2", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"7": {PartitionID: "7", NodeID: "node2", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
			"node3": {NodeID: "node3", IsActive: true},
			"node4": {NodeID: "node4", IsActive: true},
		}

		taskListInfo := managment.TaskListInfo{PartitionCount: 8}
		distributions, targetAssignments := algorithm.calculateTargetDistribution(taskListInfo, partitionStates, nodeStates)

		// Validate all unique partitions are handled
		allPartitionIds := make(map[string]bool)
		for _, partitions := range targetAssignments {
			for _, partition := range partitions {
				allPartitionIds[partition.PartitionID] = true
			}
		}
		assert.Len(t, allPartitionIds, 8, "All unique partitions should be present")

		// Validate that release state partitions are tracked
		releasePartitions := 0
		for _, dist := range distributions {
			releasePartitions += len(dist.partitionWithReleaseState)
		}
		assert.True(t, releasePartitions > 0, "Should have some release partitions")

		// Validate that placeholders exist for release partitions
		placeholderPartitions := 0
		for _, dist := range distributions {
			placeholderPartitions += len(dist.partitionWithPlaceholderState)
		}
		assert.True(t, placeholderPartitions > 0, "Should have some placeholder partitions")

		// CRITICAL: Validate stable cluster property - each release partition has placeholder elsewhere
		for nodeId, dist := range distributions {
			for releasePartitionId := range dist.partitionWithReleaseState {
				// Find placeholder for this partition on OTHER nodes
				hasPlaceholder := false
				for otherNodeId, otherDist := range distributions {
					if otherNodeId != nodeId {
						if _, exists := otherDist.partitionWithPlaceholderState[releasePartitionId]; exists {
							hasPlaceholder = true
							break
						}
					}
				}
				assert.True(t, hasPlaceholder, "Release partition %s on node %s must have placeholder elsewhere", releasePartitionId, nodeId)
			}
		}
	})
}

// TestV3AlgorithmStabilityAndConsistency tests that the algorithm produces stable and consistent results
func TestV3AlgorithmStabilityAndConsistency(t *testing.T) {
	mockCF := gox.NewNoOpCrossFunction()
	algorithm := &AlgorithmV1{
		CrossFunction:   mockCF,
		algorithmConfig: &managment.AlgorithmConfig{},
	}

	t.Run("IdempotentBehavior", func(t *testing.T) {
		// Test that running the same input multiple times produces the same output
		partitionStates := map[string]*PartitionState{
			"0": {PartitionID: "0", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"1": {PartitionID: "1", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"2": {PartitionID: "2", NodeID: "node2", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"3": {PartitionID: "3", NodeID: "node2", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
		}

		taskListInfo := managment.TaskListInfo{PartitionCount: 4}
		
		// Run algorithm multiple times
		var results []map[string][]*PartitionState
		for i := 0; i < 5; i++ {
			// Create fresh copies of partition states
			freshPartitionStates := make(map[string]*PartitionState)
			for k, v := range partitionStates {
				freshPartitionStates[k] = &PartitionState{
					PartitionID: v.PartitionID,
					NodeID:      v.NodeID,
					Status:      v.Status,
					UpdatedTime: v.UpdatedTime,
				}
			}
			
			_, result := algorithm.calculateTargetDistribution(taskListInfo, freshPartitionStates, nodeStates)
			results = append(results, result)
		}

		// Validate all results are identical in terms of partition distribution
		firstResult := results[0]
		for i := 1; i < len(results); i++ {
			for nodeId := range nodeStates {
				assert.Len(t, results[i][nodeId], len(firstResult[nodeId]), 
					"Run %d should have same partition count for node %s", i, nodeId)
			}
		}
	})

	t.Run("StableClusterProperty", func(t *testing.T) {
		// Test the stable cluster property: all partitions allocated, release partitions have placeholders
		partitionStates := map[string]*PartitionState{
			"0": {PartitionID: "0", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"1": {PartitionID: "1", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"2": {PartitionID: "2", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"3": {PartitionID: "3", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
			"4": {PartitionID: "4", NodeID: "node1", Status: managment.PartitionAllocationAssigned, UpdatedTime: mockCF.Now()},
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
		}

		taskListInfo := managment.TaskListInfo{PartitionCount: 5}
		distributions, result := algorithm.calculateTargetDistribution(taskListInfo, partitionStates, nodeStates)

		// Property 1: All partitions must be allocated somewhere
		allPartitionIds := make(map[string]bool)
		for _, partitions := range result {
			for _, partition := range partitions {
				allPartitionIds[partition.PartitionID] = true
			}
		}
		assert.Len(t, allPartitionIds, 5, "All partitions must be allocated in stable cluster")

		// Property 2: Release partitions must have placeholders on other nodes
		for nodeId, dist := range distributions {
			for releasePartitionId := range dist.partitionWithReleaseState {
				// Find placeholder for this partition on other nodes
				hasPlaceholder := false
				for otherNodeId, otherDist := range distributions {
					if otherNodeId != nodeId {
						if _, exists := otherDist.partitionWithPlaceholderState[releasePartitionId]; exists {
							hasPlaceholder = true
							break
						}
					}
				}
				assert.True(t, hasPlaceholder, "Release partition %s must have placeholder elsewhere", releasePartitionId)
			}
		}
	})
}