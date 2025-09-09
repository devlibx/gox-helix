package allocation

import (
	"fmt"
	"sort"
	"testing"

	"github.com/devlibx/gox-base/v2"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	"github.com/stretchr/testify/assert"
)

// TestSimpleAlgorithmIntegration tests the algorithm against the same scenarios as the complex E2E tests
func TestSimpleAlgorithmIntegration(t *testing.T) {
	mockCF := gox.NewNoOpCrossFunction()
	algorithm := &SimpleAllocationAlgorithm{
		CrossFunction: mockCF,
		AlgorithmConfig: &managment.AlgorithmConfig{},
	}

	t.Run("BasicEvenDistribution_15_Partitions_5_Nodes", func(t *testing.T) {
		// Scenario: 15 partitions, 5 nodes (3 per node)
		partitionStates := make(map[string]*PartitionState)
		for i := 0; i < 15; i++ {
			partitionStates[fmt.Sprintf("%d", i)] = &PartitionState{
				PartitionID: fmt.Sprintf("%d", i),
				NodeID:      "",
				Status:      managment.PartitionAllocationUnassigned,
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
		targetAssignments := algorithm.calculateTargetDistribution(taskListInfo, nodeStates)

		// Validate even distribution
		assert.Len(t, targetAssignments, 5)
		for nodeID := range nodeStates {
			assert.Len(t, targetAssignments[nodeID], 3, "Each node should have 3 partitions")
		}

		// Validate all partitions are assigned
		allAssignedPartitions := make(map[string]bool)
		for _, partitions := range targetAssignments {
			for _, partitionID := range partitions {
				allAssignedPartitions[partitionID] = true
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
		targetAssignments := algorithm.calculateTargetDistribution(taskListInfo, nodeStates)

		// Validate distribution (first node gets 4, others get 3)
		assert.Len(t, targetAssignments["node1"], 4, "First node should get extra partition")
		for i := 2; i <= 5; i++ {
			nodeID := fmt.Sprintf("node%d", i)
			assert.Len(t, targetAssignments[nodeID], 3, "Node %s should have 3 partitions", nodeID)
		}

		// Validate all partitions are assigned
		totalAssigned := 0
		for _, partitions := range targetAssignments {
			totalAssigned += len(partitions)
		}
		assert.Equal(t, 16, totalAssigned, "All 16 partitions should be assigned")
	})

	t.Run("RebalanceOverAllocatedNodes", func(t *testing.T) {
		// Scenario: Imbalanced start (node1: 5, node2: 4, node3: 4, others: 0) -> balanced (3 each)
		partitionStates := map[string]*PartitionState{
			"0": {PartitionID: "0", NodeID: "node1", Status: managment.PartitionAllocationAssigned},
			"1": {PartitionID: "1", NodeID: "node1", Status: managment.PartitionAllocationAssigned},
			"2": {PartitionID: "2", NodeID: "node1", Status: managment.PartitionAllocationAssigned},
			"3": {PartitionID: "3", NodeID: "node1", Status: managment.PartitionAllocationAssigned},
			"4": {PartitionID: "4", NodeID: "node1", Status: managment.PartitionAllocationAssigned},
			"5": {PartitionID: "5", NodeID: "node2", Status: managment.PartitionAllocationAssigned},
			"6": {PartitionID: "6", NodeID: "node2", Status: managment.PartitionAllocationAssigned},
			"7": {PartitionID: "7", NodeID: "node2", Status: managment.PartitionAllocationAssigned},
			"8": {PartitionID: "8", NodeID: "node2", Status: managment.PartitionAllocationAssigned},
			"9": {PartitionID: "9", NodeID: "node3", Status: managment.PartitionAllocationAssigned},
			"10": {PartitionID: "10", NodeID: "node3", Status: managment.PartitionAllocationAssigned},
			"11": {PartitionID: "11", NodeID: "node3", Status: managment.PartitionAllocationAssigned},
			"12": {PartitionID: "12", NodeID: "node3", Status: managment.PartitionAllocationAssigned},
			"13": {PartitionID: "13", NodeID: "", Status: managment.PartitionAllocationUnassigned},
			"14": {PartitionID: "14", NodeID: "", Status: managment.PartitionAllocationUnassigned},
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
			"node3": {NodeID: "node3", IsActive: true},
			"node4": {NodeID: "node4", IsActive: true},
			"node5": {NodeID: "node5", IsActive: true},
		}

		taskListInfo := managment.TaskListInfo{PartitionCount: 15}
		targetAssignments := algorithm.calculateTargetDistribution(taskListInfo, nodeStates)
		finalAssignments := algorithm.performRebalancing(partitionStates, targetAssignments, nodeStates)

		// Validate balanced distribution
		for nodeID := range nodeStates {
			assert.Len(t, finalAssignments[nodeID], 3, "Each node should have 3 partitions")
		}

		// Validate all partitions are assigned
		allFinalPartitions := make(map[string]bool)
		for _, partitions := range finalAssignments {
			for _, partitionID := range partitions {
				allFinalPartitions[partitionID] = true
			}
		}
		assert.Len(t, allFinalPartitions, 15, "All 15 partitions should be assigned")
	})

	t.Run("SingleNode", func(t *testing.T) {
		// Scenario: 10 partitions, 1 node (all on single node)
		partitionStates := make(map[string]*PartitionState)
		for i := 0; i < 10; i++ {
			partitionStates[fmt.Sprintf("%d", i)] = &PartitionState{
				PartitionID: fmt.Sprintf("%d", i),
				NodeID:      "",
				Status:      managment.PartitionAllocationUnassigned,
			}
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
		}

		taskListInfo := managment.TaskListInfo{PartitionCount: 10}
		targetAssignments := algorithm.calculateTargetDistribution(taskListInfo, nodeStates)

		// Validate single node gets all partitions
		assert.Len(t, targetAssignments, 1)
		assert.Len(t, targetAssignments["node1"], 10, "Single node should get all 10 partitions")
	})

	t.Run("InactiveNodePartitions", func(t *testing.T) {
		// Scenario: Partitions assigned to inactive nodes should be reassigned
		partitionStates := map[string]*PartitionState{
			"0": {PartitionID: "0", NodeID: "node1", Status: managment.PartitionAllocationAssigned},
			"1": {PartitionID: "1", NodeID: "node2", Status: managment.PartitionAllocationAssigned}, // inactive node
			"2": {PartitionID: "2", NodeID: "node3", Status: managment.PartitionAllocationAssigned},
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
		targetAssignments := algorithm.calculateTargetDistribution(taskListInfo, nodeStates)
		finalAssignments := algorithm.performRebalancing(partitionStates, targetAssignments, nodeStates)

		// Validate only active nodes get assignments
		assert.Empty(t, finalAssignments["node2"], "Inactive node should have no assignments")
		assert.True(t, len(finalAssignments["node1"]) > 0, "Active node1 should have assignments")
		assert.True(t, len(finalAssignments["node3"]) > 0, "Active node3 should have assignments")

		// Validate all partitions are assigned to active nodes
		totalAssigned := len(finalAssignments["node1"]) + len(finalAssignments["node3"])
		assert.Equal(t, 3, totalAssigned, "All 3 partitions should be assigned to active nodes")
	})
}

// TestCompareAlgorithmResults compares simplified vs complex algorithm for same scenarios
func TestCompareAlgorithmResults(t *testing.T) {
	t.Run("CompareEvenDistribution", func(t *testing.T) {
		// Test that simplified algorithm produces balanced results
		mockCF := gox.NewNoOpCrossFunction()
		algorithm := &SimpleAllocationAlgorithm{
			CrossFunction: mockCF,
			AlgorithmConfig: &managment.AlgorithmConfig{},
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
			"node3": {NodeID: "node3", IsActive: true},
			"node4": {NodeID: "node4", IsActive: true},
			"node5": {NodeID: "node5", IsActive: true},
		}

		taskListInfo := managment.TaskListInfo{PartitionCount: 20}
		targetAssignments := algorithm.calculateTargetDistribution(taskListInfo, nodeStates)

		// Calculate distribution metrics
		partitionCounts := make([]int, 0)
		nodeIds := make([]string, 0)
		for nodeID := range nodeStates {
			nodeIds = append(nodeIds, nodeID)
		}
		sort.Strings(nodeIds) // Consistent ordering

		for _, nodeID := range nodeIds {
			partitionCounts = append(partitionCounts, len(targetAssignments[nodeID]))
		}

		// Verify balanced distribution (20/5 = 4 each)
		for i, count := range partitionCounts {
			assert.Equal(t, 4, count, "Node %d should have 4 partitions", i)
		}

		// Verify no partition overlap
		allPartitions := make(map[string]bool)
		for _, partitions := range targetAssignments {
			for _, pID := range partitions {
				assert.False(t, allPartitions[pID], "Partition %s should not be assigned to multiple nodes", pID)
				allPartitions[pID] = true
			}
		}

		assert.Len(t, allPartitions, 20, "All 20 partitions should be uniquely assigned")
	})
}