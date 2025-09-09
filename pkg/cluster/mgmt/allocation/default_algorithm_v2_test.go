package allocation

import (
	"testing"
	
	"github.com/devlibx/gox-base/v2"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	"github.com/stretchr/testify/assert"
)

// TestSimpleAlgorithmBasics tests basic functionality of the simplified algorithm
func TestSimpleAlgorithmBasics(t *testing.T) {
	mockCF := gox.NewNoOpCrossFunction()
	algorithm := &SimpleAllocationAlgorithm{
		CrossFunction: mockCF,
		AlgorithmConfig: &managment.AlgorithmConfig{},
	}

	t.Run("CalculateTargetDistribution_EvenPartitions", func(t *testing.T) {
		// 15 partitions across 5 nodes = 3 per node
		taskListInfo := managment.TaskListInfo{PartitionCount: 15}
		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
			"node3": {NodeID: "node3", IsActive: true},
			"node4": {NodeID: "node4", IsActive: true},
			"node5": {NodeID: "node5", IsActive: true},
		}

		target := algorithm.calculateTargetDistribution(taskListInfo, nodeStates)

		// Each node should get exactly 3 partitions
		assert.Len(t, target, 5)
		for nodeID := range nodeStates {
			assert.Len(t, target[nodeID], 3, "Node %s should have 3 partitions", nodeID)
		}
	})

	t.Run("CalculateTargetDistribution_UnevenPartitions", func(t *testing.T) {
		// 16 partitions across 5 nodes = 3 per node + 1 remainder
		taskListInfo := managment.TaskListInfo{PartitionCount: 16}
		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
			"node3": {NodeID: "node3", IsActive: true},
			"node4": {NodeID: "node4", IsActive: true},
			"node5": {NodeID: "node5", IsActive: true},
		}

		target := algorithm.calculateTargetDistribution(taskListInfo, nodeStates)

		// First node gets 4 partitions (3 + 1 remainder), others get 3
		assert.Len(t, target, 5)
		assert.Len(t, target["node1"], 4, "First node should get extra partition")
		assert.Len(t, target["node2"], 3)
		assert.Len(t, target["node3"], 3)
		assert.Len(t, target["node4"], 3)
		assert.Len(t, target["node5"], 3)
	})

	t.Run("ResolveConflicts_DeterministicWinner", func(t *testing.T) {
		// Create simple state (conflict resolution is handled during parsing)
		conflictedStates := map[string]*PartitionState{
			"0": {PartitionID: "0", NodeID: "node2", Status: managment.PartitionAllocationAssigned},
		}

		algorithm.resolveConflicts(conflictedStates)

		// Should maintain the assignment
		assert.Equal(t, "node2", conflictedStates["0"].NodeID)
	})

	t.Run("PerformRebalancing_MinimalMovement", func(t *testing.T) {
		// Current state: node1 has partition 0, node2 has partition 1
		partitionStates := map[string]*PartitionState{
			"0": {PartitionID: "0", NodeID: "node1", Status: managment.PartitionAllocationAssigned},
			"1": {PartitionID: "1", NodeID: "node2", Status: managment.PartitionAllocationAssigned},
			"2": {PartitionID: "2", NodeID: "", Status: managment.PartitionAllocationUnassigned},
		}

		// Target: node1 gets [0], node2 gets [1], node3 gets [2]
		targetAssignments := map[string][]string{
			"node1": {"0"},
			"node2": {"1"},
			"node3": {"2"},
		}

		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: true},
			"node2": {NodeID: "node2", IsActive: true},
			"node3": {NodeID: "node3", IsActive: true},
		}

		final := algorithm.performRebalancing(partitionStates, targetAssignments, nodeStates)

		// Should keep existing assignments and only move unassigned partition
		assert.Contains(t, final["node1"], "0")
		assert.Contains(t, final["node2"], "1") 
		assert.Contains(t, final["node3"], "2")
		assert.Len(t, final["node1"], 1)
		assert.Len(t, final["node2"], 1)
		assert.Len(t, final["node3"], 1)
	})

	t.Run("EmptyCluster", func(t *testing.T) {
		taskListInfo := managment.TaskListInfo{PartitionCount: 0}
		nodeStates := map[string]*NodeState{}

		target := algorithm.calculateTargetDistribution(taskListInfo, nodeStates)
		assert.Empty(t, target)
	})

	t.Run("NoActiveNodes", func(t *testing.T) {
		taskListInfo := managment.TaskListInfo{PartitionCount: 10}
		nodeStates := map[string]*NodeState{
			"node1": {NodeID: "node1", IsActive: false},
			"node2": {NodeID: "node2", IsActive: false},
		}

		target := algorithm.calculateTargetDistribution(taskListInfo, nodeStates)
		assert.Empty(t, target)
	})
}