package allocation

import (
	"testing"
	"time"

	"github.com/devlibx/gox-base/v2"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	"github.com/stretchr/testify/assert"
)

// TestHelperV1 provides utilities for creating test data for V1 algorithm
type TestHelperV1 struct {
	algorithm *RebalanceAlgoV1
}

func NewTestHelperV1() *TestHelperV1 {
	mockCF := gox.NewNoOpCrossFunction()
	algorithm := NewRebalanceAlgoV1(mockCF, &managment.AlgorithmConfig{
		TimeToWaitForPartitionReleaseBeforeForceRelease: 5 * time.Minute,
	})
	return &TestHelperV1{algorithm: algorithm}
}

// createPartitionV1 creates a partition with the given parameters for V1 tests
func (h *TestHelperV1) createPartitionV1(partitionId, nodeId string, status managment.PartitionAllocationStatus) *nodePartitionMapping {
	return &nodePartitionMapping{
		OwnerNode:   nodeId,
		Partition:   partitionId,
		Status:      status,
		UpdatedTime: time.Now(),
	}
}

// createNodesV1 creates active nodes with the given node IDs for V1 tests
func (h *TestHelperV1) createNodesV1(nodeIds []string) []*helixClusterMysql.GetActiveNodesRow {
	nodes := make([]*helixClusterMysql.GetActiveNodesRow, len(nodeIds))
	for i, nodeId := range nodeIds {
		nodes[i] = &helixClusterMysql.GetActiveNodesRow{
			NodeUuid: nodeId,
			Status:   helixClusterMysql.NodeStatusActive,
		}
	}
	return nodes
}

// TestBuildAllocationState tests the buildAllocationState method
func TestBuildAllocationState(t *testing.T) {
	helper := NewTestHelperV1()

	t.Run("EmptyState", func(t *testing.T) {
		taskListInfo := managment.TaskListInfo{PartitionCount: 0}
		nodePartitionMappings := map[string]*nodePartitionMapping{}
		nodes := []*helixClusterMysql.GetActiveNodesRow{}

		state := helper.algorithm.buildAllocationState(taskListInfo, nodePartitionMappings, nodes)

		assert.Empty(t, state.ActiveNodeIds)
		assert.Equal(t, 0, state.TargetPerNode)
		assert.Equal(t, 0, state.ExtraPartitions)
	})

	t.Run("BasicBalancedState", func(t *testing.T) {
		// 3 nodes, 9 partitions (perfectly divisible)
		taskListInfo := managment.TaskListInfo{PartitionCount: 9}
		nodePartitionMappings := map[string]*nodePartitionMapping{
			"0": helper.createPartitionV1("0", "node1", managment.PartitionAllocationAssigned),
			"1": helper.createPartitionV1("1", "node1", managment.PartitionAllocationAssigned),
			"2": helper.createPartitionV1("2", "node1", managment.PartitionAllocationAssigned),
			"3": helper.createPartitionV1("3", "node2", managment.PartitionAllocationAssigned),
			"4": helper.createPartitionV1("4", "node2", managment.PartitionAllocationAssigned),
			"5": helper.createPartitionV1("5", "node2", managment.PartitionAllocationAssigned),
			"6": helper.createPartitionV1("6", "node3", managment.PartitionAllocationAssigned),
			"7": helper.createPartitionV1("7", "node3", managment.PartitionAllocationAssigned),
			"8": helper.createPartitionV1("8", "node3", managment.PartitionAllocationAssigned),
		}
		nodes := helper.createNodesV1([]string{"node1", "node2", "node3"})

		state := helper.algorithm.buildAllocationState(taskListInfo, nodePartitionMappings, nodes)

		assert.Equal(t, []string{"node1", "node2", "node3"}, state.ActiveNodeIds)
		assert.Equal(t, 3, state.TargetPerNode)
		assert.Equal(t, 0, state.ExtraPartitions)
		assert.Equal(t, 3, state.NodeCurrentCount["node1"])
		assert.Equal(t, 3, state.NodeCurrentCount["node2"])
		assert.Equal(t, 3, state.NodeCurrentCount["node3"])
		assert.Empty(t, state.UnassignedPartitions)
		assert.Empty(t, state.NewNodes)
		assert.Empty(t, state.OverAllocatedNodes)
		assert.Empty(t, state.UnderAllocatedNodes)
	})

	t.Run("ImbalancedStateWithNewNode", func(t *testing.T) {
		// User's exact scenario: 3 nodes with partitions 1-5, 6-9, 10-13 (14 partitions)
		// Adding nodes 4,5 should show imbalance
		taskListInfo := managment.TaskListInfo{PartitionCount: 14}
		nodePartitionMappings := map[string]*nodePartitionMapping{
			"1":  helper.createPartitionV1("1", "node1", managment.PartitionAllocationAssigned),
			"2":  helper.createPartitionV1("2", "node1", managment.PartitionAllocationAssigned),
			"3":  helper.createPartitionV1("3", "node1", managment.PartitionAllocationAssigned),
			"4":  helper.createPartitionV1("4", "node1", managment.PartitionAllocationAssigned),
			"5":  helper.createPartitionV1("5", "node1", managment.PartitionAllocationAssigned),
			"6":  helper.createPartitionV1("6", "node2", managment.PartitionAllocationAssigned),
			"7":  helper.createPartitionV1("7", "node2", managment.PartitionAllocationAssigned),
			"8":  helper.createPartitionV1("8", "node2", managment.PartitionAllocationAssigned),
			"9":  helper.createPartitionV1("9", "node2", managment.PartitionAllocationAssigned),
			"10": helper.createPartitionV1("10", "node3", managment.PartitionAllocationAssigned),
			"11": helper.createPartitionV1("11", "node3", managment.PartitionAllocationAssigned),
			"12": helper.createPartitionV1("12", "node3", managment.PartitionAllocationAssigned),
			"13": helper.createPartitionV1("13", "node3", managment.PartitionAllocationAssigned),
		}
		nodes := helper.createNodesV1([]string{"node1", "node2", "node3", "node4", "node5"})

		state := helper.algorithm.buildAllocationState(taskListInfo, nodePartitionMappings, nodes)

		assert.Equal(t, 5, len(state.ActiveNodeIds))
		assert.Equal(t, 2, state.TargetPerNode)   // 14/5 = 2
		assert.Equal(t, 4, state.ExtraPartitions) // 14%5 = 4

		// Node counts
		assert.Equal(t, 5, state.NodeCurrentCount["node1"]) // Over-allocated
		assert.Equal(t, 4, state.NodeCurrentCount["node2"]) // Over-allocated 
		assert.Equal(t, 4, state.NodeCurrentCount["node3"]) // Over-allocated
		assert.Equal(t, 0, state.NodeCurrentCount["node4"]) // New node
		assert.Equal(t, 0, state.NodeCurrentCount["node5"]) // New node

		// Node categorization
		assert.Contains(t, state.NewNodes, "node4")
		assert.Contains(t, state.NewNodes, "node5")
		assert.Contains(t, state.OverAllocatedNodes, "node1")
		assert.Contains(t, state.OverAllocatedNodes, "node2")
		assert.Contains(t, state.OverAllocatedNodes, "node3")
	})

	t.Run("UnassignedPartitions", func(t *testing.T) {
		taskListInfo := managment.TaskListInfo{PartitionCount: 5}
		nodePartitionMappings := map[string]*nodePartitionMapping{
			"0": helper.createPartitionV1("0", "node1", managment.PartitionAllocationAssigned),
			"1": helper.createPartitionV1("1", "", managment.PartitionAllocationUnassigned),
			"2": helper.createPartitionV1("2", "", managment.PartitionAllocationUnassigned),
			"3": helper.createPartitionV1("3", "node2", managment.PartitionAllocationAssigned),
			"4": helper.createPartitionV1("4", "node3", managment.PartitionAllocationAssigned),
		}
		nodes := helper.createNodesV1([]string{"node1", "node2", "node3"})

		state := helper.algorithm.buildAllocationState(taskListInfo, nodePartitionMappings, nodes)

		assert.Equal(t, 2, len(state.UnassignedPartitions))
		assert.Equal(t, 1, state.NodeCurrentCount["node1"])
		assert.Equal(t, 1, state.NodeCurrentCount["node2"])
		assert.Equal(t, 1, state.NodeCurrentCount["node3"])
	})

	t.Run("PlaceholderPartitions", func(t *testing.T) {
		taskListInfo := managment.TaskListInfo{PartitionCount: 6}
		nodePartitionMappings := map[string]*nodePartitionMapping{
			"0": helper.createPartitionV1("0", "node1", managment.PartitionAllocationAssigned),
			"1": helper.createPartitionV1("1", "node1", managment.PartitionAllocationAssigned),
			"2": helper.createPartitionV1("2", "node2", managment.PartitionAllocationPlaceholder),
			"3": helper.createPartitionV1("3", "node2", managment.PartitionAllocationPlaceholder),
			"4": helper.createPartitionV1("4", "node3", managment.PartitionAllocationAssigned),
			"5": helper.createPartitionV1("5", "node3", managment.PartitionAllocationAssigned),
		}
		nodes := helper.createNodesV1([]string{"node1", "node2", "node3"})

		state := helper.algorithm.buildAllocationState(taskListInfo, nodePartitionMappings, nodes)

		// Placeholders should count toward active load
		assert.Equal(t, 2, state.NodeCurrentCount["node1"])
		assert.Equal(t, 2, state.NodeCurrentCount["node2"])
		assert.Equal(t, 2, state.NodeCurrentCount["node3"])
	})
}

// TestExecuteGracefulMigration tests the executeGracefulMigration method
func TestExecuteGracefulMigration(t *testing.T) {
	helper := NewTestHelperV1()

	t.Run("NoMigrationNeeded", func(t *testing.T) {
		// Balanced scenario with no new nodes
		taskListInfo := managment.TaskListInfo{PartitionCount: 9}
		nodePartitionMappings := map[string]*nodePartitionMapping{
			"0": helper.createPartitionV1("0", "node1", managment.PartitionAllocationAssigned),
			"1": helper.createPartitionV1("1", "node1", managment.PartitionAllocationAssigned),
			"2": helper.createPartitionV1("2", "node1", managment.PartitionAllocationAssigned),
		}
		nodes := helper.createNodesV1([]string{"node1", "node2", "node3"})
		state := helper.algorithm.buildAllocationState(taskListInfo, nodePartitionMappings, nodes)

		originalMappingCount := len(nodePartitionMappings)
		helper.algorithm.executeGracefulMigration(nodePartitionMappings, state)

		// Should not create any placeholders
		assert.Equal(t, originalMappingCount, len(nodePartitionMappings))
	})

	t.Run("TwoPhaseGracefulMigration", func(t *testing.T) {
		// User's exact scenario: imbalanced 3 nodes + 2 new nodes
		taskListInfo := managment.TaskListInfo{PartitionCount: 14}
		nodePartitionMappings := map[string]*nodePartitionMapping{
			"1":  helper.createPartitionV1("1", "node1", managment.PartitionAllocationAssigned),
			"2":  helper.createPartitionV1("2", "node1", managment.PartitionAllocationAssigned),
			"3":  helper.createPartitionV1("3", "node1", managment.PartitionAllocationAssigned),
			"4":  helper.createPartitionV1("4", "node1", managment.PartitionAllocationAssigned),
			"5":  helper.createPartitionV1("5", "node1", managment.PartitionAllocationAssigned),
			"6":  helper.createPartitionV1("6", "node2", managment.PartitionAllocationAssigned),
			"7":  helper.createPartitionV1("7", "node2", managment.PartitionAllocationAssigned),
			"8":  helper.createPartitionV1("8", "node2", managment.PartitionAllocationAssigned),
			"9":  helper.createPartitionV1("9", "node2", managment.PartitionAllocationAssigned),
			"10": helper.createPartitionV1("10", "node3", managment.PartitionAllocationAssigned),
			"11": helper.createPartitionV1("11", "node3", managment.PartitionAllocationAssigned),
			"12": helper.createPartitionV1("12", "node3", managment.PartitionAllocationAssigned),
			"13": helper.createPartitionV1("13", "node3", managment.PartitionAllocationAssigned),
		}
		nodes := helper.createNodesV1([]string{"node1", "node2", "node3", "node4", "node5"})
		state := helper.algorithm.buildAllocationState(taskListInfo, nodePartitionMappings, nodes)

		originalMappingCount := len(nodePartitionMappings)
		helper.algorithm.executeGracefulMigration(nodePartitionMappings, state)

		// Should create placeholders for graceful migration
		assert.Greater(t, len(nodePartitionMappings), originalMappingCount)

		// Count partitions with RequestedRelease status
		releaseCount := 0
		placeholderCount := 0
		for _, partition := range nodePartitionMappings {
			if partition.Status == managment.PartitionAllocationRequestedRelease {
				releaseCount++
			} else if partition.Status == managment.PartitionAllocationPlaceholder {
				placeholderCount++
			}
		}

		// Should have some partitions marked for release and matching placeholders
		assert.Greater(t, releaseCount, 0)
		assert.Greater(t, placeholderCount, 0)
		assert.Equal(t, releaseCount, placeholderCount) // Should match 1:1

		// State counts should be updated
		assert.Greater(t, state.NodeCurrentCount["node4"], 0) // New node should have some placeholders
		assert.Greater(t, state.NodeCurrentCount["node5"], 0) // New node should have some placeholders
	})
}

// TestAssignUnassignedPartitions tests the assignUnassignedPartitions method
func TestAssignUnassignedPartitions(t *testing.T) {
	helper := NewTestHelperV1()

	t.Run("AssignToLowestCountNode", func(t *testing.T) {
		taskListInfo := managment.TaskListInfo{PartitionCount: 5}
		nodePartitionMappings := map[string]*nodePartitionMapping{
			"0": helper.createPartitionV1("0", "node1", managment.PartitionAllocationAssigned),
			"1": helper.createPartitionV1("1", "node1", managment.PartitionAllocationAssigned),
			"2": helper.createPartitionV1("2", "", managment.PartitionAllocationUnassigned),
			"3": helper.createPartitionV1("3", "", managment.PartitionAllocationUnassigned),
			"4": helper.createPartitionV1("4", "node2", managment.PartitionAllocationAssigned),
		}
		nodes := helper.createNodesV1([]string{"node1", "node2", "node3"})
		state := helper.algorithm.buildAllocationState(taskListInfo, nodePartitionMappings, nodes)

		// Before assignment
		assert.Equal(t, 2, len(state.UnassignedPartitions))
		assert.Equal(t, 0, state.NodeCurrentCount["node3"]) // Lowest count

		helper.algorithm.assignUnassignedPartitions(nodePartitionMappings, state)

		// After assignment
		assert.Equal(t, 0, len(state.UnassignedPartitions)) // All should be assigned
		// With 5 partitions, 3 nodes: targets are [2,2,1], so node3 should get 1, node2 should get 1
		assert.True(t, state.NodeCurrentCount["node3"] >= 1) // Should get at least one partition

		// Check that partitions are actually assigned
		for _, partition := range nodePartitionMappings {
			assert.NotEqual(t, managment.PartitionAllocationUnassigned, partition.Status)
			assert.NotEmpty(t, partition.OwnerNode)
		}
	})
}

// TestBalanceRemainingImbalances tests the balanceRemainingImbalances method
func TestBalanceRemainingImbalances(t *testing.T) {
	helper := NewTestHelperV1()

	t.Run("SkipDuringGracefulMigration", func(t *testing.T) {
		taskListInfo := managment.TaskListInfo{PartitionCount: 6}
		nodePartitionMappings := map[string]*nodePartitionMapping{
			"0": helper.createPartitionV1("0", "node1", managment.PartitionAllocationAssigned),
			"1": helper.createPartitionV1("1", "node1", managment.PartitionAllocationAssigned),
			"2": helper.createPartitionV1("2", "node1", managment.PartitionAllocationAssigned),
			"3": helper.createPartitionV1("3", "node2", managment.PartitionAllocationRequestedRelease),
			"4": helper.createPartitionV1("4", "node3", managment.PartitionAllocationAssigned),
			"5": helper.createPartitionV1("5", "node3", managment.PartitionAllocationAssigned),
		}
		nodes := helper.createNodesV1([]string{"node1", "node2", "node3"})
		state := helper.algorithm.buildAllocationState(taskListInfo, nodePartitionMappings, nodes)

		// Should not balance when partitions are in release state
		originalCounts := make(map[string]int)
		for nodeId, count := range state.NodeCurrentCount {
			originalCounts[nodeId] = count
		}

		helper.algorithm.balanceRemainingImbalances(nodePartitionMappings, state)

		// Counts should remain unchanged
		for nodeId, originalCount := range originalCounts {
			assert.Equal(t, originalCount, state.NodeCurrentCount[nodeId])
		}
	})

	t.Run("BalanceOverAllocatedNodes", func(t *testing.T) {
		taskListInfo := managment.TaskListInfo{PartitionCount: 6}
		nodePartitionMappings := map[string]*nodePartitionMapping{
			"0": helper.createPartitionV1("0", "node1", managment.PartitionAllocationAssigned),
			"1": helper.createPartitionV1("1", "node1", managment.PartitionAllocationAssigned),
			"2": helper.createPartitionV1("2", "node1", managment.PartitionAllocationAssigned),
			"3": helper.createPartitionV1("3", "node1", managment.PartitionAllocationAssigned), // Over-allocated
			"4": helper.createPartitionV1("4", "node2", managment.PartitionAllocationAssigned),
			"5": helper.createPartitionV1("5", "node3", managment.PartitionAllocationAssigned),
		}
		nodes := helper.createNodesV1([]string{"node1", "node2", "node3"})
		state := helper.algorithm.buildAllocationState(taskListInfo, nodePartitionMappings, nodes)

		// Before balancing
		assert.Equal(t, 4, state.NodeCurrentCount["node1"]) // Over-allocated
		assert.Equal(t, 1, state.NodeCurrentCount["node2"]) // Under-allocated
		assert.Equal(t, 1, state.NodeCurrentCount["node3"]) // Under-allocated

		helper.algorithm.balanceRemainingImbalances(nodePartitionMappings, state)

		// After balancing - should be more balanced
		assert.True(t, state.NodeCurrentCount["node1"] <= 2) // Should be reduced
		assert.True(t, state.NodeCurrentCount["node2"] >= 2) // Should be increased
		assert.True(t, state.NodeCurrentCount["node3"] >= 2) // Should be increased
	})
}

// TestDistributeWorkIntegration tests the full DistributeWork workflow
func TestDistributeWorkIntegration(t *testing.T) {
	helper := NewTestHelperV1()

	t.Run("UserScenarioTwoPhaseRebalancing", func(t *testing.T) {
		// User's exact scenario: 3 nodes (1-5, 6-9, 10-13) adding nodes 4,5
		taskListInfo := managment.TaskListInfo{PartitionCount: 14}
		nodePartitionMappings := map[string]*nodePartitionMapping{
			"1":  helper.createPartitionV1("1", "node1", managment.PartitionAllocationAssigned),
			"2":  helper.createPartitionV1("2", "node1", managment.PartitionAllocationAssigned),
			"3":  helper.createPartitionV1("3", "node1", managment.PartitionAllocationAssigned),
			"4":  helper.createPartitionV1("4", "node1", managment.PartitionAllocationAssigned),
			"5":  helper.createPartitionV1("5", "node1", managment.PartitionAllocationAssigned),
			"6":  helper.createPartitionV1("6", "node2", managment.PartitionAllocationAssigned),
			"7":  helper.createPartitionV1("7", "node2", managment.PartitionAllocationAssigned),
			"8":  helper.createPartitionV1("8", "node2", managment.PartitionAllocationAssigned),
			"9":  helper.createPartitionV1("9", "node2", managment.PartitionAllocationAssigned),
			"10": helper.createPartitionV1("10", "node3", managment.PartitionAllocationAssigned),
			"11": helper.createPartitionV1("11", "node3", managment.PartitionAllocationAssigned),
			"12": helper.createPartitionV1("12", "node3", managment.PartitionAllocationAssigned),
			"13": helper.createPartitionV1("13", "node3", managment.PartitionAllocationAssigned),
		}
		nodes := helper.createNodesV1([]string{"node1", "node2", "node3", "node4", "node5"})

		result := helper.algorithm.DistributeWork(taskListInfo, nodePartitionMappings, nodes)

		// Validate that graceful migration occurred
		releaseCount := 0
		placeholderCount := 0
		assignedCount := 0

		for _, partition := range result {
			switch partition.Status {
			case managment.PartitionAllocationRequestedRelease:
				releaseCount++
			case managment.PartitionAllocationPlaceholder:
				placeholderCount++
			case managment.PartitionAllocationAssigned:
				assignedCount++
			}
		}

		// Should have two-phase migration in progress
		assert.Greater(t, releaseCount, 0, "Should have partitions marked for release")
		assert.Greater(t, placeholderCount, 0, "Should have placeholder partitions")
		assert.Equal(t, releaseCount, placeholderCount, "Release and placeholder counts should match")

		// During two-phase migration, we should have most partitions in assigned/release state
		// The exact count might vary due to algorithm implementation details
		totalAccountedPartitions := assignedCount + releaseCount
		assert.True(t, totalAccountedPartitions >= 13, "Should account for at least 13 of 14 partitions in assigned/release state")
		assert.True(t, totalAccountedPartitions <= 14, "Should not exceed total partition count")
	})
}

// TestHelperMethods tests various helper methods
func TestHelperMethods(t *testing.T) {
	helper := NewTestHelperV1()

	t.Run("GetTargetForNode", func(t *testing.T) {
		// 14 partitions, 5 nodes: targetPerNode=2, extraPartitions=4
		// First 4 nodes get 3, last node gets 2
		assert.Equal(t, 3, helper.algorithm.getTargetForNode(0, 2, 4)) // First node
		assert.Equal(t, 3, helper.algorithm.getTargetForNode(1, 2, 4)) // Second node
		assert.Equal(t, 3, helper.algorithm.getTargetForNode(2, 2, 4)) // Third node
		assert.Equal(t, 3, helper.algorithm.getTargetForNode(3, 2, 4)) // Fourth node
		assert.Equal(t, 2, helper.algorithm.getTargetForNode(4, 2, 4)) // Fifth node
	})

	t.Run("HasPartitionsInReleaseState", func(t *testing.T) {
		nodePartitionMappings := map[string]*nodePartitionMapping{
			"0": helper.createPartitionV1("0", "node1", managment.PartitionAllocationAssigned),
			"1": helper.createPartitionV1("1", "node2", managment.PartitionAllocationRequestedRelease),
		}

		assert.True(t, helper.algorithm.hasPartitionsInReleaseState(nodePartitionMappings))

		nodePartitionMappings["1"].Status = managment.PartitionAllocationAssigned
		assert.False(t, helper.algorithm.hasPartitionsInReleaseState(nodePartitionMappings))
	})

	t.Run("FindMinimumCountNode", func(t *testing.T) {
		state := &NodeAllocationState{
			ActiveNodeIds:   []string{"node1", "node2", "node3"},
			TargetPerNode:   2,
			ExtraPartitions: 1,
			NodeCurrentCount: map[string]int{
				"node1": 3, // Target = 3 (gets extra)
				"node2": 1, // Target = 2 (under target)
				"node3": 2, // Target = 2 (at target)
			},
		}

		// Prefer under target
		minNode := helper.algorithm.findMinimumCountNode(state, true)
		assert.Equal(t, "node2", minNode) // Only under-target node

		// Any minimum
		state.NodeCurrentCount["node2"] = 5 // Make node2 over target
		minNode = helper.algorithm.findMinimumCountNode(state, false)
		assert.Equal(t, "node3", minNode) // Lowest count overall
	})
}