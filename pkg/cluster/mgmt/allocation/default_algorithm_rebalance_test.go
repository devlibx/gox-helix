package allocation

import (
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/devlibx/gox-base/v2"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	"github.com/stretchr/testify/assert"
)

// TestHelper provides utilities for creating test data and visualizing results
type TestHelper struct {
	algorithm *defaultAlgorithm
}

func NewTestHelper() *TestHelper {
	mockCF := gox.NewNoOpCrossFunction()
	algorithm := &defaultAlgorithm{
		CrossFunction: mockCF,
		AlgorithmConfig: &managment.AlgorithmConfig{
			TimeToWaitForPartitionReleaseBeforeForceRelease: 5 * time.Minute,
		},
	}
	return &TestHelper{algorithm: algorithm}
}

// createPartition creates a partition with the given parameters
func (h *TestHelper) createPartition(partitionId, nodeId string, status managment.PartitionAllocationStatus) *nodePartitionMapping {
	return &nodePartitionMapping{
		OwnerNode:   nodeId,
		Partition:   partitionId,
		Status:      status,
		UpdatedTime: time.Now(),
	}
}

// createNodes creates active nodes with the given node IDs
func (h *TestHelper) createNodes(nodeIds []string) []*helixClusterMysql.GetActiveNodesRow {
	nodes := make([]*helixClusterMysql.GetActiveNodesRow, len(nodeIds))
	for i, nodeId := range nodeIds {
		nodes[i] = &helixClusterMysql.GetActiveNodesRow{
			NodeUuid: nodeId,
			Status:   helixClusterMysql.NodeStatusActive,
		}
	}
	return nodes
}

// printAssignments provides human-friendly visualization of partition assignments
func (h *TestHelper) printAssignments(title string, nodePartitionMappings map[string]*nodePartitionMapping, nodes []*helixClusterMysql.GetActiveNodesRow) {
	fmt.Printf("\n=== %s ===\n", title)
	
	// Get all active node IDs
	nodeIds := make([]string, 0)
	for _, node := range nodes {
		if node.Status == helixClusterMysql.NodeStatusActive {
			nodeIds = append(nodeIds, node.NodeUuid)
		}
	}
	sort.Strings(nodeIds)
	
	// Group partitions by node
	nodeAssignments := make(map[string][]string)
	nodeActiveCounts := make(map[string]int)
	nodeReleaseCounts := make(map[string]int)
	unassignedPartitions := make([]string, 0)
	
	// Initialize maps
	for _, nodeId := range nodeIds {
		nodeAssignments[nodeId] = make([]string, 0)
		nodeActiveCounts[nodeId] = 0
		nodeReleaseCounts[nodeId] = 0
	}
	
	// Categorize partitions
	partitionIds := make([]string, 0, len(nodePartitionMappings))
	for partitionId := range nodePartitionMappings {
		partitionIds = append(partitionIds, partitionId)
	}
	sort.Strings(partitionIds)
	
	for _, partitionId := range partitionIds {
		partition := nodePartitionMappings[partitionId]
		
		switch partition.Status {
		case managment.PartitionAllocationAssigned:
			nodeAssignments[partition.OwnerNode] = append(nodeAssignments[partition.OwnerNode], 
				fmt.Sprintf("P%s(A)", partition.Partition))
			nodeActiveCounts[partition.OwnerNode]++
		case managment.PartitionAllocationRequestedRelease:
			nodeAssignments[partition.OwnerNode] = append(nodeAssignments[partition.OwnerNode], 
				fmt.Sprintf("P%s(RR)", partition.Partition))
			nodeReleaseCounts[partition.OwnerNode]++
		case managment.PartitionAllocationPendingRelease:
			nodeAssignments[partition.OwnerNode] = append(nodeAssignments[partition.OwnerNode], 
				fmt.Sprintf("P%s(PR)", partition.Partition))
			nodeReleaseCounts[partition.OwnerNode]++
		case managment.PartitionAllocationUnassigned:
			unassignedPartitions = append(unassignedPartitions, fmt.Sprintf("P%s(U)", partition.Partition))
		}
	}
	
	// Print assignments
	totalActivePartitions := 0
	totalReleasePartitions := 0
	
	for _, nodeId := range nodeIds {
		assignments := nodeAssignments[nodeId]
		sort.Strings(assignments)
		activeCount := nodeActiveCounts[nodeId]
		releaseCount := nodeReleaseCounts[nodeId]
		totalCount := activeCount + releaseCount
		
		fmt.Printf("Node %s: [%s] (Active: %d, Release: %d, Total: %d)\n", 
			nodeId, 
			fmt.Sprintf("%v", assignments),
			activeCount, 
			releaseCount,
			totalCount)
		
		totalActivePartitions += activeCount
		totalReleasePartitions += releaseCount
	}
	
	if len(unassignedPartitions) > 0 {
		fmt.Printf("Unassigned: [%s] (Count: %d)\n", 
			fmt.Sprintf("%v", unassignedPartitions), 
			len(unassignedPartitions))
	}
	
	totalPartitions := totalActivePartitions + totalReleasePartitions + len(unassignedPartitions)
	fmt.Printf("Summary: Active=%d, Release=%d, Unassigned=%d, Total=%d\n", 
		totalActivePartitions, totalReleasePartitions, len(unassignedPartitions), totalPartitions)
		
	// Legend
	fmt.Printf("Legend: (A)=Assigned, (RR)=RequestedRelease, (PR)=PendingRelease, (U)=Unassigned\n")
}

// validateNoPartitionsLost ensures all partitions are accounted for
func (h *TestHelper) validateNoPartitionsLost(t *testing.T, beforeMappings, afterMappings map[string]*nodePartitionMapping) {
	beforePartitions := make(map[string]bool)
	afterPartitions := make(map[string]bool)
	
	for partitionId := range beforeMappings {
		beforePartitions[partitionId] = true
	}
	
	for partitionId := range afterMappings {
		afterPartitions[partitionId] = true
	}
	
	// Check no partitions were lost
	for partitionId := range beforePartitions {
		assert.True(t, afterPartitions[partitionId], "Partition %s was lost during rebalancing", partitionId)
	}
	
	// Check no partitions were added unexpectedly
	for partitionId := range afterPartitions {
		assert.True(t, beforePartitions[partitionId], "Partition %s was unexpectedly added during rebalancing", partitionId)
	}
	
	assert.Equal(t, len(beforePartitions), len(afterPartitions), "Total partition count changed")
}

// runDistributeWorkTest executes the distributeWork method and provides visualization
func (h *TestHelper) runDistributeWorkTest(t *testing.T, testName string, taskListInfo managment.TaskListInfo, 
	nodePartitionMappings map[string]*nodePartitionMapping, nodes []*helixClusterMysql.GetActiveNodesRow) {
	
	fmt.Printf("\n" + strings.Repeat("=", 80))
	fmt.Printf("\nTEST: %s", testName)
	fmt.Printf("\n" + strings.Repeat("=", 80))
	
	// Create a deep copy for before state
	beforeMappings := make(map[string]*nodePartitionMapping)
	for id, mapping := range nodePartitionMappings {
		beforeMappings[id] = &nodePartitionMapping{
			OwnerNode:   mapping.OwnerNode,
			Partition:   mapping.Partition,
			Status:      mapping.Status,
			UpdatedTime: mapping.UpdatedTime,
		}
	}
	
	// Print before state
	h.printAssignments("BEFORE REBALANCING", beforeMappings, nodes)
	
	// Execute the algorithm
	updatedMappings := h.algorithm.distributeWork(taskListInfo, nodePartitionMappings, nodes)
	
	// Copy the result back to nodePartitionMappings for consistency with existing test logic
	for id, mapping := range updatedMappings {
		nodePartitionMappings[id] = mapping
	}
	
	// Print after state
	h.printAssignments("AFTER REBALANCING", nodePartitionMappings, nodes)
	
	// Validate no partitions were lost
	h.validateNoPartitionsLost(t, beforeMappings, nodePartitionMappings)
}

// Test 1: Basic even distribution - 15 partitions across 5 nodes (3 each)
func TestDistributeWork_BasicEvenDistribution(t *testing.T) {
	helper := NewTestHelper()
	
	taskListInfo := managment.TaskListInfo{
		PartitionCount: 15,
	}
	
	nodes := helper.createNodes([]string{"node1", "node2", "node3", "node4", "node5"})
	
	// Initial state: All partitions unassigned
	nodePartitionMappings := make(map[string]*nodePartitionMapping)
	for i := 0; i < 15; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "", managment.PartitionAllocationUnassigned)
	}
	
	helper.runDistributeWorkTest(t, "Basic Even Distribution", taskListInfo, nodePartitionMappings, nodes)
	
	// Verify results
	nodeCounts := make(map[string]int)
	for _, partition := range nodePartitionMappings {
		if partition.Status == managment.PartitionAllocationAssigned {
			nodeCounts[partition.OwnerNode]++
		}
	}
	
	for nodeId, count := range nodeCounts {
		assert.Equal(t, 3, count, "Node %s should have exactly 3 partitions", nodeId)
	}
	assert.Equal(t, 5, len(nodeCounts), "All 5 nodes should have partitions")
}

// Test 2: Uneven distribution with remainder - 16 partitions across 5 nodes (3-4 each)
func TestDistributeWork_UnevenDistributionWithRemainder(t *testing.T) {
	helper := NewTestHelper()
	
	taskListInfo := managment.TaskListInfo{
		PartitionCount: 16,
	}
	
	nodes := helper.createNodes([]string{"node1", "node2", "node3", "node4", "node5"})
	
	// Initial state: All partitions unassigned
	nodePartitionMappings := make(map[string]*nodePartitionMapping)
	for i := 0; i < 16; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "", managment.PartitionAllocationUnassigned)
	}
	
	helper.runDistributeWorkTest(t, "Uneven Distribution With Remainder", taskListInfo, nodePartitionMappings, nodes)
	
	// Verify results - some nodes get 4, others get 3
	nodeCounts := make(map[string]int)
	for _, partition := range nodePartitionMappings {
		if partition.Status == managment.PartitionAllocationAssigned {
			nodeCounts[partition.OwnerNode]++
		}
	}
	
	nodes4 := 0
	nodes3 := 0
	for _, count := range nodeCounts {
		if count == 4 {
			nodes4++
		} else if count == 3 {
			nodes3++
		} else {
			t.Errorf("Unexpected partition count: %d", count)
		}
	}
	
	assert.Equal(t, 1, nodes4, "Exactly 1 node should have 4 partitions")
	assert.Equal(t, 4, nodes3, "Exactly 4 nodes should have 3 partitions")
}

// Test 3: Rebalancing over-allocated nodes - your specific example
func TestDistributeWork_RebalanceOverAllocatedNodes(t *testing.T) {
	helper := NewTestHelper()
	
	taskListInfo := managment.TaskListInfo{
		PartitionCount: 15,
	}
	
	nodes := helper.createNodes([]string{"node1", "node2", "node3", "node4", "node5"})
	
	// Initial state: node1 and node2 have 5 partitions each, others have 0
	nodePartitionMappings := make(map[string]*nodePartitionMapping)
	
	// Assign first 10 partitions to node1 and node2
	for i := 0; i < 10; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodeId := "node1"
		if i >= 5 {
			nodeId = "node2"
		}
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, nodeId, managment.PartitionAllocationAssigned)
	}
	
	// Remaining 5 partitions unassigned
	for i := 10; i < 15; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "", managment.PartitionAllocationUnassigned)
	}
	
	helper.runDistributeWorkTest(t, "Rebalance Over-allocated Nodes", taskListInfo, nodePartitionMappings, nodes)
	
	// Verify all nodes have exactly 3 partitions
	nodeCounts := make(map[string]int)
	for _, partition := range nodePartitionMappings {
		if partition.Status == managment.PartitionAllocationAssigned {
			nodeCounts[partition.OwnerNode]++
		}
	}
	
	for nodeId, count := range nodeCounts {
		assert.Equal(t, 3, count, "Node %s should have exactly 3 partitions", nodeId)
	}
}

// Test 4: Partitions in release states are not counted or reassigned
func TestDistributeWork_ReleaseStatePartitions(t *testing.T) {
	helper := NewTestHelper()
	
	taskListInfo := managment.TaskListInfo{
		PartitionCount: 15,
	}
	
	nodes := helper.createNodes([]string{"node1", "node2", "node3", "node4", "node5"})
	
	nodePartitionMappings := make(map[string]*nodePartitionMapping)
	
	// node1: 3 assigned + 2 pending release (should count as having 3 active)
	for i := 0; i < 3; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "node1", managment.PartitionAllocationAssigned)
	}
	for i := 3; i < 5; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "node1", managment.PartitionAllocationPendingRelease)
	}
	
	// node2: 2 assigned + 3 requested release (should count as having 2 active)
	for i := 5; i < 7; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "node2", managment.PartitionAllocationAssigned)
	}
	for i := 7; i < 10; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "node2", managment.PartitionAllocationRequestedRelease)
	}
	
	// Remaining 5 partitions unassigned
	for i := 10; i < 15; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "", managment.PartitionAllocationUnassigned)
	}
	
	helper.runDistributeWorkTest(t, "Release State Partitions", taskListInfo, nodePartitionMappings, nodes)
	
	// Verify release state partitions stayed with their original nodes and weren't reassigned
	releasePartitions := []string{"3", "4", "7", "8", "9"}
	for _, partitionId := range releasePartitions {
		partition := nodePartitionMappings[partitionId]
		originalNode := "node1"
		if partitionId >= "7" {
			originalNode = "node2"
		}
		assert.Equal(t, originalNode, partition.OwnerNode, 
			"Release partition %s should stay with original node %s", partitionId, originalNode)
		assert.True(t, 
			partition.Status == managment.PartitionAllocationPendingRelease || 
			partition.Status == managment.PartitionAllocationRequestedRelease,
			"Release partition %s should maintain its release status", partitionId)
	}
	
	// Count only active partitions per node
	activeNodeCounts := make(map[string]int)
	for _, partition := range nodePartitionMappings {
		if partition.Status == managment.PartitionAllocationAssigned {
			activeNodeCounts[partition.OwnerNode]++
		}
	}
	
	// We have 10 active partitions total (5 existing assigned + 5 unassigned)
	// The algorithm preserves stickiness, so node1 keeps its 3, node2 keeps its 2
	// The 5 unassigned get distributed to fill gaps: nodes 3,4,5 get the remainder
	totalActivePartitions := 0
	for _, count := range activeNodeCounts {
		totalActivePartitions += count
	}
	assert.Equal(t, 10, totalActivePartitions, "Total active partitions should be 10")
	
	// node1 should keep its 3 assigned partitions (stickiness)
	// node2 should keep its 2 assigned partitions (stickiness) 
	// nodes 3,4,5 should get the 5 unassigned partitions distributed among them
	expectedCounts := map[string]int{
		"node1": 3, // keeps existing
		"node2": 2, // keeps existing  
		// nodes 3,4,5 get remainder distributed (5 partitions among 3 nodes = 1-2 each)
	}
	
	for nodeId, expectedCount := range expectedCounts {
		if count, exists := activeNodeCounts[nodeId]; exists {
			assert.Equal(t, expectedCount, count, "Node %s should have %d active partitions", nodeId, expectedCount)
		}
	}
	
	// Verify nodes 3,4,5 together have 5 partitions
	remainingNodes := activeNodeCounts["node3"] + activeNodeCounts["node4"] + activeNodeCounts["node5"]
	assert.Equal(t, 5, remainingNodes, "Nodes 3,4,5 should together have 5 partitions")
}

// Test 5: Inactive node partitions get reassigned
func TestDistributeWork_InactiveNodePartitions(t *testing.T) {
	helper := NewTestHelper()
	
	taskListInfo := managment.TaskListInfo{
		PartitionCount: 12,
	}
	
	// Only 3 active nodes (node4 and node5 are inactive)
	nodes := helper.createNodes([]string{"node1", "node2", "node3"})
	
	nodePartitionMappings := make(map[string]*nodePartitionMapping)
	
	// Distribute partitions including some to inactive nodes
	nodeAssignments := []string{"node1", "node2", "node3", "node4", "node5"}
	for i := 0; i < 12; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodeId := nodeAssignments[i%5]
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, nodeId, managment.PartitionAllocationAssigned)
	}
	
	helper.runDistributeWorkTest(t, "Inactive Node Partitions", taskListInfo, nodePartitionMappings, nodes)
	
	// Verify no partitions are assigned to inactive nodes
	for _, partition := range nodePartitionMappings {
		if partition.Status == managment.PartitionAllocationAssigned {
			assert.Contains(t, []string{"node1", "node2", "node3"}, partition.OwnerNode,
				"Partition %s should not be assigned to inactive node", partition.Partition)
		}
	}
	
	// Verify even distribution among active nodes (4 partitions each)
	nodeCounts := make(map[string]int)
	for _, partition := range nodePartitionMappings {
		if partition.Status == managment.PartitionAllocationAssigned {
			nodeCounts[partition.OwnerNode]++
		}
	}
	
	for nodeId, count := range nodeCounts {
		assert.Equal(t, 4, count, "Active node %s should have exactly 4 partitions", nodeId)
	}
}

// Test 6: No active nodes edge case
func TestDistributeWork_NoActiveNodes(t *testing.T) {
	helper := NewTestHelper()
	
	taskListInfo := managment.TaskListInfo{
		PartitionCount: 10,
	}
	
	// No active nodes
	nodes := []*helixClusterMysql.GetActiveNodesRow{}
	
	nodePartitionMappings := make(map[string]*nodePartitionMapping)
	for i := 0; i < 10; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "", managment.PartitionAllocationUnassigned)
	}
	
	helper.runDistributeWorkTest(t, "No Active Nodes", taskListInfo, nodePartitionMappings, nodes)
	
	// Verify all partitions remain unassigned
	for _, partition := range nodePartitionMappings {
		assert.Equal(t, managment.PartitionAllocationUnassigned, partition.Status,
			"Partition %s should remain unassigned when no active nodes", partition.Partition)
		assert.Equal(t, "", partition.OwnerNode,
			"Partition %s should have no owner when no active nodes", partition.Partition)
	}
}

// Test 7: Single node gets all partitions
func TestDistributeWork_SingleNode(t *testing.T) {
	helper := NewTestHelper()
	
	taskListInfo := managment.TaskListInfo{
		PartitionCount: 10,
	}
	
	nodes := helper.createNodes([]string{"node1"})
	
	nodePartitionMappings := make(map[string]*nodePartitionMapping)
	for i := 0; i < 10; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "", managment.PartitionAllocationUnassigned)
	}
	
	helper.runDistributeWorkTest(t, "Single Node", taskListInfo, nodePartitionMappings, nodes)
	
	// Verify single node gets all partitions
	nodeCount := 0
	for _, partition := range nodePartitionMappings {
		if partition.Status == managment.PartitionAllocationAssigned {
			assert.Equal(t, "node1", partition.OwnerNode, "All partitions should be assigned to node1")
			nodeCount++
		}
	}
	
	assert.Equal(t, 10, nodeCount, "All 10 partitions should be assigned to the single node")
}

// Test 8: Complex scenario with mix of states and rebalancing
func TestDistributeWork_ComplexMixedScenario(t *testing.T) {
	helper := NewTestHelper()
	
	taskListInfo := managment.TaskListInfo{
		PartitionCount: 20,
	}
	
	nodes := helper.createNodes([]string{"node1", "node2", "node3", "node4"})
	
	nodePartitionMappings := make(map[string]*nodePartitionMapping)
	
	// Complex initial state:
	// node1: 8 partitions (2 assigned, 3 pending release, 3 requested release)
	// node2: 4 assigned partitions
	// node3: 2 assigned partitions  
	// node4: 0 partitions
	// 6 unassigned partitions
	
	partitionIdx := 0
	
	// node1: 2 assigned
	for i := 0; i < 2; i++ {
		partitionId := fmt.Sprintf("%d", partitionIdx)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "node1", managment.PartitionAllocationAssigned)
		partitionIdx++
	}
	
	// node1: 3 pending release
	for i := 0; i < 3; i++ {
		partitionId := fmt.Sprintf("%d", partitionIdx)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "node1", managment.PartitionAllocationPendingRelease)
		partitionIdx++
	}
	
	// node1: 3 requested release
	for i := 0; i < 3; i++ {
		partitionId := fmt.Sprintf("%d", partitionIdx)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "node1", managment.PartitionAllocationRequestedRelease)
		partitionIdx++
	}
	
	// node2: 4 assigned
	for i := 0; i < 4; i++ {
		partitionId := fmt.Sprintf("%d", partitionIdx)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "node2", managment.PartitionAllocationAssigned)
		partitionIdx++
	}
	
	// node3: 2 assigned
	for i := 0; i < 2; i++ {
		partitionId := fmt.Sprintf("%d", partitionIdx)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "node3", managment.PartitionAllocationAssigned)
		partitionIdx++
	}
	
	// 6 unassigned partitions
	for i := 0; i < 6; i++ {
		partitionId := fmt.Sprintf("%d", partitionIdx)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "", managment.PartitionAllocationUnassigned)
		partitionIdx++
	}
	
	helper.runDistributeWorkTest(t, "Complex Mixed Scenario", taskListInfo, nodePartitionMappings, nodes)
	
	// Verify balanced distribution - we have 14 active partitions among 4 nodes
	// That's 3-4 partitions per node (14/4 = 3.5, so 2 nodes get 4, 2 nodes get 3)
	activeNodeCounts := make(map[string]int)
	for _, partition := range nodePartitionMappings {
		if partition.Status == managment.PartitionAllocationAssigned {
			activeNodeCounts[partition.OwnerNode]++
		}
	}
	
	totalActive := 0
	nodes3 := 0
	nodes4 := 0
	for _, count := range activeNodeCounts {
		totalActive += count
		if count == 3 {
			nodes3++
		} else if count == 4 {
			nodes4++
		}
	}
	
	assert.Equal(t, 14, totalActive, "Total active partitions should be 14")
	assert.Equal(t, 2, nodes3, "Should have 2 nodes with 3 partitions")
	assert.Equal(t, 2, nodes4, "Should have 2 nodes with 4 partitions")
	
	// Verify release state partitions stayed with node1
	releasePartitions := []string{"2", "3", "4", "5", "6", "7"}
	for _, partitionId := range releasePartitions {
		partition := nodePartitionMappings[partitionId]
		assert.Equal(t, "node1", partition.OwnerNode,
			"Release partition %s should stay with node1", partitionId)
		assert.True(t,
			partition.Status == managment.PartitionAllocationPendingRelease ||
				partition.Status == managment.PartitionAllocationRequestedRelease,
			"Partition %s should maintain its release status", partitionId)
	}
}

// Test 9: Zero partitions edge case
func TestDistributeWork_ZeroPartitions(t *testing.T) {
	helper := NewTestHelper()
	
	taskListInfo := managment.TaskListInfo{
		PartitionCount: 0,
	}
	
	nodes := helper.createNodes([]string{"node1", "node2"})
	
	nodePartitionMappings := make(map[string]*nodePartitionMapping)
	
	helper.runDistributeWorkTest(t, "Zero Partitions", taskListInfo, nodePartitionMappings, nodes)
	
	// Should have no partitions
	assert.Equal(t, 0, len(nodePartitionMappings), "Should have no partitions")
}

// Test 10: More nodes than partitions
func TestDistributeWork_MoreNodesThanPartitions(t *testing.T) {
	helper := NewTestHelper()
	
	taskListInfo := managment.TaskListInfo{
		PartitionCount: 3,
	}
	
	nodes := helper.createNodes([]string{"node1", "node2", "node3", "node4", "node5"})
	
	nodePartitionMappings := make(map[string]*nodePartitionMapping)
	for i := 0; i < 3; i++ {
		partitionId := fmt.Sprintf("%d", i)
		nodePartitionMappings[partitionId] = helper.createPartition(partitionId, "", managment.PartitionAllocationUnassigned)
	}
	
	helper.runDistributeWorkTest(t, "More Nodes Than Partitions", taskListInfo, nodePartitionMappings, nodes)
	
	// Count how many nodes got partitions
	nodeCounts := make(map[string]int)
	for _, partition := range nodePartitionMappings {
		if partition.Status == managment.PartitionAllocationAssigned {
			nodeCounts[partition.OwnerNode]++
		}
	}
	
	// Should have exactly 3 nodes with 1 partition each
	assert.Equal(t, 3, len(nodeCounts), "Exactly 3 nodes should have partitions")
	for nodeId, count := range nodeCounts {
		assert.Equal(t, 1, count, "Node %s should have exactly 1 partition", nodeId)
	}
}

// Integration test runner
func TestDistributeWork_AllScenarios(t *testing.T) {
	fmt.Printf("\n" + strings.Repeat("=", 100))
	fmt.Printf("\nRUNNING ALL PARTITION DISTRIBUTION TESTS")
	fmt.Printf("\n" + strings.Repeat("=", 100))
	
	t.Run("BasicEvenDistribution", TestDistributeWork_BasicEvenDistribution)
	t.Run("UnevenDistributionWithRemainder", TestDistributeWork_UnevenDistributionWithRemainder)
	t.Run("RebalanceOverAllocatedNodes", TestDistributeWork_RebalanceOverAllocatedNodes)
	t.Run("ReleaseStatePartitions", TestDistributeWork_ReleaseStatePartitions)
	t.Run("InactiveNodePartitions", TestDistributeWork_InactiveNodePartitions)
	t.Run("NoActiveNodes", TestDistributeWork_NoActiveNodes)
	t.Run("SingleNode", TestDistributeWork_SingleNode)
	t.Run("ComplexMixedScenario", TestDistributeWork_ComplexMixedScenario)
	t.Run("ZeroPartitions", TestDistributeWork_ZeroPartitions)
	t.Run("MoreNodesThanPartitions", TestDistributeWork_MoreNodesThanPartitions)
	
	fmt.Printf("\n" + strings.Repeat("=", 100))
	fmt.Printf("\nALL TESTS COMPLETED")
	fmt.Printf("\n" + strings.Repeat("=", 100))
}