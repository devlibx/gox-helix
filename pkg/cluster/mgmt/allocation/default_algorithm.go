package allocation

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	goxJsonUtils "github.com/devlibx/gox-base/v2/serialization/utils/json"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	"sort"
	"time"
)

type defaultAlgorithm struct {
	gox.CrossFunction
	dbInterface     helixClusterMysql.Querier
	AlgorithmConfig *managment.AlgorithmConfig
}

func NewDefaultAlgorithm(
	cf gox.CrossFunction,
	clusterDbInterface helixClusterMysql.Querier,
	algorithmConfig *managment.AlgorithmConfig,

) (managment.Algorithm, error) {

	da := &defaultAlgorithm{
		CrossFunction:   cf,
		dbInterface:     clusterDbInterface,
		AlgorithmConfig: algorithmConfig,
	}
	return da, nil
}

type nodePartitionMapping struct {
	OwnerNode   string
	Partition   string
	Status      managment.PartitionAllocationStatus
	UpdatedTime time.Time
}

type dbPartitionAllocationInfos struct {
	PartitionAllocationInfos []managment.PartitionAllocationInfo `json:"partition_allocation_infos"`
}

func (d *defaultAlgorithm) CalculateAllocation(ctx context.Context, taskListInfo managment.TaskListInfo) (*managment.AllocationResponse, error) {

	allocations, err := d.dbInterface.GetAllocationsForTasklist(ctx, helixClusterMysql.GetAllocationsForTasklistParams{
		Cluster:  taskListInfo.Cluster,
		Domain:   taskListInfo.Domain,
		Tasklist: taskListInfo.TaskList,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to get allocations for tasklist: cluster=%s, domain=%s, tasklist=%s", taskListInfo.Cluster, taskListInfo.Domain, taskListInfo.TaskList)
	}

	nodes, err := d.dbInterface.GetActiveNodes(ctx, taskListInfo.Cluster)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get active nodes for cluster: cluster=%s", taskListInfo.Cluster)
	}

	// Get all the current mapping first
	nodePartitionMappings := d.buildNodePartitionMappingFromDbAllocation(allocations)

	// If we already waited for long enough to release nodes then mark them released
	// We will re-assign them to new nodes (assuming the node is no longer alive and cannot free the partition)
	d.markPartitionsStuckInRelease(nodePartitionMappings)

	// If any allocation is given to an inactive node then remove that
	// Just in case some partitions are not allocated to any node (maybe we increase no of partitions)
	d.markPartitionsAllocatedToInactiveNodes(nodes, nodePartitionMappings)

	// Finally we will have a map where we have all partition mapping - all assigned, and un-assigned
	d.addMissingPartitionsAsUnsigned(taskListInfo, nodePartitionMappings)

	// Distribute the work to balance partitions across active nodes
	balancedMappings := d.distributeWork(taskListInfo, nodePartitionMappings, nodes)

	// Update assignment in DB
	err = d.updateAssignment(ctx, taskListInfo, balancedMappings)
	if err != nil {
		return nil, errors.Wrap(err, "failed to update assignment")
	}

	return &managment.AllocationResponse{}, nil
}

func (d *defaultAlgorithm) buildNodePartitionMappingFromDbAllocation(allocations []*helixClusterMysql.HelixAllocation) map[string]*nodePartitionMapping {
	nodePartitionMappings := map[string]*nodePartitionMapping{}
	for _, allocation := range allocations {
		infos, err := goxJsonUtils.BytesToObject[dbPartitionAllocationInfos]([]byte(allocation.PartitionInfo))
		if err == nil {
			for _, info := range infos.PartitionAllocationInfos {
				m := &nodePartitionMapping{
					OwnerNode:   allocation.NodeID,
					Partition:   info.PartitionId,
					Status:      info.AllocationStatus,
					UpdatedTime: info.UpdatedTime,
				}
				nodePartitionMappings[info.PartitionId] = m
			}
		}
	}
	return nodePartitionMappings
}

func (d *defaultAlgorithm) markPartitionsStuckInRelease(nodePartitionMappings map[string]*nodePartitionMapping) {
	// If we already waited for long enough to release nodes then mark them released
	for _, ai := range nodePartitionMappings {
		if ai.Status == managment.PartitionAllocationRequestedRelease || ai.Status == managment.PartitionAllocationPendingRelease {
			if ai.UpdatedTime.Add(d.AlgorithmConfig.TimeToWaitForPartitionReleaseBeforeForceRelease).Before(d.Now()) {
				ai.Status = managment.PartitionAllocationUnassigned
				ai.OwnerNode = ""
			}
		}
	}
}

func (d *defaultAlgorithm) markPartitionsAllocatedToInactiveNodes(nodes []*helixClusterMysql.GetActiveNodesRow, nodePartitionMappings map[string]*nodePartitionMapping) {

	// Get all active node ids
	activeNodes := map[string]string{}
	for _, node := range nodes {
		if node.Status == helixClusterMysql.NodeStatusActive {
			activeNodes[node.NodeUuid] = node.NodeUuid
		}
	}

	// Remove assignments given to inactive nodes
	for _, partition := range nodePartitionMappings {
		if partition.OwnerNode != "" {
			if _, ok := activeNodes[partition.OwnerNode]; !ok {
				// Mark partition as unassigned if its owner node is inactive
				partition.Status = managment.PartitionAllocationUnassigned
				partition.OwnerNode = ""
			}
		}
	}
}

func (d *defaultAlgorithm) addMissingPartitionsAsUnsigned(taskListInfo managment.TaskListInfo, nodePartitionMappings map[string]*nodePartitionMapping) {
	for i := 0; i < taskListInfo.PartitionCount; i++ {
		if _, ok := nodePartitionMappings[fmt.Sprintf("%d", i)]; !ok {
			nodePartitionMappings[fmt.Sprintf("%d", i)] = &nodePartitionMapping{
				OwnerNode:   "",
				Partition:   fmt.Sprintf("%d", i),
				Status:      managment.PartitionAllocationUnassigned,
				UpdatedTime: time.Now(),
			}
		}
	}
}

func (d *defaultAlgorithm) distributeWork(taskListInfo managment.TaskListInfo, nodePartitionMappings map[string]*nodePartitionMapping, nodes []*helixClusterMysql.GetActiveNodesRow) map[string]*nodePartitionMapping {

	// Get active node IDs
	activeNodeIds := make([]string, 0)
	for _, node := range nodes {
		if node.Status == helixClusterMysql.NodeStatusActive {
			activeNodeIds = append(activeNodeIds, node.NodeUuid)
		}
	}

	if len(activeNodeIds) == 0 {
		return nodePartitionMappings // No active nodes to distribute to, return unchanged
	}

	// Calculate target distribution
	targetPerNode := taskListInfo.PartitionCount / len(activeNodeIds)
	extraPartitions := taskListInfo.PartitionCount % len(activeNodeIds)

	// Build current allocation map by node (only count active partitions)
	nodeCurrentAllocations := make(map[string][]*nodePartitionMapping)
	nodeActiveCount := make(map[string]int)
	unassignedPartitions := make([]*nodePartitionMapping, 0)

	// Initialize maps for all active nodes
	for _, nodeId := range activeNodeIds {
		nodeCurrentAllocations[nodeId] = make([]*nodePartitionMapping, 0)
		nodeActiveCount[nodeId] = 0
	}

	// Categorize partitions
	for _, partition := range nodePartitionMappings {
		switch partition.Status {
		case managment.PartitionAllocationAssigned:
			// Only count if node is active
			if _, exists := nodeActiveCount[partition.OwnerNode]; exists {
				nodeCurrentAllocations[partition.OwnerNode] = append(nodeCurrentAllocations[partition.OwnerNode], partition)
				nodeActiveCount[partition.OwnerNode]++
			} else {
				// Node is inactive, treat partition as unassigned
				partition.Status = managment.PartitionAllocationUnassigned
				partition.OwnerNode = ""
				unassignedPartitions = append(unassignedPartitions, partition)
			}
		case managment.PartitionAllocationPlaceholder:
			// Placeholders count toward active load for rebalancing calculations
			// but represent future assignments that don't exist in DB yet
			if _, exists := nodeActiveCount[partition.OwnerNode]; exists {
				nodeCurrentAllocations[partition.OwnerNode] = append(nodeCurrentAllocations[partition.OwnerNode], partition)
				nodeActiveCount[partition.OwnerNode]++
			}
		case managment.PartitionAllocationRequestedRelease, managment.PartitionAllocationPendingRelease:
			// Keep with current owner but don't count as active - don't reassign these
			// These partitions are already correctly assigned and should not be moved
			continue
		case managment.PartitionAllocationUnassigned:
			unassignedPartitions = append(unassignedPartitions, partition)
		}
	}

	// Helper function to get target for a node (some get +1 for remainder)
	getTargetForNode := func(nodeIndex int) int {
		if nodeIndex < extraPartitions {
			return targetPerNode + 1
		}
		return targetPerNode
	}

	// Sort nodes by current allocation count for balanced assignment
	sort.Strings(activeNodeIds)

	// Step 1: Two-phase migration - create placeholders for graceful rebalancing FIRST
	// This handles scenarios like adding new nodes to an imbalanced cluster
	d.implementTwoPhaseRebalancing(nodePartitionMappings, nodeActiveCount, activeNodeIds, getTargetForNode)

	// Step 2: Assign unassigned partitions to nodes with lowest counts
	for _, partition := range unassignedPartitions {
		// Find node with lowest current count that hasn't reached its target
		var minNodeId string
		minCount := taskListInfo.PartitionCount + 1 // Start with impossible high value

		for i, nodeId := range activeNodeIds {
			target := getTargetForNode(i)
			if nodeActiveCount[nodeId] < target && nodeActiveCount[nodeId] < minCount {
				minCount = nodeActiveCount[nodeId]
				minNodeId = nodeId
			}
		}

		// If no node is under target, assign to node with lowest count overall
		if minNodeId == "" {
			minCount = nodeActiveCount[activeNodeIds[0]]
			minNodeId = activeNodeIds[0]

			for _, nodeId := range activeNodeIds {
				if nodeActiveCount[nodeId] < minCount {
					minCount = nodeActiveCount[nodeId]
					minNodeId = nodeId
				}
			}
		}

		// Assign partition to this node
		partition.OwnerNode = minNodeId
		partition.Status = managment.PartitionAllocationAssigned
		partition.UpdatedTime = d.Now()
		nodeCurrentAllocations[minNodeId] = append(nodeCurrentAllocations[minNodeId], partition)
		nodeActiveCount[minNodeId]++
	}

	// Step 3: Rebalance over-allocated nodes (move excess active partitions)
	// Only move partitions if there are under-allocated nodes that need them
	for i, nodeId := range activeNodeIds {
		target := getTargetForNode(i)
		currentCount := nodeActiveCount[nodeId]

		if currentCount > target {
			partitionsToMove := make([]*nodePartitionMapping, 0)

			// Select excess partitions to move (from the end of the list for simplicity)
			nodePartitions := nodeCurrentAllocations[nodeId]
			if len(nodePartitions) > target {
				partitionsToMove = nodePartitions[target:]
				nodeCurrentAllocations[nodeId] = nodePartitions[:target]
				nodeActiveCount[nodeId] = target
			}

			// Redistribute excess partitions to under-allocated nodes
			for _, partitionToMove := range partitionsToMove {
				// Find a node that needs more partitions
				var targetNodeId string
				for j, candidateNodeId := range activeNodeIds {
					targetForNode := getTargetForNode(j)
					if nodeActiveCount[candidateNodeId] < targetForNode {
						targetNodeId = candidateNodeId
						break
					}
				}

				if targetNodeId != "" {
					// Move partition to this node
					partitionToMove.OwnerNode = targetNodeId
					partitionToMove.UpdatedTime = d.Now()
					nodeCurrentAllocations[targetNodeId] = append(nodeCurrentAllocations[targetNodeId], partitionToMove)
					nodeActiveCount[targetNodeId]++
				} else {
					// No under-allocated nodes found, keep with current node
					nodeCurrentAllocations[nodeId] = append(nodeCurrentAllocations[nodeId], partitionToMove)
					nodeActiveCount[nodeId]++
				}
			}
		}
	}

	// Return the updated mapping with all partitions correctly assigned
	return nodePartitionMappings
}

// implementTwoPhaseRebalancing creates placeholders for graceful partition migration
// This enables adding new nodes to imbalanced clusters without immediate disruption
func (d *defaultAlgorithm) implementTwoPhaseRebalancing(
	nodePartitionMappings map[string]*nodePartitionMapping,
	nodeActiveCount map[string]int,
	activeNodeIds []string,
	getTargetForNode func(int) int) {

	// Identify significantly under-allocated nodes (likely new nodes)
	underAllocatedNodes := make([]string, 0)
	overAllocatedNodes := make([]string, 0)

	for i, nodeId := range activeNodeIds {
		target := getTargetForNode(i)
		currentCount := nodeActiveCount[nodeId]

		// Consider a node significantly under-allocated if it has 0 partitions
		// and target > 0 (indicating new node joining existing cluster)
		if currentCount == 0 && target > 0 {
			underAllocatedNodes = append(underAllocatedNodes, nodeId)
		}

		// Consider a node over-allocated if it exceeds target 
		// This triggers two-phase migration when adding new nodes to imbalanced clusters
		if currentCount > target {
			overAllocatedNodes = append(overAllocatedNodes, nodeId)
		}
	}

	// Only proceed if we have both under-allocated and over-allocated nodes
	// This indicates a scenario where graceful migration would be beneficial
	if len(underAllocatedNodes) == 0 || len(overAllocatedNodes) == 0 {
		return
	}

	// Create placeholders and mark partitions for release
	underAllocatedIndex := 0
	for _, overNodeId := range overAllocatedNodes {
		if underAllocatedIndex >= len(underAllocatedNodes) {
			break // No more under-allocated nodes to assign to
		}

		// Find target count for this over-allocated node
		overNodeTarget := 0
		for i, nodeId := range activeNodeIds {
			if nodeId == overNodeId {
				overNodeTarget = getTargetForNode(i)
				break
			}
		}

		currentCount := nodeActiveCount[overNodeId]
		excessCount := currentCount - overNodeTarget

		// Mark excess partitions as RequestedRelease and create placeholders
		partitionsToRelease := 0
		for partitionId, partition := range nodePartitionMappings {
			if partition.OwnerNode == overNodeId && 
			   partition.Status == managment.PartitionAllocationAssigned && 
			   partitionsToRelease < excessCount {

				// Mark partition for release
				partition.Status = managment.PartitionAllocationRequestedRelease
				partition.UpdatedTime = d.Now()

				// Create placeholder on under-allocated node
				underNodeId := underAllocatedNodes[underAllocatedIndex]
				placeholderKey := partitionId + "_placeholder_" + underNodeId
				nodePartitionMappings[placeholderKey] = &nodePartitionMapping{
					OwnerNode:   underNodeId,
					Partition:   partition.Partition,
					Status:      managment.PartitionAllocationPlaceholder,
					UpdatedTime: d.Now(),
				}

				// Update active count for load balancing
				nodeActiveCount[overNodeId]--        // Reduce over-allocated node count
				nodeActiveCount[underNodeId]++       // Increase under-allocated node count

				partitionsToRelease++

				// Move to next under-allocated node when current one is sufficiently filled
				underNodeTarget := 0
				for i, nodeId := range activeNodeIds {
					if nodeId == underNodeId {
						underNodeTarget = getTargetForNode(i)
						break
					}
				}
				if nodeActiveCount[underNodeId] >= underNodeTarget {
					underAllocatedIndex++
					if underAllocatedIndex >= len(underAllocatedNodes) {
						break
					}
				}
			}
		}
	}
}

func (d *defaultAlgorithm) updateAssignment(ctx context.Context, taskListInfo managment.TaskListInfo, nodePartitionMappings map[string]*nodePartitionMapping) error {

	allocations := map[string]*managment.Allocation{}
	for _, v := range nodePartitionMappings {
		// Skip unassigned partitions and placeholders - they don't get stored in DB
		// Placeholders are used for calculation only and filtered out before final result
		if v.OwnerNode == "" || v.Status == managment.PartitionAllocationUnassigned || v.Status == managment.PartitionAllocationPlaceholder {
			continue
		}
		
		if _, ok := allocations[v.OwnerNode]; !ok {
			allocations[v.OwnerNode] = &managment.Allocation{
				Cluster:                  taskListInfo.Cluster,
				Domain:                   taskListInfo.Domain,
				TaskList:                 taskListInfo.TaskList,
				NodeId:                   v.OwnerNode,
				PartitionAllocationInfos: make([]managment.PartitionAllocationInfo, 0),
			}
		}
		allocations[v.OwnerNode].PartitionAllocationInfos = append(allocations[v.OwnerNode].PartitionAllocationInfos, managment.PartitionAllocationInfo{
			PartitionId:      v.Partition,
			AllocationStatus: v.Status,
			UpdatedTime:      d.Now(),
		})
	}

	for nodeId, allocation := range allocations {

		// Generate the payload to store in partition info
		info, err := goxJsonUtils.ObjectToString(allocation)
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to marshal allocation info for node %s", nodeId))
		}

		// Upsert new allocations
		err = d.dbInterface.UpsertAllocation(ctx, helixClusterMysql.UpsertAllocationParams{
			Cluster:       taskListInfo.Cluster,
			Domain:        taskListInfo.Domain,
			Tasklist:      taskListInfo.TaskList,
			NodeID:        nodeId,
			PartitionInfo: info,
			Metadata:      sql.NullString{Valid: true, String: "{}"},
		})
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("failed to upsert allocation for node %s", nodeId))
		}
	}

	return nil
}
