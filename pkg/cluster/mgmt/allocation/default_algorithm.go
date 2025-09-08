package allocation

import (
	"context"
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

	// Convert the balanced mappings back to the response format
	// TODO: Implement conversion from balancedMappings to AllocationResponse
	_, _ = allocations, balancedMappings
	return nil, nil
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
	for id, _ := range nodePartitionMappings {
		if _, ok := activeNodes[id]; !ok {
			delete(nodePartitionMappings, id)
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

	// Step 1: Assign unassigned partitions to nodes with lowest counts
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

	// Step 2: Rebalance over-allocated nodes (move excess active partitions)
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
