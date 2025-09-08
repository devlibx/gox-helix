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

	_, _ = allocations, nodes
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

func (d *defaultAlgorithm) distributeWork(taskListInfo managment.TaskListInfo, nodePartitionMappings map[string]*nodePartitionMapping, nodes []*helixClusterMysql.GetActiveNodesRow) {

	newMapping := map[string][]*nodePartitionMapping{}

	mappings := map[string]*nodePartitionMapping{}
	for id, node := range nodePartitionMappings {
		mappings[id] = node
	}

	// First step - assign existing partitions to existing nodes
	for id, node := range mappings {
		if node.Status == managment.PartitionAllocationAssigned {
			if _, ok := nodePartitionMappings[id]; !ok {
				newMapping[id] = make([]*nodePartitionMapping, 0)
			}
			newMapping[id] = append(newMapping[id], node)
			delete(mappings, id)
		}
	}

	type t struct {
		key      string
		mappings []*nodePartitionMapping
	}

	// Now we will have unsigned partitions left in mappings
	for _, node := range mappings {
		keys := make([]t, 0)
		for k, v := range newMapping {
			keys = append(keys, t{
				key:      k,
				mappings: v,
			})
		}
		sort.Slice(keys, func(i, j int) bool {
			return len(keys[i].mappings) < len(keys[j].mappings)
		})

		topId := keys[0]
		if _, ok := newMapping[topId.key]; !ok {
			newMapping[topId.key] = make([]*nodePartitionMapping, 0)
		}
		newMapping[topId.key] = append(newMapping[topId.key], node)
	}
}
