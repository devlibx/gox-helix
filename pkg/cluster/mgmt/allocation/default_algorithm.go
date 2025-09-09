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
	duplicateAssignments := make([]string, 0) // Track partitions with conflicts
	
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
				
				// Check for duplicate partition assignment
				if existing, exists := nodePartitionMappings[info.PartitionId]; exists {
					// Duplicate found! Choose winner based on deterministic rules
					var winner, loser *nodePartitionMapping
					
					// Rule 1: Prefer "assigned" status over others
					if existing.Status == managment.PartitionAllocationAssigned && m.Status != managment.PartitionAllocationAssigned {
						winner, loser = existing, m
					} else if m.Status == managment.PartitionAllocationAssigned && existing.Status != managment.PartitionAllocationAssigned {
						winner, loser = m, existing
					} else {
						// Rule 2: If same status, prefer more recent timestamp
						if m.UpdatedTime.After(existing.UpdatedTime) {
							winner, loser = m, existing
						} else if existing.UpdatedTime.After(m.UpdatedTime) {
							winner, loser = existing, m
						} else {
							// Rule 3: If same timestamp, prefer lexicographically smaller node ID for consistency
							if existing.OwnerNode < m.OwnerNode {
								winner, loser = existing, m
							} else {
								winner, loser = m, existing
							}
						}
					}
					
					// Keep winner, mark loser for cleanup (we can't directly modify DB here)
					nodePartitionMappings[info.PartitionId] = winner
					
					// Safe string truncation for logging
					winnerNodeShort := winner.OwnerNode
					if len(winnerNodeShort) > 8 {
						winnerNodeShort = winnerNodeShort[:8]
					}
					loserNodeShort := loser.OwnerNode
					if len(loserNodeShort) > 8 {
						loserNodeShort = loserNodeShort[:8]
					}
					
					duplicateAssignments = append(duplicateAssignments, 
						fmt.Sprintf("partition %s: kept node %s, conflicted with node %s", 
							info.PartitionId, winnerNodeShort, loserNodeShort))
				} else {
					nodePartitionMappings[info.PartitionId] = m
				}
			}
		}
	}
	
	// Log duplicate assignments for debugging
	if len(duplicateAssignments) > 0 {
		fmt.Printf("⚠️  Resolved %d duplicate partition assignments:\n", len(duplicateAssignments))
		for _, dup := range duplicateAssignments {
			fmt.Printf("   - %s\n", dup)
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
	// Use the new clean RebalanceAlgoV1 algorithm
	rebalancer := NewRebalanceAlgoV1(d.CrossFunction, d.AlgorithmConfig)
	return rebalancer.DistributeWork(taskListInfo, nodePartitionMappings, nodes)
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
