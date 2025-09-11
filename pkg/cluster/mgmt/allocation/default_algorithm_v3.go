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
	"github.com/devlibx/gox-helix/pkg/common/database"
	"log/slog"
	"sort"
)

type AlgorithmV1 struct {
	gox.CrossFunction
	dbInterface               helixClusterMysql.Querier
	dbInterfaceWithTxnSupport *helixClusterMysql.Queries
	algorithmConfig           *managment.AlgorithmConfig
	databaseConnectionHolder  database.ConnectionHolder
	clusterManager            managment.ClusterManager
}

// NewAllocationAlgorithmV1 creates a new simplified allocation algorithm
func NewAllocationAlgorithmV1(
	cf gox.CrossFunction,
	clusterDbInterface helixClusterMysql.Querier,
	clusterDbInterfaceWithTxnSupport *helixClusterMysql.Queries,
	algorithmConfig *managment.AlgorithmConfig,
	databaseConnectionHolder database.ConnectionHolder,
	clusterManager managment.ClusterManager,
) (managment.Algorithm, error) {
	return &AlgorithmV1{
		CrossFunction:             cf,
		dbInterface:               clusterDbInterface,
		dbInterfaceWithTxnSupport: clusterDbInterfaceWithTxnSupport,
		algorithmConfig:           algorithmConfig,
		databaseConnectionHolder:  databaseConnectionHolder,
		clusterManager:            clusterManager,
	}, nil
}

func (a *AlgorithmV1) CalculateAllocation(ctx context.Context, taskListInfo managment.TaskListInfo) (*managment.AllocationResponse, error) {

	// Step 1 - get the current state of the allocations
	partitionInfos, nodeStates, err := a.getCurrentState(ctx, taskListInfo)
	if err != nil {
		return nil, err
	}

	// Step 2 - get new distribution
	_, newAssignments := a.calculateTargetDistribution(taskListInfo, partitionInfos, nodeStates)

	// Step 3 - atomically update database with new assignments
	err = a.updateDatabase(ctx, taskListInfo, newAssignments)
	if err != nil {
		return nil, errors.Wrap(err, "failed to update database")
	}

	return &managment.AllocationResponse{}, nil
}

func (a *AlgorithmV1) getCurrentState(ctx context.Context, taskListInfo managment.TaskListInfo) (map[string]*PartitionState, map[string]*NodeState, error) {

	// Get current allocations
	// Note - we may get nodes which are not active i.e. may be not cleaned-up as of now
	allocations, err := a.dbInterface.GetAllocationsForTasklist(ctx, helixClusterMysql.GetAllocationsForTasklistParams{
		Cluster:  taskListInfo.Cluster,
		Domain:   taskListInfo.Domain,
		Tasklist: taskListInfo.TaskList,
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get allocations: cluster=%s domain=%s", taskListInfo.Cluster, taskListInfo.Domain)
	}

	// Get active nodes
	nodes, err := a.dbInterface.GetActiveNodes(ctx, taskListInfo.Cluster)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get active nodes: cluster=%s domain=%s", taskListInfo.Cluster, taskListInfo.Domain)
	}

	// Initialize partition states (all partitions start as unassigned)
	partitionStates := make(map[string]*PartitionState)
	for i := 0; i < taskListInfo.PartitionCount; i++ {
		partitionID := fmt.Sprintf("%d", i)
		partitionStates[partitionID] = &PartitionState{
			PartitionID: partitionID,
			NodeID:      "",
			Status:      managment.PartitionAllocationUnassigned,
			UpdatedTime: a.Now(),
		}
	}

	// Initialize node states
	nodeStates := make(map[string]*NodeState)
	for _, node := range nodes {
		nodeStates[node.NodeUuid] = &NodeState{
			NodeID:   node.NodeUuid,
			IsActive: node.Status == helixClusterMysql.NodeStatusActive,
		}
	}

	for _, allocation := range allocations {

		// Read partition info data from DB to get structured partition info
		allocInfo, err := goxJsonUtils.BytesToObject[dbPartitionAllocationInfos]([]byte(allocation.PartitionInfo))
		if err != nil {
			continue
		}

		for _, partitionInfo := range allocInfo.PartitionAllocationInfos {
			partitionID := partitionInfo.PartitionId
			nodeID := allocation.NodeID

			// Only process known partitions
			var partition *PartitionState
			var exists bool
			if partition, exists = partitionStates[partitionID]; !exists {
				continue // Skip invalid partitions ids
			}

			// Only consider assignments to active nodes (this should never happen)
			if nodeState, nodeExists := nodeStates[nodeID]; !nodeExists || !nodeState.IsActive {
				// Add this as an inactive node
				nodeStates[partition.NodeID] = &NodeState{
					NodeID:       partition.NodeID,
					IsActive:     false,
					PartitionIDs: []string{},
				}

				// Make sure these partitions are considered as unsigned
				partition.Status = managment.PartitionAllocationUnassigned
				partition.NodeID = ""
				continue // Skip inactive nodes (we don't care what partitions are with inactive nodes)
			}

			// Handle partitions stuck in release state (force reassign)
			// We just mark them unsigned if they are stuck for long time
			if partitionInfo.AllocationStatus == managment.PartitionAllocationRequestedRelease ||
				partitionInfo.AllocationStatus == managment.PartitionAllocationPendingRelease {
				// Use NormalizeDuration to handle time acceleration correctly
				// normalizedTimeout := a.NormalizeDuration(a.algorithmConfig.TimeToWaitForPartitionReleaseBeforeForceRelease)
				normalizedTimeout := a.algorithmConfig.TimeToWaitForPartitionReleaseBeforeForceRelease
				if partitionInfo.UpdatedTime.Add(normalizedTimeout).Before(a.Now()) {
					partition.Status = managment.PartitionAllocationUnassigned
					partition.NodeID = ""
				} else {
					partition.Status = partitionInfo.AllocationStatus
					partition.NodeID = nodeID
					partition.UpdatedTime = partitionInfo.UpdatedTime
				}
			} else if partitionInfo.AllocationStatus == managment.PartitionAllocationAssigned {
				partition.NodeID = nodeID
				partition.Status = managment.PartitionAllocationAssigned
				partition.UpdatedTime = partitionInfo.UpdatedTime
			} else if partitionInfo.AllocationStatus == managment.PartitionAllocationUnassigned {
				partition.NodeID = nodeID
				partition.Status = managment.PartitionAllocationUnassigned
				partition.UpdatedTime = partitionInfo.UpdatedTime
			}
		}
	}

	return partitionStates, nodeStates, nil
}

func (a *AlgorithmV1) calculateTargetDistribution(
	taskListInfo managment.TaskListInfo,
	partitionInfos map[string]*PartitionState,
	nodeStates map[string]*NodeState,
) (map[string]*nodeDistribution, map[string][]*PartitionState) {

	// Count only active nodes for capacity calculation
	activeNodeCount := 0
	for _, nodeState := range nodeStates {
		if nodeState.IsActive {
			activeNodeCount++
		}
	}

	// Handle edge case - Return empty distributions - this will result in no allocations
	if activeNodeCount == 0 {
		return map[string]*nodeDistribution{}, map[string][]*PartitionState{}
	}

	// Calculate exact partition distribution via round-robin
	activeNodes := make([]string, 0, activeNodeCount)
	for _, nodeState := range nodeStates {
		if nodeState.IsActive {
			activeNodes = append(activeNodes, nodeState.NodeID)
		}
	}
	sort.Strings(activeNodes) // Ensure consistent ordering

	// Use round-robin to determine exact partition distribution
	nodePartitionCounts := make(map[string]int)
	for i := 0; i < taskListInfo.PartitionCount; i++ {
		nodeIndex := i % len(activeNodes)
		nodeId := activeNodes[nodeIndex]
		nodePartitionCounts[nodeId]++
	}

	// Create node distributions with EXACT capacity (no buffer, no approximation)
	nodeDistributions := make(map[string]*nodeDistribution)
	totalCapacity := 0
	for _, nodeId := range activeNodes {
		exactCapacity := nodePartitionCounts[nodeId] // Exact capacity needed
		n := newNodeDistribution(a.CrossFunction, exactCapacity, nodeId)
		nodeDistributions[nodeId] = n
		totalCapacity += exactCapacity
	}

	// Phase 1: Sticky allocation - keep existing partitions on their current nodes if capacity allows
	// We loop through sorted partitions Ids as map may give different order in each run
	partitionIds := make([]string, 0)
	for k, _ := range partitionInfos {
		partitionIds = append(partitionIds, k)
	}
	nodeIds := make([]string, 0)
	for _, nodeId := range nodeDistributions {
		nodeIds = append(nodeIds, nodeId.nodeId)
	}
	sort.Strings(partitionIds)
	sort.Strings(nodeIds)
	for _, partitionId := range partitionIds {
		partitionInfo := partitionInfos[partitionId]
		for _, nodeId := range nodeIds {
			if done := nodeDistributions[nodeId].tryToAllocatePhase1(partitionInfo); done {
				delete(partitionInfos, partitionId)
				break
			}
		}
	}

	// Phase 2: Assign remaining partitions and create placeholders for migrations
	// We loop through sorted partitions Ids as map may give different order in each run
	partitionIds = make([]string, 0)
	for k, _ := range partitionInfos {
		partitionIds = append(partitionIds, k)
	}
	nodeIds = make([]string, 0)
	for _, ns := range nodeStates {
		nodeIds = append(nodeIds, ns.NodeID)
	}
	sort.Strings(partitionIds)
	sort.Strings(nodeIds)
	for _, partitionId := range partitionIds {
		partitionInfo := partitionInfos[partitionId]
		for _, nid := range nodeIds {
			targetNodeId := nid
			if nd, exists := nodeDistributions[targetNodeId]; exists {
				if done := nd.tryToAllocatePhase2(partitionInfo, targetNodeId); done {
					delete(partitionInfos, partitionId)
					break
				}
			}
		}
	}

	// Build final result with placeholders to return
	result := make(map[string][]*PartitionState)
	for _, nd := range nodeDistributions {
		result[nd.nodeId] = nd.getFinalResult()
	}

	// Add blank partitions for inactive nodes if we found - so that we can clean up partitions
	// I don't think this is needed
	for _, nodeState := range nodeStates {
		if !nodeState.IsActive {
			result[nodeState.NodeID] = make([]*PartitionState, 0)
		}
	}

	return nodeDistributions, result
}

type nodeDistribution struct {
	gox.CrossFunction
	maxAllowed                    int
	nodeId                        string
	partitionWithAssignedState    map[string]*PartitionState
	partitionWithReleaseState     map[string]*PartitionState
	partitionWithPlaceholderState map[string]*PartitionState
}

func newNodeDistribution(cf gox.CrossFunction, maxAllowed int, nodeId string) *nodeDistribution {
	return &nodeDistribution{
		CrossFunction:                 cf,
		maxAllowed:                    maxAllowed,
		nodeId:                        nodeId,
		partitionWithAssignedState:    make(map[string]*PartitionState),
		partitionWithReleaseState:     make(map[string]*PartitionState),
		partitionWithPlaceholderState: make(map[string]*PartitionState),
	}
}

func (d *nodeDistribution) getFinalResult() []*PartitionState {
	result := make([]*PartitionState, 0)
	for _, nd := range d.partitionWithAssignedState {
		result = append(result, nd)
	}
	for _, nd := range d.partitionWithReleaseState {
		result = append(result, nd)
	}

	// IMPORTANT: Do NOT include placeholders in final result
	// Placeholders are for internal capacity calculation only
	// They should never be persisted to database
	return result
}

func (d *nodeDistribution) getTotalCapacityUsed() int {
	return len(d.partitionWithAssignedState) + len(d.partitionWithPlaceholderState)
}

// Only allocate an existing assigned partitions to existing node (sticky allocation of partition to original node if possible)
//
// What to expect:
// At the end of this phase we will give partitions to exiting nodes (sticky allocation)
// Other important part is -> mark a partition request release if node is out of capacity
// Any existing request release will continue to be with original node
//
// NOTE - in this phase capacity used = partitions in assigned state (ignore release state partitions to calculation)
func (d *nodeDistribution) tryToAllocatePhase1(pi *PartitionState) bool {

	// Ignore if this partitions does not belong to this node
	if d.nodeId != pi.NodeID {
		return false
	}

	switch pi.Status {

	case managment.PartitionAllocationAssigned:
		// A partition in assigned state
		// Option 1 - if we have capacity in this node then keep it assigned
		// Option 2 - if not capacity then add an entry with request release
		//            We will give this to other node in placeholder state in later phase
		if d.getTotalCapacityUsed() < d.maxAllowed {
			// If we have space, and it is already assigned to this node then go ahead
			d.partitionWithAssignedState[pi.PartitionID] = pi
			return true
		} else {
			// If we do not have capacity left in this node and this partition was in assigned state in this node
			// We will add an entry with released state
			d.partitionWithReleaseState[pi.PartitionID] = &PartitionState{
				PartitionID: pi.PartitionID,
				NodeID:      d.nodeId,
				Status:      managment.PartitionAllocationRequestedRelease,
				UpdatedTime: d.Now(),
			}
			return false // this is still not consumed -> it has to be assigned to other node (in placeholder state)
		}

	case managment.PartitionAllocationRequestedRelease, managment.PartitionAllocationPendingRelease:
		// We allow to add beyond max allowed for release (requested or pending)
		// Track on original node but still needs placeholder elsewhere
		d.partitionWithReleaseState[pi.PartitionID] = pi
		return false // Let Phase 2 create placeholders on other nodes
	}

	return false
}

// 1. Any unassigned partitions will be allocated to a node
// 2. Any partitions in assigned state which are still not assigned
//   - we will give to other node with placeholder status (for capacity reservation)
//
// 3. Any partition unassigned will be given to a node were we have capacity
// 4. Release requested or pending - we will allocate them to other node with placeholder state (for capacity reservation)
func (d *nodeDistribution) tryToAllocatePhase2(pi *PartitionState, nodeId string) bool {

	switch pi.Status {

	case managment.PartitionAllocationAssigned, managment.PartitionAllocationRequestedRelease, managment.PartitionAllocationPendingRelease:
		if d.getTotalCapacityUsed() < d.maxAllowed && nodeId != pi.NodeID {
			// If there is a partition which reached here means it does not have space in
			// its original node
			// Note - we would have added requested release in original node (as we want original node to release it in next run)
			//
			// We need to add an entry in this node with PartitionAllocationPlaceholder,
			d.partitionWithPlaceholderState[pi.PartitionID] = &PartitionState{
				PartitionID: pi.PartitionID,
				NodeID:      nodeId,
				Status:      managment.PartitionAllocationPlaceholder,
				UpdatedTime: d.Now(),
			}
			return true
		}
		return false

	case managment.PartitionAllocationUnassigned:
		if d.getTotalCapacityUsed() < d.maxAllowed {
			d.partitionWithAssignedState[pi.PartitionID] = pi
			pi.Status = managment.PartitionAllocationAssigned
			pi.UpdatedTime = d.Now()
			pi.NodeID = nodeId
			return true
		}
	}

	return false
}

// updateDatabase writes the final assignments to the database using atomic transactions
func (a *AlgorithmV1) updateDatabase(ctx context.Context, taskListInfo managment.TaskListInfo, newAssignments map[string][]*PartitionState) error {

	// Get database connection from holder and Begin transaction
	db := a.databaseConnectionHolder.GetHelixMasterDbConnection()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
		} else {
			_ = tx.Commit()
		}
	}()

	// Create transaction-aware queries using SQLC pattern
	qtx := a.dbInterfaceWithTxnSupport.WithTx(tx)

	// Update all nodes within transaction
	for nodeID, partitions := range newAssignments {
		err = a.updateNodeAllocation(ctx, qtx, taskListInfo, nodeID, partitions)
		if err != nil {
			return errors.Wrap(err, "failed to update allocation for node "+nodeID)
		}
	}

	// Also remove bad allocations i.e. allocation where nodes are inactive
	a.removeAllocationWithInactiveNode(ctx, qtx, taskListInfo)

	// Why we do this - just in case there are 2 controller then this will make it safe
	// Validate coordinator still holds the lock before committing transaction
	// This prevents stale coordinators from committing conflicting allocation updates
	if !a.coordinatorStillHasLock(ctx) {
		err = errors.New("coordinator lock lost during allocation update - rollback transaction")
		slog.Warn("coordinator lock lost during allocation update - rolling back transaction",
			slog.String("cluster", taskListInfo.Cluster),
			slog.String("domain", taskListInfo.Domain),
			slog.String("tasklist", taskListInfo.TaskList))
		return err
	}

	return nil
}

// updateNodeAllocation updates the allocation for a single node within a transaction
func (a *AlgorithmV1) updateNodeAllocation(ctx context.Context, qtx *helixClusterMysql.Queries, taskListInfo managment.TaskListInfo, nodeID string, partitions []*PartitionState) error {

	// Convert V3 PartitionState to database format
	allocation := &managment.Allocation{
		Cluster:                  taskListInfo.Cluster,
		Domain:                   taskListInfo.Domain,
		TaskList:                 taskListInfo.TaskList,
		NodeId:                   nodeID,
		PartitionAllocationInfos: make([]managment.PartitionAllocationInfo, 0, len(partitions)),
	}

	// Convert each PartitionState to PartitionAllocationInfo
	for _, partition := range partitions {
		allocation.PartitionAllocationInfos = append(allocation.PartitionAllocationInfos,
			managment.PartitionAllocationInfo{
				PartitionId:      partition.PartitionID,
				AllocationStatus: partition.Status, // Only assigned/request_release/pending_release (no placeholders reach here)
				UpdatedTime:      partition.UpdatedTime,
			})
	}

	// Serialize to JSON
	allocationJSON, err := goxJsonUtils.ObjectToString(allocation)
	if err != nil {
		return errors.Wrap(err, "failed to serialize allocation")
	}

	// Use transaction-aware queries for atomic update
	return qtx.UpsertAllocation(ctx, helixClusterMysql.UpsertAllocationParams{
		Cluster:       taskListInfo.Cluster,
		Domain:        taskListInfo.Domain,
		Tasklist:      taskListInfo.TaskList,
		NodeID:        nodeID,
		PartitionInfo: allocationJSON,
		Metadata:      sql.NullString{Valid: true, String: "{}"},
	})
}

func (a *AlgorithmV1) removeAllocationWithInactiveNode(ctx context.Context, qtx *helixClusterMysql.Queries, taskListInfo managment.TaskListInfo) {
	if err := qtx.MarkAllocationsInactiveForInactiveNodes(
		ctx,
		helixClusterMysql.MarkAllocationsInactiveForInactiveNodesParams{
			Cluster:  taskListInfo.Cluster,
			Domain:   taskListInfo.Domain,
			Tasklist: taskListInfo.TaskList,
		}); err != nil {
		slog.Warn("failed to remove allocation for inactive node", slog.String("cluster", taskListInfo.Cluster), slog.String("domain", taskListInfo.Domain), slog.String("tasklist", taskListInfo.TaskList), slog.String("error", err.Error()))
	}
}

// coordinatorStillHasLock checks if the coordinator still owns the cluster coordination lock
func (a *AlgorithmV1) coordinatorStillHasLock(ctx context.Context) bool {
	if a.clusterManager == nil {
		return true // If no cluster manager provided, skip validation (for backward compatibility)
	} else {
		lockResult := a.clusterManager.BecomeClusterCoordinator(ctx)
		return lockResult.Acquired
	}
}
