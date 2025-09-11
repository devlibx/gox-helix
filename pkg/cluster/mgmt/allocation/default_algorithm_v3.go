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
)

type AlgorithmV1 struct {
	gox.CrossFunction
	dbInterface               helixClusterMysql.Querier
	dbInterfaceWithTxnSupport *helixClusterMysql.Queries
	algorithmConfig           *managment.AlgorithmConfig
	databaseConnectionHolder  database.ConnectionHolder
}

// NewAllocationAlgorithmV1 creates a new simplified allocation algorithm
func NewAllocationAlgorithmV1(
	cf gox.CrossFunction,
	clusterDbInterface helixClusterMysql.Querier,
	clusterDbInterfaceWithTxnSupport *helixClusterMysql.Queries,
	algorithmConfig *managment.AlgorithmConfig,
	databaseConnectionHolder database.ConnectionHolder,
) (managment.Algorithm, error) {
	return &AlgorithmV1{
		CrossFunction:             cf,
		dbInterface:               clusterDbInterface,
		dbInterfaceWithTxnSupport: clusterDbInterfaceWithTxnSupport,
		algorithmConfig:           algorithmConfig,
		databaseConnectionHolder:  databaseConnectionHolder,
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

			// Only consider assignments to active nodes
			if nodeState, nodeExists := nodeStates[nodeID]; !nodeExists || !nodeState.IsActive {
				continue // Skip inactive nodes (we don't care what partitions are with inactive nodes)
			}

			// Only process known partitions
			var partition *PartitionState
			var exists bool
			if partition, exists = partitionStates[partitionID]; !exists {
				continue // Skip invalid partitions ids
			}

			// Handle partitions stuck in release state (force reassign)
			// We just mark them unsigned if they are stuck for long time
			if partitionInfo.AllocationStatus == managment.PartitionAllocationRequestedRelease ||
				partitionInfo.AllocationStatus == managment.PartitionAllocationPendingRelease {
				if partitionInfo.UpdatedTime.Add(a.algorithmConfig.TimeToWaitForPartitionReleaseBeforeForceRelease).Before(a.Now()) {
					partition.Status = managment.PartitionAllocationUnassigned
					partition.NodeID = ""
				} else {
					partition.Status = partitionInfo.AllocationStatus
					partition.NodeID = nodeID
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

	// Handle edge case
	if activeNodeCount == 0 {
		// Return empty distributions - this will result in no allocations
		return map[string]*nodeDistribution{}, map[string][]*PartitionState{}
	}

	// Calculate target distribution based on actual active nodes
	basePartitionsPerNode := taskListInfo.PartitionCount / activeNodeCount
	remainder := taskListInfo.PartitionCount % activeNodeCount

	// Create a holder of partition distribution per node
	nodeDistributions := make(map[string]*nodeDistribution)
	nodeIndex := 0
	for _, nodeState := range nodeStates {
		if _, exists := nodeDistributions[nodeState.NodeID]; !exists && nodeState.IsActive {
			maxAllowed := basePartitionsPerNode
			if nodeIndex < remainder {
				maxAllowed++ // Only first 'remainder' nodes get extra partition
			}
			n := newNodeDistribution(a.CrossFunction, maxAllowed, nodeState.NodeID)
			nodeDistributions[nodeState.NodeID] = n
			nodeIndex++
		}
	}

	// Phase 1
	// We have allocated partitions, which we want to give to existing nodes (sticky partitions to node allocation)
	// In this step it will ensure that we only allocate assigned partitions ot a node (As long as we have capacity)
	// It also ensure that we mark an assigned partitions to request released if node does not have capacity
	//    Why - because we want to ask node to release these nodes
	for partitionId, partitionInfo := range partitionInfos {
		for _, nd := range nodeDistributions {
			if done := nd.tryToAllocatePhase1(partitionInfo); done {
				delete(partitionInfos, partitionId)
				break
			}
		}
	}

	// Phase 2 - In this phase we will do the following
	// 1. Any unassigned partitions will be allocated to a node
	// 2. Any partitions in assigned state which are still not assigned
	//        - we will give to other node with placeholder status (for capacity reservation)
	// 3. Any partition unassigned will be given to a node were we have capacity
	// 4. Release requested or pending - we will allocate them to other node with placeholder state (for capacity reservation)
	for partitionId, partitionInfo := range partitionInfos {
		for targetNodeId, _ := range nodeStates {
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

	// Get database connection from holder
	db := a.databaseConnectionHolder.GetHelixMasterDbConnection()

	// Begin transaction
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction")
	}

	defer func() {
		if err != nil {
			tx.Rollback()
		} else {
			tx.Commit()
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
