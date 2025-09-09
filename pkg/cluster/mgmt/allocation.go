// TaskListInfo represents the information about a task list, including its cluster, domain, name, and total partition count.
type TaskListInfo struct {
	Cluster        string `json:"cluster"`
	Domain         string `json:"domain"`
	TaskList       string `json:"task_list"`
	PartitionCount int    `json:"partition_count"`
}

// Allocation represents the assignment of partitions to a specific node within a task list.
type Allocation struct {
	Cluster                  string                    `json:"cluster"`
	Domain                   string                    `json:"domain"`
	TaskList                 string                    `json:"task_list"`
	NodeId                   string                    `json:"node_id"`
	PartitionAllocationInfos []PartitionAllocationInfo `json:"partition_allocation_infos"`
}

// PartitionAllocationStatus defines the possible states of a partition's allocation.
type PartitionAllocationStatus string

const (
	// PartitionAllocationAssigned indicates the partition is actively assigned to a node.
	PartitionAllocationAssigned         PartitionAllocationStatus = "assigned"
	// PartitionAllocationUnassigned indicates the partition is not currently assigned to any node.
	PartitionAllocationUnassigned       PartitionAllocationStatus = "unassigned"
	// PartitionAllocationRequestedRelease indicates the partition is marked for release from its current node.
	PartitionAllocationRequestedRelease PartitionAllocationStatus = "requested-release"
	// PartitionAllocationPendingRelease indicates the partition is pending release from its current node.
	PartitionAllocationPendingRelease   PartitionAllocationStatus = "pending-release"
	// PartitionAllocationPlaceholder is a temporary state used during two-phase rebalancing,
	// indicating a partition is intended for a new node but not yet physically assigned.
	PartitionAllocationPlaceholder      PartitionAllocationStatus = "placeholder"
)

// PartitionAllocationInfo provides details about a single partition's allocation status and last update time.
type PartitionAllocationInfo struct {
	PartitionId      string                    `json:"partition_id"`
	AllocationStatus PartitionAllocationStatus `json:"allocation_status"`
	UpdatedTime      time.Time                 `json:"updated_time"`
}

// AllocationResponse encapsulates the result of an allocation calculation.
type AllocationResponse struct {
	Allocation *Allocation `json:"allocation"`
}

// AllocationManager defines the interface for managing partition allocations.
type AllocationManager interface {
	// CalculateAllocation determines the optimal partition assignments for a given task list.
	CalculateAllocation(ctx context.Context, taskListInfo TaskListInfo) (*AllocationResponse, error)
}

// AlgorithmConfig holds configuration parameters for the allocation algorithm.
type AlgorithmConfig struct {
	// TimeToWaitForPartitionReleaseBeforeForceRelease specifies the duration to wait before
	// forcefully reassigning a partition that is stuck in a release state.
	TimeToWaitForPartitionReleaseBeforeForceRelease time.Duration
}

// Algorithm defines the interface for a partition allocation algorithm.
type Algorithm interface {
	// CalculateAllocation calculates the partition assignments based on the provided task list information.
	CalculateAllocation(ctx context.Context, taskListInfo TaskListInfo) (*AllocationResponse, error)
}
