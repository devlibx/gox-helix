package managment

import (
	"context"
	"time"
)

type TaskListInfo struct {
	Cluster        string `json:"cluster"`
	Domain         string `json:"domain"`
	TaskList       string `json:"task_list"`
	PartitionCount int    `json:"partition_count"`
}

type Allocation struct {
	Cluster                  string                    `json:"cluster"`
	Domain                   string                    `json:"domain"`
	TaskList                 string                    `json:"task_list"`
	NodeId                   string                    `json:"node_id"`
	PartitionAllocationInfos []PartitionAllocationInfo `json:"partition_allocation_infos"`
}

type PartitionAllocationStatus string

const (
	PartitionAllocationAssigned         PartitionAllocationStatus = "assigned"
	PartitionAllocationUnassigned       PartitionAllocationStatus = "unassigned"
	PartitionAllocationRequestedRelease PartitionAllocationStatus = "requested-release"
	PartitionAllocationPendingRelease   PartitionAllocationStatus = "pending-release"
	PartitionAllocationPlaceholder      PartitionAllocationStatus = "placeholder"
)

type PartitionAllocationInfo struct {
	PartitionId      string                    `json:"partition_id"`
	AllocationStatus PartitionAllocationStatus `json:"allocation_status"`
	UpdatedTime      time.Time                 `json:"updated_time"`
}

type AllocationResponse struct {
	Allocation *Allocation `json:"allocation"`
}

type AllocationManager interface {
	CalculateAllocation(ctx context.Context, taskListInfo TaskListInfo) (*AllocationResponse, error)
}

type AlgorithmConfig struct {
	TimeToWaitForPartitionReleaseBeforeForceRelease time.Duration
}

type Algorithm interface {
	CalculateAllocation(ctx context.Context, taskListInfo TaskListInfo) (*AllocationResponse, error)
}
