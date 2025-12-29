package databaseCommon

// Worker status constants
const (
	WorkerStatusInactive  int8 = 0 // inactive
	WorkerStatusActive    int8 = 1 // active
	WorkerStatusDeletable int8 = 2 // deletable
)

// PartitionStatus status
type PartitionStatus int8

const (
	PartitionStatusInactive PartitionStatus = 0 // inactive
	PartitionStatusActive   PartitionStatus = 1 // active
)

type PartitionAssignmentStatus int8

const (
	PartitionAssignmentStatusAssigned   PartitionAssignmentStatus = 1
	PartitionAssignmentStatusUnassigned PartitionAssignmentStatus = 0
)
