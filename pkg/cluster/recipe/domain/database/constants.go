package helixDomainMysql

// Worker status constants
const (
	WorkerStatusActive    int8 = 1 // active
	WorkerStatusInactive  int8 = 0 // inactive
	WorkerStatusDeletable int8 = 2 // deletable
)
