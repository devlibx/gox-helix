package helixMysql

// Lock status constants - using TINYINT values
const (
	LockStatusActive    int8 = 1 // active
	LockStatusInactive  int8 = 0 // inactive
	LockStatusDeletable int8 = 2 // deletable
)
