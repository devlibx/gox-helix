package helixClusterMysql

// Node status constants - using TINYINT values
const (
	NodeStatusActive    int8 = 1 // active
	NodeStatusInactive  int8 = 0 // inactive
	NodeStatusDeletable int8 = 2 // deletable
)