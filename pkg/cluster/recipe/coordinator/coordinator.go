package coordinator

import (
	"github.com/devlibx/gox-base/v2"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	"github.com/devlibx/gox-helix/pkg/common/lock"
)

type coordinatorImpl struct {
	gox.CrossFunction
	clusterManager     managment.ClusterManager
	allocationManager  managment.AllocationManager
	clusterDbInterface helixClusterMysql.Querier
	locker             lock.Locker
}

func NewCoordinator(
	cf gox.CrossFunction,
	clusterManager managment.ClusterManager,
	allocationManager managment.AllocationManager,
	clusterDbInterface helixClusterMysql.Querier,
	locker lock.Locker,
) (Coordinator, error) {
	c := coordinatorImpl{
		CrossFunction:      cf,
		clusterManager:     clusterManager,
		allocationManager:  allocationManager,
		clusterDbInterface: clusterDbInterface,
		locker:             locker,
	}
	return &c, nil
}
