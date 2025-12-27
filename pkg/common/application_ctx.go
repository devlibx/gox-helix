package common

import (
	"context"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/helper"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
)

type ApplicationCtx struct {
	ConnectionHolder             databaseCommon.ConnectionHolder
	WorkerHelper                 *helper.WorkerHelper
	WorkerDataLayer              *worker.DataLayer
	DomainDataLayer              *domain.DataLayer
	PartitionDistributionService coordinator.PartitionDistributionService
}

type ApplicationStopSignal struct {
	Ctx context.Context
}
