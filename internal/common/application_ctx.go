package common

import (
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/processor"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/executor"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
)

type ApplicationCtx struct {
	ConnectionHolder             databaseCommon.ConnectionHolder
	WorkerDataLayer              *worker.DataLayer
	DomainDataLayer              *domain.DataLayer
	PartitionDistributionService coordinator.PartitionDistributionService
	ProcessorFactory             processor.Factory
	ExecutorService              executor.Service
}
