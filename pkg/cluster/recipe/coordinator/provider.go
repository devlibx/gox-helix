package coordinator

import (
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	"go.uber.org/fx"
)

var Provider = fx.Options(

	fx.Provide(NewPartitionDistributionService),
	fx.Provide(NewCoordinatorDataLayer),

	fx.Provide(func(cf gox.CrossFunction, ws WorkerService, ps PartitionService, ds DomainService) (DistributorStrategy, error) {
		return NewDistributorStrategy(cf, ws, ps, ds)
	}),

	fx.Provide(func(dataLayer *DataLayer) PartitionService { return dataLayer }),
	fx.Provide(func(dataLayer *worker.DataLayer) WorkerService { return dataLayer }),
	fx.Provide(func(dataLayer *domain.DataLayer) DomainService { return dataLayer }),
)
