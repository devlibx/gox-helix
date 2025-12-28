package executor

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	"github.com/devlibx/gox-helix/pkg/common/config"
	"github.com/google/uuid"
	"go.uber.org/fx"
)

type Service interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
}

type serviceImpl struct {
	gox.CrossFunction

	domainConfigs *config.Config
	workerId      string

	workerDataLayer  *worker.DataLayer
	domainDataLayer  *domain.DataLayer
	partitionService coordinator.PartitionService
}

func NewExecutor(
	cf gox.CrossFunction,
	domainConfigs *config.Config,
	workerDataLayer *worker.DataLayer,
	domainDataLayer *domain.DataLayer,
	partitionService coordinator.PartitionService,
) (Service, error) {
	s := &serviceImpl{
		CrossFunction:    cf,
		domainConfigs:    domainConfigs,
		workerDataLayer:  workerDataLayer,
		domainDataLayer:  domainDataLayer,
		partitionService: partitionService,
		workerId:         uuid.NewString(),
	}
	return s, nil
}

func NewExecutorLifecycle(lifecycle fx.Lifecycle, executorService Service) {
	var cancelFuncOnStop context.CancelFunc
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			ctx, cancelFuncOnStop = context.WithCancel(ctx)
			return executorService.Start(ctx)
		},
		OnStop: func(ctx context.Context) error {
			cancelFuncOnStop()
			return executorService.Stop(ctx)
		},
	})
}
