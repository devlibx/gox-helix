package executor

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/processor"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	"github.com/devlibx/gox-helix/pkg/common"
	"github.com/devlibx/gox-helix/pkg/common/config"
	"go.uber.org/fx"
	"log/slog"
)

// Service is the main executor service which is responsible for managing the lifecycle of the workers.
// It starts the workers, registers them and also de-registers them when the service is stopped.
type Service interface {
	GetWorkerId() string

	// Start begins the executor service. For each configured domain, it ensures all tasklists are registered
	// in the database and registers a unique worker instance to participate in the cluster for that domain.
	// This method is typically called once at application startup.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the executor service. It de-registers the worker from all domains it had
	// joined, signaling that this instance will no longer be processing tasks.
	// This method is typically called once during application shutdown.
	Stop(ctx context.Context) error
}

type serviceImpl struct {
	gox.CrossFunction
	logger *slog.Logger

	domainConfigs *config.Config
	workerId      string

	applicationSingleton *common.ApplicationSingleton

	workerDataLayer              *worker.DataLayer
	domainDataLayer              *domain.DataLayer
	partitionService             coordinator.PartitionService
	domainService                domain.Service
	PartitionDistributionService coordinator.PartitionDistributionService
	ProcessorFactory             processor.Factory

	ClientFunctionProcessWork coordinator.ClientFunctionProcessWork
}

func (s *serviceImpl) GetWorkerId() string {
	return s.applicationSingleton.GetWorkerId()
}

func NewExecutor(
	cf gox.CrossFunction,
	domainConfigs *config.Config,
	workerDataLayer *worker.DataLayer,
	domainDataLayer *domain.DataLayer,
	domainService domain.Service,
	partitionService coordinator.PartitionService,
	PartitionDistributionService coordinator.PartitionDistributionService,
	ProcessorFactory processor.Factory,
	ClientFunctionProcessWork coordinator.ClientFunctionProcessWork,
	applicationSingleton *common.ApplicationSingleton,
) (Service, error) {
	s := &serviceImpl{
		CrossFunction:                cf,
		domainConfigs:                domainConfigs,
		workerDataLayer:              workerDataLayer,
		domainDataLayer:              domainDataLayer,
		domainService:                domainService,
		partitionService:             partitionService,
		PartitionDistributionService: PartitionDistributionService,
		ProcessorFactory:             ProcessorFactory,
		ClientFunctionProcessWork:    ClientFunctionProcessWork,
		applicationSingleton:         applicationSingleton,
		workerId:                     applicationSingleton.GetWorkerId(),
		logger:                       applicationSingleton.GetModuleLogger("executor"),
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
