package main

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/serialization"
	helix "github.com/devlibx/gox-helix"
	"github.com/devlibx/gox-helix/internal/common"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/processor"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/executor"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	common2 "github.com/devlibx/gox-helix/pkg/common"
	"github.com/devlibx/gox-helix/pkg/common/config"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/fx"
	"time"
)

//go:embed config.yaml
var configStr string

func main() {
	helix.SetupTestEnv()

	appConfig := config.Config{}
	err := serialization.ReadParameterizedYaml(configStr, &appConfig, "env")
	if err != nil {
		panic(err)
	}
	appConfig.SetDefaults()
	fmt.Printf("%+v\n", appConfig)

	appSignal := &common2.ApplicationStopSignal{Ctx: context.Background()}

	appCtx := &common.ApplicationCtx{}
	app := fx.New(

		fx.Supply(&appConfig),
		fx.Supply(&appConfig.Domains),
		fx.Supply(appSignal),

		fx.Provide(gox.NewCrossFunction),
		fx.Provide(func() (*sql.DB, error) {
			return sql.Open("mysql", helix.GetDefaultSqlUrl())
		}),
		fx.Provide(databaseCommon.NewConnectionHolder),

		// All setup for this framework
		fx.Provide(domain.NewService),
		fx.Provide(processor.NewProcessorFactory),

		fx.Provide(coordinator.NewPartitionDistributionService),
		fx.Provide(func(cf gox.CrossFunction, ws coordinator.WorkerService, ps coordinator.PartitionService, ds coordinator.DomainService) (coordinator.DistributorStrategy, error) {
			return coordinator.NewDistributorStrategy(cf, ws, ps, ds)
		}),
		fx.Provide(locker.NewLockerDataLayer),           // Locker data layer
		fx.Provide(locker.NewLocker),                    // Locker
		fx.Provide(coordinator.NewCoordinatorDataLayer), // Coordinator
		fx.Provide(worker.NewWorkerDataLayer),           // Worker
		fx.Provide(domain.NewDomainDataLayer),           // Domain
		fx.Provide(func(dataLayer *coordinator.DataLayer) coordinator.PartitionService { return dataLayer }),
		fx.Provide(func(dataLayer *worker.DataLayer) coordinator.WorkerService { return dataLayer }),
		fx.Provide(func(dataLayer *domain.DataLayer) coordinator.DomainService { return dataLayer }),

		fx.Provide(executor.NewExecutor),
		fx.Invoke(NewCleanupOnBootupProvider),
		fx.Invoke(executor.NewExecutorLifecycle),

		fx.Populate(
			&appCtx.DomainDataLayer,
			&appCtx.WorkerDataLayer,
			&appCtx.ConnectionHolder,
			&appCtx.PartitionDistributionService,
			&appCtx.ProcessorFactory,
			&appCtx.ExecutorService,
		),
	)
	err = app.Start(context.Background())
	if err != nil {
		panic(err)
	}

	for _, domainObj := range appConfig.Domains {
		for _, tl := range domainObj.TaskLists {
			_, _ = appCtx.ProcessorFactory.GetOrCreateDomainTasklistProcessor(
				context.Background(),
				processor.CreateDomainTasklistProcessorRequest{
					Domain:   domainObj.Name,
					TaskList: tl.Name,
					WorkerId: appCtx.ExecutorService.GetWorkerId(),
				},
			)
		}
	}
	/*for _, domainObj := range appConfig.Domains {
		for _, tl := range domainObj.TaskLists {
			go func(d *config.Domain, tl *config.TaskList) {
				err := appCtx.PartitionDistributionService.Process(context.Background(), coordinator.DistributionRequest{
					DomainName: domainObj.Name,
					TaskList:   tl.Name,
				})
				if err != nil {
					panic(err)
				}
			}(domainObj, tl)
		}
	}*/

	time.Sleep(11 * time.Minute)
}

func NewCleanupOnBootupProvider(lifecycle fx.Lifecycle, connectionHolder databaseCommon.ConnectionHolder) {
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			db := connectionHolder.GetHelixMasterDbConnection()
			_, _ = db.Exec("TRUNCATE TABLE helix_workers")
			_, _ = db.Exec("TRUNCATE TABLE helix_domain")
			_, _ = db.Exec("TRUNCATE TABLE helix_worker_partition_mapping")
			_, _ = db.Exec("TRUNCATE TABLE helix_locks")
			_, _ = db.Exec("DELETE FROM helix_workers WHERE domain='food'")
			_, _ = db.Exec("DELETE FROM helix_workers WHERE domain='mobility'")
			_, _ = db.Exec("DELETE FROM helix_domain WHERE domain='food'")
			_, _ = db.Exec("DELETE FROM helix_domain WHERE domain='mobility'")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			_ = connectionHolder.GetHelixMasterDbConnection().Close()
			return nil
		},
	})

}
