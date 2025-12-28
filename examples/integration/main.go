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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	appSignal := &common2.ApplicationStopSignal{Ctx: ctx, ContextCancel: cancel}

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

		processor.Provider,
		locker.Provider,
		coordinator.Provider,
		domain.Provider,
		worker.Provider,

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
	err = app.Start(ctx)
	if err != nil {
		panic(err)
	}

	for _, domainObj := range appConfig.Domains {
		for _, tl := range domainObj.TaskLists {
			_, _ = appCtx.ProcessorFactory.GetOrCreateDomainTasklistProcessor(
				ctx,
				processor.CreateDomainTasklistProcessorRequest{
					Domain:   domainObj.Name,
					TaskList: tl.Name,
					WorkerId: appCtx.ExecutorService.GetWorkerId(),
				},
			)
		}
	}

	time.Sleep(30 * time.Minute)
	appSignal.ContextCancel()
	time.Sleep(30 * time.Minute)
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
