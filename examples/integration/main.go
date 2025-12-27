package main

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/serialization"
	helix "github.com/devlibx/gox-helix"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/helper"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	"github.com/devlibx/gox-helix/pkg/common"
	"github.com/devlibx/gox-helix/pkg/common/config"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/fx"
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

	appCtx := &common.ApplicationCtx{}
	app := fx.New(
		fx.Provide(gox.NewCrossFunction),
		fx.Provide(func() (*sql.DB, error) {
			return sql.Open("mysql", helix.GetDefaultSqlUrl())
		}),
		fx.Provide(databaseCommon.NewConnectionHolder),
		fx.Provide(helper.NewWorkerHelper),

		// All setup for this framework
		fx.Provide(locker.NewLockerDataLayer),           // Locker data layer
		fx.Provide(locker.NewLocker),                    // Locker
		fx.Provide(coordinator.NewCoordinatorDataLayer), // Coordinator
		fx.Provide(worker.NewWorkerDataLayer),           // Worker
		fx.Provide(domain.NewDomainDataLayer),           // Domain
		fx.Provide(func(dataLayer *coordinator.DataLayer) coordinator.PartitionService { return dataLayer }),
		fx.Provide(func(dataLayer *worker.DataLayer) coordinator.WorkerService { return dataLayer }),
		fx.Provide(func(dataLayer *domain.DataLayer) coordinator.DomainService { return dataLayer }),

		fx.Populate(
			&appCtx.DomainDataLayer,
			&appCtx.WorkerDataLayer,
			&appCtx.WorkerHelper,
			&appCtx.ConnectionHolder,
		),
	)
	err = app.Start(context.Background())
	if err != nil {
		panic(err)
	}

	// Clean DB
	db := appCtx.ConnectionHolder.GetHelixMasterDbConnection()
	if db == nil {
		panic("failed to connect to database")
	}
	defer db.Close()
	_, _ = db.Exec("DELETE FROM helix_workers WHERE domain='food'")
	_, _ = db.Exec("DELETE FROM helix_workers WHERE domain='mobility'")
	_, _ = db.Exec("DELETE FROM helix_domain WHERE domain='food'")
	_, _ = db.Exec("DELETE FROM helix_domain WHERE domain='mobility'")
	for _, domainObj := range appConfig.Domains {
		if err = appCtx.WorkerHelper.Setup(context.Background(), domainObj); err != nil {
			panic(err)
		}
	}
}
