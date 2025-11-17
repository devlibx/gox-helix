package main

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"go.uber.org/fx"
	"log"
	"log/slog"
	"time"
)

func main() {
	// Load env from embedded env files
	helix.SetupTestEnv()

	id := uuid.NewString()
	domainName := "example-" + id

	var lockerService locker.Locker
	var workerDataLayer *worker.DataLayer
	var cf gox.CrossFunction

	app := fx.New(
		fx.Provide(gox.NewCrossFunction),
		fx.Provide(func() (*sql.DB, error) {
			return sql.Open("mysql", helix.GetDefaultSqlUrl())
		}),
		fx.Provide(databaseCommon.NewConnectionHolder),

		// Provide Service components
		fx.Provide(func() domain.Config {
			return domain.Config{
				Domain: domainName,
				Domains: []domain.TaskList{
					{Name: "simulation-task", PartitionCount: 10},
				},
			}
		}),

		fx.Provide(domain.NewDomainDataLayer),
		fx.Provide(domain.NewService),
		fx.Provide(worker.NewWorkerDataLayer),
		fx.Provide(locker.NewLockerDataLayer),
		fx.Provide(locker.NewLocker),

		fx.Invoke(func(lc fx.Lifecycle, ds domain.Service) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					return ds.Init(ctx)
				},
				OnStop: func(ctx context.Context) error {
					return nil
				},
			})
		}),

		fx.Populate(&lockerService, &workerDataLayer, &cf),
	)

	if err := app.Start(context.Background()); err != nil {
		log.Fatalf("Application failed to start: %v", err)
	}

	for i := 0; i < 5; i++ {
		w := worker.NewWorker(
			cf,
			worker.Config{
				Domain:            domainName,
				HeartbeatInterval: time.Second,
			},
			workerDataLayer,
		)
		if err := w.Start(context.Background()); err != nil {
			log.Fatalf("Worker failed to start: %v", err)
		} else {
			slog.Info("Worker started", slog.String("worker_id", w.ID()))
		}
	}

	fmt.Println("Application started")
	time.Sleep(2 * time.Second)
}
