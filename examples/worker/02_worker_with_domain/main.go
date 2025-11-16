package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	_ "github.com/go-sql-driver/mysql"
	"go.uber.org/fx"
)

// RealTimeService provides the actual current time.
type RealTimeService struct{}

func (r *RealTimeService) Now() time.Time { return time.Now() }

func main() {
	// Load env from embedded env files
	helix.SetupTestEnv()

	app := fx.New(
		// Provide common dependencies
		fx.Provide(gox.NewCrossFunction),
		fx.Provide(func() (*sql.DB, error) {
			user := os.Getenv("DB_USER")
			password := os.Getenv("DB_PASSWORD")
			host := os.Getenv("DB_HOST")
			port := os.Getenv("DB_PORT")
			dbName := os.Getenv("DB_NAME")
			url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, password, host, port, dbName)
			return sql.Open("mysql", url)
		}),
		fx.Provide(databaseCommon.NewConnectionHolder),

		// Provide Domain components
		fx.Provide(func() domain.Config {
			return domain.Config{
				Domain: "example-domain",
				Domains: []domain.TaskList{
					{Name: "task1", PartitionCount: 10},
				},
			}
		}),
		fx.Provide(domain.NewDomainDataLayer),
		fx.Provide(domain.NewDomain),

		// Provide Worker components
		fx.Provide(worker.NewWorkerDataLayer),
		fx.Provide(worker.NewWorker),
		
		fx.Provide(func() worker.Config {
			return worker.Config{
				Domain:            "example-domain",
				HeartbeatInterval: 10 * time.Second,
			}
		}),

		// Run the application
		fx.Invoke(func(lc fx.Lifecycle, d domain.Domain, w worker.Worker) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					log.Println("### Starting Application ###")

					// Initialize the domain
					if err := d.Init(ctx); err != nil {
						log.Fatalf("failed to init domain: %v", err)
					}
					log.Println("Domain initialized successfully")

					// Start the worker
					go func() {
						log.Printf("Worker %s starting...", w.ID())
						if err := w.Start(ctx); err != nil {
							log.Printf("worker stopped with error: %v", err)
						}
						log.Printf("Worker %s has stopped.", w.ID())
					}()

					return nil
				},
				OnStop: func(ctx context.Context) error {
					log.Println("### Stopping Application ###")
					w.Stop()
					log.Println("Worker stop signal sent.")
					// Give worker a moment to shut down
					time.Sleep(1 * time.Second)
					return nil
				},
			})
		}),
	)

	// Run the application. It will block until a signal is received.
	// We'll simulate a shutdown signal after a delay.
	go func() {
		time.Sleep(30 * time.Second)
		log.Println("Simulating shutdown...")
		if err := app.Stop(context.Background()); err != nil {
			log.Fatalf("failed to stop application: %v", err)
		}
	}()

	if err := app.Start(context.Background()); err != nil {
		log.Fatalf("Application failed to start: %v", err)
	}
}
