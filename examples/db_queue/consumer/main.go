package main

import (
	"context"
	"database/sql"
	_ "embed"
	"flag"
	"fmt"
	config2 "github.com/devlibx/gox-helix/pkg/common/config"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	"github.com/rcrowley/go-metrics"
	"log"
	"log/slog"
	"sync"

	"time"

	"github.com/devlibx/gox-base/v2"
	helix "github.com/devlibx/gox-helix"
	"github.com/devlibx/gox-helix/examples/db_queue/database"
	goxHelixApi "github.com/devlibx/gox-helix/pkg/api"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/executor"
	pkgCommon "github.com/devlibx/gox-helix/pkg/common"
	"go.uber.org/fx"
	"gopkg.in/yaml.v3"
)

// Define flags for algorithm selection
var (
	algo = flag.String("algo", "GetNextJob", "Job fetching algorithm: GetNextJob, GetNextJobForUpdate, GetNextJobMin, GetNextJobMinForUpdate")
)

// AppConfig holds the application configuration, read from config.yaml.
type AppConfig struct {
	Config *config2.Config `yaml:"worker"`
}

type MetricsCollectorV1 struct {
	registry metrics.Registry
	timer    metrics.Timer
	m        *sync.RWMutex
	algo     string
}

func NewMetricsCollectorV1() *MetricsCollectorV1 {
	m := &MetricsCollectorV1{
		m:        &sync.RWMutex{},
		registry: metrics.NewRegistry(),
		timer:    metrics.NewTimer(),
		algo:     "",
	}
	_ = m.registry.Register("latency", m.timer)

	go func() {
		for {
			time.Sleep(10 * time.Second)
			m.FlushReport()
		}
	}()
	return m
}

func (mc *MetricsCollectorV1) FlushReport() {
	mc.m.Lock()
	defer mc.m.Unlock()

	slog.Info("Flushing metrics",
		"algo", mc.algo,
		"count", mc.timer.Count(),
		"rps", mc.timer.Count()/10,
		"99", time.Duration(mc.timer.Percentile(0.99)),
		"999", time.Duration(mc.timer.Percentile(0.999)),
	)

	mc.registry = metrics.NewRegistry()
	mc.timer = metrics.NewTimer()
	_ = mc.registry.Register("latency", mc.timer)
}

func main() {
	flag.Parse()
	helix.SetupTestEnv()

	appConfig := &AppConfig{}
	if err := readConfig(appConfig); err != nil {
		log.Fatalf("failed to read config: %v", err)
	}
	appConfig.Config.SetDefaults()

	appSignal := pkgCommon.NewApplicationSingletonWithContext(context.Background())

	// Database connection setup
	db, queries, err := setupDatabase()
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	}
	defer func() {
		_ = db.Close()
	}()

	// Build the go-fx application.
	app := fx.New(
		// Supply the application config and the domain definitions
		fx.Supply(&appConfig),
		fx.Supply(appConfig.Config),
		fx.Supply(appSignal),
		fx.Provide(gox.NewNoOpCrossFunction, NewMetricsCollectorV1, databaseCommon.NewConnectionHolder),

		fx.Provide(func() (*sql.DB, error) {
			return sql.Open("mysql", helix.GetDefaultSqlUrl())
		}),

		// Provide the core gox-helix services
		goxHelixApi.Provider,

		// Provide our custom job processing function to the container.
		fx.Provide(func(mc *MetricsCollectorV1) coordinator.ClientFunctionProcessWork {
			return func(ctx context.Context, work coordinator.Work) {
				mc.algo = *algo
				processWork(ctx, work, queries, mc, *algo)
			}
		}),

		// Invoke the executor lifecycle to start the worker.
		fx.Invoke(executor.NewExecutorLifecycle),
	)

	// Run the application.
	if err := app.Start(context.Background()); err != nil {
		log.Fatalf("app failed to start: %v", err)
	}
	<-app.Done()
	if err := app.Stop(context.Background()); err != nil {
		log.Fatalf("app failed to stop: %v", err)
	}
}

// processWork is the main function that consumes jobs from the database.
func processWork(ctx context.Context, work coordinator.Work, queries *database.Queries, metricsCollector *MetricsCollectorV1, algo string) {
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()

	defer func() {
		work.CompletedChannel <- coordinator.WorkResponse{}
		close(work.CompletedChannel)
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			return
		default:
			var err error
			metricsCollector.timer.Time(func() {
				err = realProcessWork(ctx, work, queries, algo)
			})
			if err != nil {
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
}

func realProcessWork(ctx context.Context, work coordinator.Work, queries *database.Queries, algo string) error {
	var err error
	var jobID string

	switch algo {
	case "GetNextJob":
		job, e := queries.GetNextJob(ctx, database.GetNextJobParams{
			Domain: work.Domain, Tasklist: work.Tasklist, PartitionID: uint32(work.Partition),
		})
		if e == nil {
			jobID = job
		}
		err = e
	case "GetNextJobForUpdate":
		job, e := queries.GetNextJobForUpdate(ctx, database.GetNextJobForUpdateParams{
			Domain: work.Domain, Tasklist: work.Tasklist, PartitionID: uint32(work.Partition),
		})
		if e == nil {
			jobID = job
		}
		err = e
	case "GetNextJobMin":
		id, e := queries.GetNextJobMin(ctx, database.GetNextJobMinParams{
			Domain: work.Domain, Tasklist: work.Tasklist, PartitionID: uint32(work.Partition),
		})
		if e == nil {
			switch i := id.(type) {
			case []uint8:
				jobID = string(i)
			}
		}
		err = e
	case "GetNextJobMinForUpdate":
		id, e := queries.GetNextJobMinForUpdate(ctx, database.GetNextJobMinForUpdateParams{
			Domain: work.Domain, Tasklist: work.Tasklist, PartitionID: uint32(work.Partition),
		})
		switch i := id.(type) {
		case []uint8:
			jobID = string(i)
		}
		err = e
	}

	if err == nil && jobID != "" {
		if e := queries.UpdateJobStatus(ctx, database.UpdateJobStatusParams{ID: jobID, Status: "in_progress"}); e != nil {
			slog.Error("Failed to update job status to in_progress", "job_id", jobID, "err", e)
		}
	}
	return err
}

// setupDatabase initializes the database connection and returns the db object and sqlc queries object.
func setupDatabase() (*sql.DB, *database.Queries, error) {
	helix.SetupTestEnv()
	db, err := sql.Open("mysql", helix.GetDefaultSqlUrl())
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, nil, fmt.Errorf("failed to ping database: %w", err)
	}
	db.SetMaxOpenConns(50) // Limit connections for stability
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	queries := database.New(db)
	return db, queries, nil
}

//go:embed config.yaml
var data []byte

// readConfig reads and unmarshals the YAML configuration file.
func readConfig(appConfig *AppConfig) error {

	if err := yaml.Unmarshal(data, appConfig); err != nil {
		return fmt.Errorf("failed to unmarshal config: %w", err)
	}

	return nil
}
