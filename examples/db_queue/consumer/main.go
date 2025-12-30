package main

import (
	"context"
	"database/sql"
	_ "embed"
	"flag"
	"fmt"
	config2 "github.com/devlibx/gox-helix/pkg/common/config"
	"log"
	"log/slog"
	"sort"
	"sync"

	"time"

	"github.com/devlibx/gox-base/v2"
	helix "github.com/devlibx/gox-helix"
	"github.com/devlibx/gox-helix/examples/db_queue/database"
	goxHelixApi "github.com/devlibx/gox-helix/pkg/api"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/executor"
	pkgCommon "github.com/devlibx/gox-helix/pkg/common"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	"go.uber.org/fx"
	"gopkg.in/yaml.v3"
)

// AppConfig holds the application configuration, read from config.yaml.
type AppConfig struct {
	Config *config2.Config `yaml:"worker"`
}

// MetricsCollector collects and reports performance metrics for job processing.
type MetricsCollector struct {
	mu             sync.Mutex
	tasklistStats  map[string]*TasklistMetrics
	reportInterval time.Duration
	exitCh         chan struct{}
	wg             sync.WaitGroup
}

// TasklistMetrics holds statistics for a specific tasklist.
type TasklistMetrics struct {
	mu            sync.Mutex
	jobsProcessed int64
	latencies     []time.Duration
}

func NewMetricsCollector(reportInterval time.Duration) *MetricsCollector {
	return &MetricsCollector{
		tasklistStats:  make(map[string]*TasklistMetrics),
		reportInterval: reportInterval,
		exitCh:         make(chan struct{}),
	}
}

// Record adds a new latency measurement for a given tasklist.
func (mc *MetricsCollector) Record(tasklist string, latency time.Duration) {
	mc.mu.Lock()
	stats, ok := mc.tasklistStats[tasklist]
	if !ok {
		stats = &TasklistMetrics{}
		mc.tasklistStats[tasklist] = stats
	}
	mc.mu.Unlock()

	stats.mu.Lock()
	stats.jobsProcessed++
	stats.latencies = append(stats.latencies, latency)
	stats.mu.Unlock()
}

// StartReporter starts a goroutine that periodically reports the metrics.
func (mc *MetricsCollector) StartReporter(ctx context.Context) {
	mc.wg.Add(1)
	go func() {
		defer mc.wg.Done()
		ticker := time.NewTicker(mc.reportInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				mc.Report()
			case <-ctx.Done():
				slog.Info("Metrics reporter shutting down.")
				return
			case <-mc.exitCh:
				slog.Info("Metrics reporter received exit signal.")
				return
			}
		}
	}()
}

// Report calculates and prints the aggregated metrics for all tasklists.
func (mc *MetricsCollector) Report() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	slog.Info("--- Metrics Report ---")
	for tasklist, stats := range mc.tasklistStats {
		stats.mu.Lock()
		numJobs := stats.jobsProcessed
		latencies := make([]time.Duration, len(stats.latencies))
		copy(latencies, stats.latencies)
		stats.latencies = []time.Duration{} // Clear latencies after reporting
		stats.mu.Unlock()

		if numJobs == 0 {
			continue
		}

		sort.Slice(latencies, func(i, j int) bool {
			return latencies[i] < latencies[j]
		})

		p95 := latencies[int(float64(len(latencies))*0.95)]
		p99 := latencies[int(float64(len(latencies))*0.99)]
		p999 := latencies[int(float64(len(latencies))*0.999)]
		max := latencies[len(latencies)-1]

		slog.Info("Tasklist Metrics",
			"tasklist", tasklist,
			"jobs_processed", numJobs,
			"p95_latency", p95,
			"p99_latency", p99,
			"p999_latency", p999,
			"max_latency", max,
		)
	}
	slog.Info("----------------------")
}

// Shutdown signals the reporter to exit and waits for it to finish.
func (mc *MetricsCollector) Shutdown() {
	close(mc.exitCh)
	mc.wg.Wait()
}

// Define flags for algorithm selection
var (
	algo = flag.String("algo", "GetNextJob", "Job fetching algorithm: GetNextJob, GetNextJobForUpdate, GetNextJobMin, GetNextJobMinForUpdate")
)

func main() {
	flag.Parse() // Parse command-line flags

	helix.SetupTestEnv() // Ensure test environment is set up

	// Read application config
	appConfig := &AppConfig{}                     // Initialize as a pointer
	if err := readConfig(appConfig); err != nil { // Pass pointer directly
		log.Fatalf("failed to read config: %v", err)
	}
	// Default config values (e.g., app name)
	// appConfig.Domains() // This should now work as receiver is *AppConfig (which embeds *config.App)

	// Create a new application singleton. It manages the application's context and lifecycle.
	appSignal := pkgCommon.NewApplicationSingletonWithContext(context.Background())
	// Create cross function for gox-base utilities
	crossFunction := gox.NewCrossFunction()

	// Database connection setup
	db, queries, err := setupDatabase()
	if err != nil {
		log.Fatalf("failed to setup database: %v", err)
	}
	defer db.Close()

	// Initialize MetricsCollector
	metricsCollector := NewMetricsCollector(10 * time.Second) // Report every 10 seconds

	appConfig.Config.SetDefaults()

	// Build the go-fx application.
	app := fx.New(
		// Supply the application config and the domain definitions
		fx.Supply(&appConfig),
		fx.Supply(appConfig.Config),
		fx.Provide(gox.NewNoOpCrossFunction),

		// Supply singletons
		fx.Supply(appSignal),
		fx.Supply(crossFunction),
		fx.Supply(metricsCollector), // Supply the metrics collector

		// Database connection provider (used by gox-helix internal components)
		fx.Provide(func() (*sql.DB, error) {
			return sql.Open("mysql", helix.GetDefaultSqlUrl())
		}),
		fx.Provide(databaseCommon.NewConnectionHolder),

		// Provide the core gox-helix services
		goxHelixApi.Provider,

		// Provide our custom job processing function to the container.
		fx.Provide(func() coordinator.ClientFunctionProcessWork {
			return func(ctx context.Context, work coordinator.Work) {
				processWork(ctx, work, queries, metricsCollector, *algo) // Pass algo and collector
			}
		}),

		// Invoke the executor lifecycle to start the worker.
		fx.Invoke(executor.NewExecutorLifecycle),
		// Register metrics collector shutdown hook
		fx.Invoke(func(lifecycle fx.Lifecycle, mc *MetricsCollector) {
			lifecycle.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					mc.StartReporter(ctx)
					return nil
				},
				OnStop: func(ctx context.Context) error {
					mc.Shutdown()
					return nil
				},
			})
		}),
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
func processWork(ctx context.Context, work coordinator.Work, queries *database.Queries, metricsCollector *MetricsCollector, algo string) {
	var jobID string
	var err error
	var startTime time.Time

	for {
		select {
		case <-ctx.Done():
			slog.Info("Context cancelled, stopping work processing for partition", "work", work)
			work.CompletedChannel <- coordinator.WorkResponse{}
			close(work.CompletedChannel)
			return

		default:
			startTime = time.Now() // Start timing for latency measurement

			// Select job based on chosen algorithm
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
					// sqlc returns MIN(id) as an interface{}, so we need to type assert it to string
					jobID = string(id.([]uint8))
				}
				err = e
			case "GetNextJobMinForUpdate":
				id, e := queries.GetNextJobMinForUpdate(ctx, database.GetNextJobMinForUpdateParams{
					Domain: work.Domain, Tasklist: work.Tasklist, PartitionID: uint32(work.Partition),
				})
				if e == nil {
					// sqlc returns MIN(id) as an interface{}, so we need to type assert it to string
					jobID = string(id.([]uint8))
				}
				err = e
			default:
				slog.Error("Unknown algorithm selected", "algo", algo)
				time.Sleep(1 * time.Second)
				continue
			}

			// If no job is found, sleep for a bit and continue polling.
			if err == sql.ErrNoRows {
				time.Sleep(100 * time.Millisecond)
				continue
			} else if err != nil {
				slog.Error("Failed to get next job", "work", work, "err", err)
				time.Sleep(500 * time.Millisecond) // Sleep longer on error
				continue
			}

			// We found a job, mark it as in_progress
			if err := queries.UpdateJobStatus(ctx, database.UpdateJobStatusParams{ID: jobID, Status: "in_progress"}); err != nil {
				slog.Error("Failed to update job status to in_progress", "job_id", jobID, "err", err)
				continue
			}

			// Process the job (simulate work)
			time.Sleep(5 * time.Millisecond) // Simulate processing time

			// Mark the job as completed
			if err := queries.UpdateJobStatus(ctx, database.UpdateJobStatusParams{ID: jobID, Status: "completed"}); err != nil {
				slog.Error("Failed to update job status to completed", "job_id", jobID, "err", err)
				continue
			}

			// Record processing latency
			latency := time.Since(startTime)
			metricsCollector.Record(work.Tasklist, latency)
		}
	}
}

// setupDatabase initializes the database connection and returns the db object and sqlc queries object.
func setupDatabase() (*sql.DB, *database.Queries, error) {
	dsn := "root:credroot@tcp(127.0.0.1:3306)/automation?parseTime=true"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to database: %w", err)
	}
	if err := db.Ping(); err != nil {
		return nil, nil, fmt.Errorf("failed to ping database: %w", err)
	}
	db.SetMaxOpenConns(10) // Limit connections for stability
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
