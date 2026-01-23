package worker

import (
	"context"
	"database/sql"
	"github.com/devlibx/gox-base/v2"
	helixWorkerMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
	"log/slog"
	"time"
)

// Monitor is responsible for maintaining the health of the worker cluster by marking
// stale workers as inactive. It periodically runs a check to identify workers that
// haven't sent a heartbeat within the configured threshold.
type Monitor interface {
	// Start begins the monitoring process in a background goroutine.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the monitoring process.
	Stop()
}

// mysqlMonitor is the MySQL-backed implementation of the Monitor interface.
type mysqlMonitor struct {
	gox.CrossFunction
	config    MonitorConfig
	dataLayer *DataLayer
	stopChan  chan struct{}
}

// MonitorConfig holds the configuration for the worker monitor.
type MonitorConfig struct {
	// Interval is the frequency at which the monitor runs the stale worker check.
	Interval time.Duration

	// DeadWorkerThreshold is the duration after which a worker is considered dead
	// if no heartbeat has been received.
	DeadWorkerThreshold time.Duration
}

// NewMonitor creates a new instance of the worker monitor.
func NewMonitor(cf gox.CrossFunction, config MonitorConfig, dataLayer *DataLayer) Monitor {
	if config.Interval == 0 {
		config.Interval = 3 * time.Second
	}
	if config.DeadWorkerThreshold == 0 {
		config.DeadWorkerThreshold = 10 * time.Second
	}

	return &mysqlMonitor{
		CrossFunction: cf,
		config:        config,
		dataLayer:     dataLayer,
		stopChan:      make(chan struct{}),
	}
}

func (m *mysqlMonitor) Start(ctx context.Context) error {
	slog.Info("Starting worker monitor", "interval", m.config.Interval, "dead_threshold", m.config.DeadWorkerThreshold)
	go func() {
		ticker := time.NewTicker(m.config.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				slog.Info("Stopping worker monitor (context done)")
				return
			case <-m.stopChan:
				slog.Info("Stopping worker monitor (stop signal)")
				return
			case <-ticker.C:
				m.markInactiveWorkers(ctx)
			}
		}
	}()
	return nil
}

func (m *mysqlMonitor) Stop() {
	close(m.stopChan)
}

func (m *mysqlMonitor) markInactiveWorkers(ctx context.Context) {
	// Calculate the cutoff time. Any worker with a last_heartbeat_at before this time is considered dead.
	cutoffTime := m.Now().Add(-m.config.DeadWorkerThreshold)

	// Execute the query to mark workers as inactive
	err := m.dataLayer.Querier.MarkInactiveWorkers(ctx, helixWorkerMysql.MarkInactiveWorkersParams{
		InactiveReason:  sql.NullString{String: "marked inactive by monitor", Valid: true},
		LastHeartbeatAt: cutoffTime,
	})
	if err != nil {
		slog.Error("Failed to mark inactive workers", "error", err)
	} else {
		// Optionally, we could log how many rows were affected if the sqlc generated code supports it,
		// but MarkInactiveWorkers is currently an :exec query (no result returned).
		// We can change it to :execresult if we want that info.
		slog.Debug("Ran MarkInactiveWorkers check", "cutoff_time", cutoffTime)
	}
}
