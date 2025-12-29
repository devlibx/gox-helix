package worker

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	helixWorkerMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	"github.com/google/uuid"
	"log/slog"
	"time"
)

// mysqlWorker is the MySQL-based implementation of the worker.Worker interface.
type mysqlWorker struct {
	gox.CrossFunction
	id         string
	config     Config
	dataLayer  *DataLayer
	stopChan   chan struct{}
	cancelFunc context.CancelFunc
	isRunning  bool
}

// NewWorker is the constructor for the mysqlWorker.
func NewWorker(cf gox.CrossFunction, config Config, dataLayer *DataLayer) Worker {
	return &mysqlWorker{
		CrossFunction: cf,
		id:            uuid.NewString(),
		config:        config,
		dataLayer:     dataLayer,
		stopChan:      make(chan struct{}),
		isRunning:     false,
	}
}

func (m *mysqlWorker) Start(ctx context.Context) error {
	m.id = uuid.NewString()
	err := m.dataLayer.Querier.RegisterWorker(ctx, helixWorkerMysql.RegisterWorkerParams{
		WorkerID:        m.id,
		Domain:          m.config.Domain,
		CreatedAt:       m.Now(),
		LastHeartbeatAt: m.Now(),
	})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to register worker %s", m.id))
	}
	m.isRunning = true

	// Run heart beat
	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-ctx.Done():
				goto exit
			case <-ticker.C:
				result, err := m.dataLayer.Querier.SendHeartbeat(context.Background(), helixWorkerMysql.SendHeartbeatParams{
					LastHeartbeatAt: m.Now(),
					Domain:          m.config.Domain,
					WorkerID:        m.id,
				})
				if err == nil {
					if count, err := result.RowsAffected(); err == nil && count == 0 {
						if w, err := m.dataLayer.Querier.GetWorker(context.Background(), helixWorkerMysql.GetWorkerParams{
							Domain:   m.config.Domain,
							WorkerID: m.id,
						}); err == nil && w.Status != databaseCommon.WorkerStatusActive {
							slog.Warn("(worker is inactive) failed to send heartbeat", "domain", m.config.Domain, "worker_id", m.id)
							goto exit
						}
					}
				} else {
					slog.Warn("failed to send heartbeat", "domain", m.config.Domain, "worker_id", m.id, "error", err.Error())
				}
			}
		}

	exit:
		slog.Warn("worker heartbeat stopped", "domain", m.config.Domain, "worker_id", m.id)
		m.isRunning = false
	}()
	return nil
}

func (m *mysqlWorker) Stop() {
	_ = m.dataLayer.Querier.DeregisterWorker(context.Background(), helixWorkerMysql.DeregisterWorkerParams{
		Domain:   m.config.Domain,
		WorkerID: m.id,
	})
}

func (m *mysqlWorker) ID() string {
	return m.id
}

func (m *mysqlWorker) IsRunning() bool {
	return m.isRunning
}
