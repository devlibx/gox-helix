package worker

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	helixWorkerMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
	"github.com/google/uuid"
)

// mysqlWorker is the MySQL-based implementation of the worker.Worker interface.
type mysqlWorker struct {
	gox.CrossFunction
	id         string
	config     Config
	dataLayer  *DataLayer
	stopChan   chan struct{}
	cancelFunc context.CancelFunc
}

// NewWorker is the constructor for the mysqlWorker.
func NewWorker(cf gox.CrossFunction, config Config, dataLayer *DataLayer) Worker {
	return &mysqlWorker{
		CrossFunction: cf,
		id:            uuid.NewString(),
		config:        config,
		dataLayer:     dataLayer,
		stopChan:      make(chan struct{}),
	}
}

func (m *mysqlWorker) Start(ctx context.Context) error {
	m.id = uuid.NewString()
	err := m.dataLayer.RegisterWorker(ctx, helixWorkerMysql.RegisterWorkerParams{
		WorkerID:        m.id,
		Domain:          m.config.Domain,
		CreatedAt:       m.Now(),
		LastHeartbeatAt: m.Now(),
	})
	if err != nil {
		return errors.Wrap(err, fmt.Sprintf("failed to register worker %s", m.id))
	}
	return nil
}

func (m *mysqlWorker) Stop() {
	_ = m.dataLayer.DeregisterWorker(context.Background(), helixWorkerMysql.DeregisterWorkerParams{
		Domain:   m.config.Domain,
		WorkerID: m.id,
	})
}

func (m *mysqlWorker) ID() string {
	return m.id
}
