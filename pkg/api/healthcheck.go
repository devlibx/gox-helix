package goxHelixApi

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	helixWorkerMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
	"github.com/devlibx/gox-helix/pkg/common"
	"github.com/devlibx/gox-helix/pkg/common/config"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	"time"
)

type HealthCheck struct {
	gox.CrossFunction
	workerDataLayer      *worker.DataLayer
	applicationSingleton *common.ApplicationSingleton
	domainConfigs        *config.Config
}

func (h *HealthCheck) Check() error {
	for _, domain := range h.domainConfigs.Domains {
		if domain.Disabled {
			continue
		}
		hw, err := h.workerDataLayer.Querier.GetWorkerByWorkerIdAndDomain(context.Background(), helixWorkerMysql.GetWorkerByWorkerIdAndDomainParams{
			Domain:   domain.Name,
			WorkerID: h.applicationSingleton.GetWorkerId(),
		})
		if err != nil {
			return errors.Wrap(err, "failed to get worker: domain=%s, worker_id=%s", domain.Name, h.applicationSingleton.GetWorkerId())
		}

		if hw.Status != databaseCommon.WorkerStatusActive {
			return errors.Wrap(err, "worker is not active: domain=%s, worker_id=%s, status=%d", domain.Name, h.applicationSingleton.GetWorkerId(), hw.Status)
		}

		if hw.LastHeartbeatAt.Before(h.Now().Add(-1 * time.Minute)) {
			return errors.Wrap(err, "worker is heartbeat is old: domain=%s, worker_id=%s, status=%d, last_heartbeat_at=%v, now=%v", domain.Name, h.applicationSingleton.GetWorkerId(), hw.Status, hw.LastHeartbeatAt, h.Now())
		}
	}
	return nil
}

func NewHealthCheck(
	cf gox.CrossFunction,
	workerDataLayer *worker.DataLayer,
	applicationSingleton *common.ApplicationSingleton,
	domainConfigs *config.Config,
) *HealthCheck {
	h := &HealthCheck{
		CrossFunction:        cf,
		workerDataLayer:      workerDataLayer,
		applicationSingleton: applicationSingleton,
		domainConfigs:        domainConfigs,
	}
	return h
}
