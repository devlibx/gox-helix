package helper

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	helixDomainMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/domain/database"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	helixWorkerMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
	"github.com/devlibx/gox-helix/pkg/common/config"
	"github.com/google/uuid"
)

type WorkerHelper struct {
	gox.CrossFunction
	workerDataLayer *worker.DataLayer
	domainDataLayer *domain.DataLayer
	nodeId          string
}

func (w *WorkerHelper) Setup(ctx context.Context, domain *config.Domain) error {
	if domain.Disabled == true {
		return nil
	}

	for tasklistName, taskList := range domain.TaskLists {
		if taskList.Disabled {
			continue
		}
		if err := w.domainDataLayer.UpsertTasklist(ctx, helixDomainMysql.UpsertTasklistParams{
			Domain:         domain.Name,
			Tasklist:       tasklistName,
			Metadata:       sql.NullString{Valid: true, String: `{}`},
			PartitionCount: uint32(taskList.PartitionCount),
		}); err != nil {
			return errors.Wrap(err, "failed to upsert tasklist: domain=%s, tasklist=%s", domain.Name, tasklistName)
		}
	}

	for i := 0; i < domain.WorkerCountToProcessDomain; i++ {
		if err := w.workerDataLayer.RegisterWorker(ctx, helixWorkerMysql.RegisterWorkerParams{
			WorkerID:        fmt.Sprintf("worker-%d-%s", i, w.nodeId),
			Domain:          domain.Name,
			CreatedAt:       w.Now(),
			LastHeartbeatAt: w.Now(),
		}); err != nil {
			return errors.Wrap(err, "failed to register worker: domain=%s, i=%d", domain.Name, i)
		}
	}

	return nil
}

func NewWorkerHelper(
	cf gox.CrossFunction,
	workerDataLayer *worker.DataLayer,
	domainDataLayer *domain.DataLayer,
) *WorkerHelper {
	return &WorkerHelper{
		CrossFunction:   cf,
		workerDataLayer: workerDataLayer,
		domainDataLayer: domainDataLayer,
		nodeId:          uuid.New().String(),
	}
}
