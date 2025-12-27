package helper

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	helixDomainMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/domain/database"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	helixWorkerMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
	"github.com/devlibx/gox-helix/pkg/common/config"
	"github.com/google/uuid"
	"log/slog"
	"sort"
	"time"
)

type WorkerHelper struct {
	gox.CrossFunction
	workerDataLayer  *worker.DataLayer
	domainDataLayer  *domain.DataLayer
	partitionService coordinator.PartitionService
	nodeId           string
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

	workerIds := make([]string, 0)
	for i := 0; i < domain.WorkerCountToProcessDomain; i++ {
		wid := fmt.Sprintf("worker-%d-%s", i, w.nodeId)
		workerIds = append(workerIds, wid)
		if err := w.workerDataLayer.RegisterWorker(ctx, helixWorkerMysql.RegisterWorkerParams{
			WorkerID:        wid,
			Domain:          domain.Name,
			CreatedAt:       w.Now(),
			LastHeartbeatAt: w.Now(),
		}); err != nil {
			return errors.Wrap(err, "failed to register worker: domain=%s, i=%d", domain.Name, i)
		}
	}

	go func() {
		for {
			for tasklistName, _ := range domain.TaskLists {
				for _, workerId := range workerIds {
					go func() {
						if result, err := w.partitionService.GetValidPartitionByOwnerId(ctx, domain.Name, tasklistName); err == nil {
							for _, r := range result {
								if r.OwnerID == workerId && tasklistName == "driver_pickup" {
									p := make([]int, 0)
									for k, _ := range r.Mapping {
										p = append(p, k)
									}
									sort.Ints(p)
									slog.Info("these are the worker", "domain", domain.Name, "tasklist", tasklistName, "workerId", workerId, "partitions", p)
								}
							}
						}
					}()
				}
			}

			time.Sleep(1 * time.Second)
		}
	}()

	return nil
}

func NewWorkerHelper(
	cf gox.CrossFunction,
	workerDataLayer *worker.DataLayer,
	domainDataLayer *domain.DataLayer,
	partitionService coordinator.PartitionService,
) *WorkerHelper {
	return &WorkerHelper{
		CrossFunction:    cf,
		workerDataLayer:  workerDataLayer,
		domainDataLayer:  domainDataLayer,
		partitionService: partitionService,
		nodeId:           uuid.New().String(),
	}
}
