package processor

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/common"
	"log/slog"
	"sync"
)

type domainTasklistProcessorImpl struct {
	gox.CrossFunction
	lockService           locker.Locker
	partitionService      coordinator.PartitionService
	config                *DomainTasklistProcessorCfg // Only the config, not the full dependencies
	activePartitions      []int
	activePartitionsMutex *sync.Mutex
	tasklistProcessor     map[int]coordinator.TasklistProcessor
	applicationSingleton  *common.ApplicationSingleton
}

func (d *domainTasklistProcessorImpl) Process(ctx context.Context, request coordinator.DomainTasklistProcessRequest) (*coordinator.DomainTasklistProcessResponse, error) {
	d.activePartitionsMutex.Lock()
	defer d.activePartitionsMutex.Unlock()

	// Make sure we stopped any partitions which we no longer own first
	for _, activePartition := range d.activePartitions {

		partitionIsStillActiveAssignedToMe := false
		for _, newPartition := range request.Partitions {
			if newPartition == activePartition {
				partitionIsStillActiveAssignedToMe = true
			}
		}

		if !partitionIsStillActiveAssignedToMe {
			if tp, ok := d.tasklistProcessor[activePartition]; ok {

				// Stop this tasklist processor
				if err := tp.Stop(context.Background()); err != nil {
					slog.Warn("DomainTasklistProcessor failed to stop partition processor (when we get new assignment, then we need to stop unassigned partitions)",
						"domain", d.config.Domain,
						"tasklist", d.config.TaskList,
						"partition", activePartition,
						"err", err.Error(),
					)
				}

				// Delete this partition processor
				delete(d.tasklistProcessor, activePartition)
			}
		}
	}

	// Rebuild the new partitions
	d.activePartitions = make([]int, 0)
	for task, _ := range request.Partitions {
		d.activePartitions = append(d.activePartitions, task)
	}

	// Start all tasklist processors (for each active partitions)
	// Important - if a tasklist processor is already started, it is a no-op
	//    start and stop on tasklist processor is idempotent
	for _, partition := range d.activePartitions {
		if _, ok := d.tasklistProcessor[partition]; !ok {
			d.tasklistProcessor[partition] = NewTasklistProcessor(
				d.CrossFunction,
				coordinator.NewDefaultTasklistProcessorConfig(),
				d.lockService,
				d.partitionService,
				&ProcessTasklistRequest{
					Domain:                    d.config.Domain,
					TaskList:                  d.config.TaskList,
					Partition:                 partition,
					WorkerId:                  d.config.WorkerId,
					ClientFunctionProcessWork: d.config.ClientFunctionProcessWork,
				},
				d.applicationSingleton,
			)
		}
		if _, err := d.tasklistProcessor[partition].Start(context.Background()); err != nil {
			slog.Warn("DomainTasklistProcessor failed to start partition processor (when we get new assignment, then we need to start partition processing)",
				"domain", d.config.Domain,
				"tasklist", d.config.TaskList,
				"partition", partition,
				"err", err.Error(),
			)
		}
	}

	return &coordinator.DomainTasklistProcessResponse{}, nil
}

func (d *domainTasklistProcessorImpl) Stop(ctx context.Context) error {
	d.activePartitionsMutex.Lock()
	defer d.activePartitionsMutex.Unlock()
	for _, partition := range d.activePartitions {
		if err := d.tasklistProcessor[partition].Stop(context.Background()); err != nil {
			slog.Warn("DomainTasklistProcessor failed to stop partition processor (when we stop the domain task processor we need to stop it)",
				"domain", d.config.Domain,
				"tasklist", d.config.TaskList,
				"partition", partition,
				"err", err.Error(),
			)
		}
	}
	return nil
}

type DomainTasklistProcessorCfg struct {
	Domain                    string
	TaskList                  string
	WorkerId                  string
	ClientFunctionProcessWork coordinator.ClientFunctionProcessWork
}

func NewDomainTasklistProcessor(
	cf gox.CrossFunction,
	lockService locker.Locker,
	partitionService coordinator.PartitionService,
	cfg *DomainTasklistProcessorCfg,
	applicationSingleton *common.ApplicationSingleton,
) coordinator.DomainTasklistProcessor {
	p := &domainTasklistProcessorImpl{
		CrossFunction:         cf,
		lockService:           lockService,
		partitionService:      partitionService,
		config:                cfg,
		activePartitions:      make([]int, 0),
		tasklistProcessor:     make(map[int]coordinator.TasklistProcessor),
		activePartitionsMutex: &sync.Mutex{},
		applicationSingleton:  applicationSingleton,
	}
	return p
}
