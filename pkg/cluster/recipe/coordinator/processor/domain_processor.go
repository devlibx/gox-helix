package processor

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"sync"
)

type domainTasklistProcessorImpl struct {
	gox.CrossFunction
	lockService           locker.Locker
	partitionService      coordinator.PartitionService
	config                *DomainTasklistProcessorCfg // Only the config, not the full dependencies
	activePartitions      []int
	activePartitionsMutex *sync.Mutex
	tasklistProcessor     map[int]TasklistProcessor
}

func (d *domainTasklistProcessorImpl) Process(ctx context.Context, request DomainTasklistProcessRequest) (*DomainTasklistProcessResponse, error) {
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
				_ = tp.Stop(context.Background())

				// Delete this task processor
				delete(d.tasklistProcessor, activePartition)
			}
		}
	}

	// Rebuild the new partitions
	d.activePartitions = make([]int, 0)
	for task, _ := range request.Partitions {
		d.activePartitions = append(d.activePartitions, task)
	}

	// Start all tasklist processors
	for _, task := range d.activePartitions {
		if _, ok := d.tasklistProcessor[task]; !ok {
			d.tasklistProcessor[task] = NewTasklistProcessor(
				d.CrossFunction,
				NewDefaultTasklistProcessorConfig(),
				d.lockService,
				d.partitionService,
				&ProcessTasklistRequest{
					Domain:    d.config.Domain,
					TaskList:  d.config.TaskList,
					Partition: task,
					WorkerId:  d.config.WorkerId,
				},
			)
		}
		_, _ = d.tasklistProcessor[task].Start(context.Background())
	}

	return &DomainTasklistProcessResponse{}, nil
}

func (d *domainTasklistProcessorImpl) Stop(ctx context.Context) error {
	d.activePartitionsMutex.Lock()
	defer d.activePartitionsMutex.Unlock()
	for _, activePartition := range d.activePartitions {
		_ = d.tasklistProcessor[activePartition].Stop(context.Background())
	}
	return nil
}

type DomainTasklistProcessorCfg struct {
	Domain   string
	TaskList string
	WorkerId string
}

func NewDomainTasklistProcessor(
	cf gox.CrossFunction,
	lockService locker.Locker,
	partitionService coordinator.PartitionService,
	cfg *DomainTasklistProcessorCfg,
) DomainTasklistProcessor {
	p := &domainTasklistProcessorImpl{
		CrossFunction:         cf,
		lockService:           lockService,
		partitionService:      partitionService,
		config:                cfg,
		activePartitions:      make([]int, 0),
		tasklistProcessor:     make(map[int]TasklistProcessor),
		activePartitionsMutex: &sync.Mutex{},
	}
	return p
}
