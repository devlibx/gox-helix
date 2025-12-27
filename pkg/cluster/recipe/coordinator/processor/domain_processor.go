package processor

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/common"
	"sync"
)

type domainTasklistProcessorImpl struct {
	gox.CrossFunction
	stopSignal            *common.ApplicationStopSignal
	lockService           locker.Locker
	domain                string
	tasklist              string
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
				_ = tp.Stop(context.Background())
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
				d.stopSignal,
				d.domain,
				d.tasklist,
				task,
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

func NewDomainTasklistProcessor(
	cf gox.CrossFunction,
	stopSignal *common.ApplicationStopSignal,
	lockService locker.Locker,
	domain string,
	tasklist string,
) DomainTasklistProcessor {
	p := &domainTasklistProcessorImpl{
		CrossFunction:         cf,
		stopSignal:            stopSignal,
		lockService:           lockService,
		domain:                domain,
		tasklist:              tasklist,
		activePartitions:      make([]int, 0),
		tasklistProcessor:     make(map[int]TasklistProcessor),
		activePartitionsMutex: &sync.Mutex{},
	}
	return p
}
