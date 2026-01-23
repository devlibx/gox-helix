package processor

import (
	"context"
	"log/slog"
	"sort"
	"sync"

	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/common"
)

// used to write test
type newTasklistProcessorBuilder func(
	cf gox.CrossFunction,
	config *coordinator.TasklistProcessorConfig,
	lockService locker.Locker,
	partitionService coordinator.PartitionService,
	request *ProcessTasklistRequest,
	applicationSingleton *common.ApplicationSingleton,
) coordinator.TasklistProcessor

type domainTasklistProcessorImpl struct {
	gox.CrossFunction
	lockService           locker.Locker
	partitionService      coordinator.PartitionService
	config                *DomainTasklistProcessorCfg // Only the config, not the full dependencies
	activePartitions      []int
	activePartitionsMutex *sync.Mutex
	tasklistProcessor     map[int]coordinator.TasklistProcessor
	applicationSingleton  *common.ApplicationSingleton

	// Used in testing - in prod code it is not set and not used
	newTasklistProcessorBuilder newTasklistProcessorBuilder
}

func (d *domainTasklistProcessorImpl) Process(ctx context.Context, request coordinator.DomainTasklistProcessRequest) (*coordinator.DomainTasklistProcessResponse, error) {
	d.activePartitionsMutex.Lock()
	defer d.activePartitionsMutex.Unlock()

	before := make([]int, 0)
	for _, activePartitionValue := range d.activePartitions {
		before = append(before, activePartitionValue)
	}

	// Make sure we stopped any partitions which we no longer own first
	for _, activePartitionValue := range d.activePartitions {

		partitionIsStillActiveAssignedToMe := false
		for _, newPartitionValue := range request.Partitions {
			if newPartitionValue == activePartitionValue {
				partitionIsStillActiveAssignedToMe = true
				break // Found it, no need to continue inner loop
			}
		}

		if !partitionIsStillActiveAssignedToMe {
			if tp, ok := d.tasklistProcessor[activePartitionValue]; ok {

				// Stop this tasklist processor
				if err := tp.Stop(context.Background()); err != nil {
					slog.Warn("DomainTasklistProcessor failed to stop partition processor (when we get new assignment, then we need to stop unassigned partitions)",
						"domain", d.config.Domain,
						"tasklist", d.config.TaskList,
						"partition", activePartitionValue,
						"err", err.Error(),
					)
				}

				// Delete this partition processor
				delete(d.tasklistProcessor, activePartitionValue)
			}
		}
	}

	// Rebuild the new partitions
	d.activePartitions = make([]int, 0)
	for _, task := range request.Partitions {
		d.activePartitions = append(d.activePartitions, task)
	}

	// Create sorted copies for logging
	activePartitionsSorted := make([]int, len(d.activePartitions))
	copy(activePartitionsSorted, d.activePartitions)
	sort.Ints(activePartitionsSorted)

	beforeSorted := make([]int, len(before))
	copy(beforeSorted, before)
	sort.Ints(beforeSorted)

	slog.Info("[HELIX_IMP] active partitions allocated to worker: ",
		"domain", d.config.Domain,
		"tasklist", d.config.TaskList,
		"len", len(d.activePartitions),
		"partitions", activePartitionsSorted,
		"before", beforeSorted,
	)

	// Start all tasklist processors (for each active partitions)
	// Important - if a tasklist processor is already started, it is a no-op
	//    start and stop on tasklist processor is idempotent
	for _, partition := range d.activePartitions {
		if _, ok := d.tasklistProcessor[partition]; !ok {

			// d.newTasklistProcessorBuilder is set only in tests
			// In prod code it will always be nil
			if d.newTasklistProcessorBuilder == nil {
				d.tasklistProcessor[partition] = NewTasklistProcessor(
					d.CrossFunction,
					coordinator.NewDefaultTasklistProcessorConfig(),
					d.lockService,
					d.partitionService,
					&ProcessTasklistRequest{
						Domain:                 d.config.Domain,
						TaskList:               d.config.TaskList,
						Partition:              partition,
						WorkerId:               d.config.WorkerId,
						ClientFunctionProvider: d.config.ClientFunctionProcessWork,
					},
					d.applicationSingleton,
				)
			} else {
				d.tasklistProcessor[partition] = d.newTasklistProcessorBuilder(
					d.CrossFunction,
					coordinator.NewDefaultTasklistProcessorConfig(),
					d.lockService,
					d.partitionService,
					&ProcessTasklistRequest{
						Domain:                 d.config.Domain,
						TaskList:               d.config.TaskList,
						Partition:              partition,
						WorkerId:               d.config.WorkerId,
						ClientFunctionProvider: d.config.ClientFunctionProcessWork,
					},
					d.applicationSingleton,
				)
			}
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

	ret := make([]int, 0)
	for _, task := range d.activePartitions {
		ret = append(ret, task)
	}
	return &coordinator.DomainTasklistProcessResponse{
		Partitions: ret,
	}, nil
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
	d.activePartitions = make([]int, 0)
	return nil
}

type DomainTasklistProcessorCfg struct {
	Domain                    string
	TaskList                  string
	WorkerId                  string
	ClientFunctionProcessWork coordinator.ClientFunctionProvider
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
