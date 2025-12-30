package processor

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/common"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"sync"
	"testing"
)

func TestDomainTasklistProcessor_Process_Lifecycle(t *testing.T) {

	t.Run("InitialAssignment", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockProcessors := make(map[int]*coordinator.MockTasklistProcessor)

		mockBuilder := func(
			cf gox.CrossFunction,
			config *coordinator.TasklistProcessorConfig,
			lockService locker.Locker,
			partitionService coordinator.PartitionService,
			request *ProcessTasklistRequest,
			applicationSingleton *common.ApplicationSingleton,
		) coordinator.TasklistProcessor {
			if _, ok := mockProcessors[request.Partition]; !ok {
				mockProcessors[request.Partition] = coordinator.NewMockTasklistProcessor(ctrl)
			}
			return mockProcessors[request.Partition]
		}

		processor := &domainTasklistProcessorImpl{
			CrossFunction:               gox.NewCrossFunction(),
			config:                      &DomainTasklistProcessorCfg{Domain: "d", TaskList: "tl"},
			activePartitions:            make([]int, 0),
			tasklistProcessor:           make(map[int]coordinator.TasklistProcessor),
			activePartitionsMutex:       &sync.Mutex{},
			newTasklistProcessorBuilder: mockBuilder,
		}

		// Expect Start to be called for 1, 2, 3 once.
		mockProcessors[1] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[2] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[3] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[1].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[2].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[3].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)

		expectedPartitions := []int{1, 2, 3}
		resp, err := processor.Process(context.Background(), coordinator.DomainTasklistProcessRequest{Partitions: expectedPartitions})
		assert.NoError(t, err)
		assert.Equal(t, expectedPartitions, resp.Partitions)
	})

	t.Run("AddPartition", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockProcessors := make(map[int]*coordinator.MockTasklistProcessor)

		mockBuilder := func(
			cf gox.CrossFunction,
			config *coordinator.TasklistProcessorConfig,
			lockService locker.Locker,
			partitionService coordinator.PartitionService,
			request *ProcessTasklistRequest,
			applicationSingleton *common.ApplicationSingleton,
		) coordinator.TasklistProcessor {
			if _, ok := mockProcessors[request.Partition]; !ok {
				mockProcessors[request.Partition] = coordinator.NewMockTasklistProcessor(ctrl)
			}
			return mockProcessors[request.Partition]
		}

		processor := &domainTasklistProcessorImpl{
			CrossFunction:               gox.NewCrossFunction(),
			config:                      &DomainTasklistProcessorCfg{Domain: "d", TaskList: "tl"},
			activePartitions:            make([]int, 0),
			tasklistProcessor:           make(map[int]coordinator.TasklistProcessor),
			activePartitionsMutex:       &sync.Mutex{},
			newTasklistProcessorBuilder: mockBuilder,
		}

		// First assign {1, 2, 3} and expect Start calls
		initialPartitions := []int{1, 2, 3}
		mockProcessors[1] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[2] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[3] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[1].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[2].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[3].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		processor.Process(context.Background(), coordinator.DomainTasklistProcessRequest{Partitions: initialPartitions})

		// Now add {4} - Expect Start to be called for 1, 2, 3, 4 once.
		expectedPartitions := []int{1, 2, 3, 4}
		mockProcessors[4] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[1].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[2].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[3].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[4].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)

		resp, err := processor.Process(context.Background(), coordinator.DomainTasklistProcessRequest{Partitions: expectedPartitions})
		assert.NoError(t, err)
		assert.Equal(t, expectedPartitions, resp.Partitions)
	})

	t.Run("RemovePartition", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockProcessors := make(map[int]*coordinator.MockTasklistProcessor)

		mockBuilder := func(
			cf gox.CrossFunction,
			config *coordinator.TasklistProcessorConfig,
			lockService locker.Locker,
			partitionService coordinator.PartitionService,
			request *ProcessTasklistRequest,
			applicationSingleton *common.ApplicationSingleton,
		) coordinator.TasklistProcessor {
			if _, ok := mockProcessors[request.Partition]; !ok {
				mockProcessors[request.Partition] = coordinator.NewMockTasklistProcessor(ctrl)
			}
			return mockProcessors[request.Partition]
		}

		processor := &domainTasklistProcessorImpl{
			CrossFunction:               gox.NewCrossFunction(),
			config:                      &DomainTasklistProcessorCfg{Domain: "d", TaskList: "tl"},
			activePartitions:            make([]int, 0),
			tasklistProcessor:           make(map[int]coordinator.TasklistProcessor),
			activePartitionsMutex:       &sync.Mutex{},
			newTasklistProcessorBuilder: mockBuilder,
		}

		// First assign {1, 2, 3, 4}
		initialPartitions := []int{1, 2, 3, 4}
		mockProcessors[1] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[2] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[3] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[4] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[1].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[2].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[3].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[4].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		processor.Process(context.Background(), coordinator.DomainTasklistProcessRequest{Partitions: initialPartitions})

		// Remove {2} - Expect Stop for 2. Expect Start for 1, 3, 4.
		expectedPartitions := []int{1, 3, 4}
		mockProcessors[2].EXPECT().Stop(gomock.Any()).Return(nil).Times(1)
		mockProcessors[1].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[3].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[4].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)

		resp, err := processor.Process(context.Background(), coordinator.DomainTasklistProcessRequest{Partitions: expectedPartitions})
		assert.NoError(t, err)
		assert.Equal(t, expectedPartitions, resp.Partitions)
	})

	t.Run("NoChange", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockProcessors := make(map[int]*coordinator.MockTasklistProcessor)

		mockBuilder := func(
			cf gox.CrossFunction,
			config *coordinator.TasklistProcessorConfig,
			lockService locker.Locker,
			partitionService coordinator.PartitionService,
			request *ProcessTasklistRequest,
			applicationSingleton *common.ApplicationSingleton,
		) coordinator.TasklistProcessor {
			if _, ok := mockProcessors[request.Partition]; !ok {
				mockProcessors[request.Partition] = coordinator.NewMockTasklistProcessor(ctrl)
			}
			return mockProcessors[request.Partition]
		}

		processor := &domainTasklistProcessorImpl{
			CrossFunction:               gox.NewCrossFunction(),
			config:                      &DomainTasklistProcessorCfg{Domain: "d", TaskList: "tl"},
			activePartitions:            make([]int, 0),
			tasklistProcessor:           make(map[int]coordinator.TasklistProcessor),
			activePartitionsMutex:       &sync.Mutex{},
			newTasklistProcessorBuilder: mockBuilder,
		}

		// First assign {1, 3, 4}
		initialPartitions := []int{1, 3, 4}
		mockProcessors[1] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[3] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[4] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[1].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[3].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[4].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		processor.Process(context.Background(), coordinator.DomainTasklistProcessRequest{Partitions: initialPartitions})

		// No change {1, 3, 4} - Expect Start for 1, 3, 4.
		expectedPartitions := []int{1, 3, 4}
		mockProcessors[1].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[3].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[4].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)

		resp, err := processor.Process(context.Background(), coordinator.DomainTasklistProcessRequest{Partitions: expectedPartitions})
		assert.NoError(t, err)
		assert.Equal(t, expectedPartitions, resp.Partitions)
	})

	t.Run("StopAll", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockProcessors := make(map[int]*coordinator.MockTasklistProcessor)

		mockBuilder := func(
			cf gox.CrossFunction,
			config *coordinator.TasklistProcessorConfig,
			lockService locker.Locker,
			partitionService coordinator.PartitionService,
			request *ProcessTasklistRequest,
			applicationSingleton *common.ApplicationSingleton,
		) coordinator.TasklistProcessor {
			if _, ok := mockProcessors[request.Partition]; !ok {
				mockProcessors[request.Partition] = coordinator.NewMockTasklistProcessor(ctrl)
			}
			return mockProcessors[request.Partition]
		}

		processor := &domainTasklistProcessorImpl{
			CrossFunction:               gox.NewCrossFunction(),
			config:                      &DomainTasklistProcessorCfg{Domain: "d", TaskList: "tl"},
			activePartitions:            make([]int, 0),
			tasklistProcessor:           make(map[int]coordinator.TasklistProcessor),
			activePartitionsMutex:       &sync.Mutex{},
			newTasklistProcessorBuilder: mockBuilder,
		}

		// First assign {1, 3, 4}
		initialPartitions := []int{1, 3, 4}
		mockProcessors[1] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[3] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[4] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[1].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[3].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[4].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		_, _ = processor.Process(context.Background(), coordinator.DomainTasklistProcessRequest{Partitions: initialPartitions})

		// Call Stop - Expect Stop for 1, 3, 4.
		mockProcessors[1].EXPECT().Stop(gomock.Any()).Return(nil).Times(1)
		mockProcessors[3].EXPECT().Stop(gomock.Any()).Return(nil).Times(1)
		mockProcessors[4].EXPECT().Stop(gomock.Any()).Return(nil).Times(1)

		err := processor.Stop(context.Background())
		assert.NoError(t, err)

	})

	t.Run("AssignEmptyPartitions", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		mockProcessors := make(map[int]*coordinator.MockTasklistProcessor)

		mockBuilder := func(
			cf gox.CrossFunction,
			config *coordinator.TasklistProcessorConfig,
			lockService locker.Locker,
			partitionService coordinator.PartitionService,
			request *ProcessTasklistRequest,
			applicationSingleton *common.ApplicationSingleton,
		) coordinator.TasklistProcessor {
			if _, ok := mockProcessors[request.Partition]; !ok {
				mockProcessors[request.Partition] = coordinator.NewMockTasklistProcessor(ctrl)
			}
			return mockProcessors[request.Partition]
		}

		processor := &domainTasklistProcessorImpl{
			CrossFunction:               gox.NewCrossFunction(),
			config:                      &DomainTasklistProcessorCfg{Domain: "d", TaskList: "tl"},
			activePartitions:            make([]int, 0),
			tasklistProcessor:           make(map[int]coordinator.TasklistProcessor),
			activePartitionsMutex:       &sync.Mutex{},
			newTasklistProcessorBuilder: mockBuilder,
		}

		// Initial assignment {1, 2, 7, 8}
		initialPartitions := []int{1, 2, 7, 8}
		mockProcessors[1] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[2] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[7] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[8] = coordinator.NewMockTasklistProcessor(ctrl)
		mockProcessors[1].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[2].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[7].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		mockProcessors[8].EXPECT().Start(gomock.Any()).Return(nil, nil).Times(1)
		_, _ = processor.Process(context.Background(), coordinator.DomainTasklistProcessRequest{Partitions: initialPartitions})

		// Assign empty array - Expect Stop for all initial partitions.
		expectedPartitions := []int{}
		mockProcessors[1].EXPECT().Stop(gomock.Any()).Return(nil).Times(1)
		mockProcessors[2].EXPECT().Stop(gomock.Any()).Return(nil).Times(1)
		mockProcessors[7].EXPECT().Stop(gomock.Any()).Return(nil).Times(1)
		mockProcessors[8].EXPECT().Stop(gomock.Any()).Return(nil).Times(1)

		resp, err := processor.Process(context.Background(), coordinator.DomainTasklistProcessRequest{Partitions: expectedPartitions})
		assert.NoError(t, err)
		assert.Equal(t, expectedPartitions, resp.Partitions)
	})
}
