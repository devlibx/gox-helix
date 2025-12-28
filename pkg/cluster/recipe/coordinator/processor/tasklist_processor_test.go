package processor

import (
	"context"
	"fmt"
	"go.uber.org/mock/gomock"
	"sync"
	"testing"
	"time"

	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/common"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type testProcessorDeps struct {
	cf                   gox.CrossFunction
	config               *TasklistProcessorConfig
	mockCtrl             *gomock.Controller
	mockLocker           *locker.MockLocker
	mockPartitionService *coordinator.MockPartitionService // Use PartitionService mock
	stopSignal           *common.ApplicationStopSignal
	processor            TasklistProcessor
}

func setupTest(t *testing.T) *testProcessorDeps {
	mockCtrl := gomock.NewController(t)
	mockLocker := locker.NewMockLocker(mockCtrl)
	mockPartitionService := coordinator.NewMockPartitionService(mockCtrl)
	stopCtx, cancel := context.WithCancel(context.Background())
	stopSignal := &common.ApplicationStopSignal{Ctx: stopCtx}
	config := NewDefaultTasklistProcessorConfig()

	p := NewTasklistProcessor(
		gox.NewCrossFunction(),
		config,
		mockLocker,
		mockPartitionService,
		&ProcessTasklistRequest{
			Domain:    "test-domain",
			TaskList:  "test-tasklist",
			Partition: 1,
			WorkerId:  "test-worker-id",
		},
	)

	// Default expectation for ownership checks to allow existing tests to pass
	mockPartitionService.EXPECT().IsPartitionOwnedByOwner(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

	t.Cleanup(func() {
		cancel()
		time.Sleep(10 * time.Millisecond)
	})

	return &testProcessorDeps{
		cf:                   gox.NewCrossFunction(),
		config:               config,
		mockCtrl:             mockCtrl,
		mockLocker:           mockLocker,
		mockPartitionService: mockPartitionService,
		stopSignal:           stopSignal,
		processor:            p,
	}
}

func TestStart_Idempotency(t *testing.T) {
	deps := setupTest(t)

	deps.mockLocker.EXPECT().AcquireLock(gomock.Any(), gomock.Any()).Return(&locker.AcquireLockResponse{}, nil).Times(1)
	deps.mockLocker.EXPECT().ReleaseLock(gomock.Any(), gomock.Any()).Return(&locker.ReleaseLockResponse{}, nil).Times(1)

	resp, err := deps.processor.Start(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "STARTED", resp.Status)

	resp, err = deps.processor.Start(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, "ALREADY_RUNNING", resp.Status)

	err = deps.processor.Stop(context.Background())
	assert.NoError(t, err)
}

func TestStop_GracefulShutdown(t *testing.T) {
	deps := setupTest(t)

	deps.mockLocker.EXPECT().AcquireLock(gomock.Any(), gomock.Any()).Return(&locker.AcquireLockResponse{}, nil).AnyTimes()
	deps.mockLocker.EXPECT().ReleaseLock(gomock.Any(), gomock.Any()).Return(&locker.ReleaseLockResponse{}, nil).Times(1)

	_, err := deps.processor.Start(context.Background())
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	err = deps.processor.Stop(context.Background())
	assert.NoError(t, err)

	p := deps.processor.(*tasklistProcessorImpl)
	p.mutex.Lock()
	assert.False(t, p.running, "Processor should be marked as not running after stop")
	p.mutex.Unlock()
}

func TestApplicationStopSignal_ShutsDownProcessor(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockLocker := locker.NewMockLocker(mockCtrl)
	mockPartitionService := coordinator.NewMockPartitionService(mockCtrl)
	config := NewDefaultTasklistProcessorConfig()

	processor := NewTasklistProcessor(
		gox.NewCrossFunction(),
		config,
		mockLocker,
		mockPartitionService,
		&ProcessTasklistRequest{
			Domain:    "test-domain",
			TaskList:  "test-tasklist",
			Partition: 1,
			WorkerId:  "test-worker-id",
		},
	)

	mockLocker.EXPECT().AcquireLock(gomock.Any(), gomock.Any()).Return(&locker.AcquireLockResponse{}, nil).AnyTimes()
	mockLocker.EXPECT().ReleaseLock(gomock.Any(), gomock.Any()).Return(&locker.ReleaseLockResponse{}, nil).Times(1)
	mockPartitionService.EXPECT().IsPartitionOwnedByOwner(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(true, nil).AnyTimes()

	_, err := processor.Start(context.Background())
	assert.NoError(t, err)

	time.Sleep(10 * time.Millisecond)

	// Trigger the application-wide stop signal
	_ = processor.Stop(context.Background())

	var isRunning bool
	p := processor.(*tasklistProcessorImpl)
	// Wait for the processor's own stop mechanism to complete
	for i := 0; i < 100; i++ {
		p.mutex.Lock()
		isRunning = p.running
		p.mutex.Unlock()
		if !isRunning {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	assert.False(t, isRunning, "Processor should stop after application context is cancelled")
}

func TestProcessingLoop_SuspendsOnLockRefreshFailure(t *testing.T) {
	deps := setupTest(t)
	// Make intervals fast for testing
	deps.config.WorkInterval = 50 * time.Millisecond
	deps.config.LockAcquireRefreshInterval = 100 * time.Millisecond

	p := deps.processor.(*tasklistProcessorImpl)

	var workDoneCounter int
	var mu sync.Mutex
	p.workCallback = func() {
		mu.Lock()
		workDoneCounter++
		mu.Unlock()
	}

	// Sequence of mock calls:
	// 1. First AcquireLock succeeds.
	// 2. The second call (first refresh) fails.
	// 3. Subsequent calls succeed.
	deps.mockLocker.EXPECT().AcquireLock(gomock.Any(), gomock.Any()).Return(&locker.AcquireLockResponse{}, nil).Times(1)
	deps.mockLocker.EXPECT().AcquireLock(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("transient error")).Times(1)
	deps.mockLocker.EXPECT().AcquireLock(gomock.Any(), gomock.Any()).Return(&locker.AcquireLockResponse{}, nil).AnyTimes()
	deps.mockLocker.EXPECT().ReleaseLock(gomock.Any(), gomock.Any()).Return(&locker.ReleaseLockResponse{}, nil).Times(1)

	_, err := deps.processor.Start(context.Background())
	assert.NoError(t, err)

	// Wait long enough for at least one work tick to occur.
	time.Sleep(deps.config.WorkInterval + 20*time.Millisecond)
	mu.Lock()
	assert.GreaterOrEqual(t, workDoneCounter, 1, "Work should have been done at least once while lock was held")
	mu.Unlock()

	// Wait for the refresh cycle that is expected to fail.
	time.Sleep(deps.config.LockAcquireRefreshInterval)

	// Capture the counter value *after* the failure.
	mu.Lock()
	workCountAfterFailure := workDoneCounter
	mu.Unlock()

	// Wait for another work interval. No work should be done in this period.
	time.Sleep(deps.config.WorkInterval + 20*time.Millisecond)

	mu.Lock()
	assert.Equal(t, workCountAfterFailure, workDoneCounter, "Work should be suspended when lock refresh fails")
	mu.Unlock()

	// Wait for the next refresh cycle, which should succeed and resume work.
	time.Sleep(deps.config.LockAcquireRefreshInterval)

	// Wait for another work interval and check that work has resumed.
	mu.Lock()
	assert.Greater(t, workDoneCounter, workCountAfterFailure, "Work should resume after lock is re-acquired")
	mu.Unlock()

	err = deps.processor.Stop(context.Background())
	assert.NoError(t, err)
}
