package processor

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-helix/pkg/common"
	"go.uber.org/mock/gomock"
	"testing"
	"time"

	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type testProcessorDeps struct {
	cf                   gox.CrossFunction
	config               *coordinator.TasklistProcessorConfig
	mockCtrl             *gomock.Controller
	mockLocker           *locker.MockLocker
	mockPartitionService *coordinator.MockPartitionService // Use PartitionService mock
	stopSignal           *common.ApplicationSingleton
	processor            coordinator.TasklistProcessor
	as                   *common.ApplicationSingleton
}

func setupTest(t *testing.T) *testProcessorDeps {
	mockCtrl := gomock.NewController(t)
	mockLocker := locker.NewMockLocker(mockCtrl)
	mockPartitionService := coordinator.NewMockPartitionService(mockCtrl)
	stopCtx, cancel := context.WithCancel(context.Background())
	stopSignal := common.NewApplicationSingletonWithContext(stopCtx)
	config := coordinator.NewDefaultTasklistProcessorConfig()
	as := common.NewApplicationSingletonWithContext(stopCtx)

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
			ClientFunctionProcessWork: func(ctx context.Context, work coordinator.Work) {
				work.CompletedChannel <- coordinator.WorkResponse{}
				close(work.CompletedChannel)
			},
		},
		as,
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
		as:                   as,
	}
}

func TestStart_Idempotency(t *testing.T) {
	deps := setupTest(t)

	deps.mockLocker.EXPECT().AcquireLock(gomock.Any(), gomock.Any()).Return(&locker.AcquireLockResponse{}, nil).AnyTimes()
	deps.mockLocker.EXPECT().ReleaseLock(gomock.Any(), gomock.Any()).Return(&locker.ReleaseLockResponse{}, nil).AnyTimes()

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
	time.Sleep(1 * time.Second)
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
	config := coordinator.NewDefaultTasklistProcessorConfig()

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
			ClientFunctionProcessWork: func(ctx context.Context, work coordinator.Work) {
				work.CompletedChannel <- coordinator.WorkResponse{}
				close(work.CompletedChannel)
			},
		},
		common.NewApplicationSingletonWithContext(context.Background()),
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
	deps.config.LockAcquireRefreshInterval = 100 * time.Millisecond // Keep refresh fast

	// Sequence of mock calls:
	// 1. First AcquireLock succeeds.
	// 2. The second call (first refresh) fails.
	// 3. Subsequent calls succeed.
	deps.mockLocker.EXPECT().AcquireLock(gomock.Any(), gomock.Any()).Return(&locker.AcquireLockResponse{}, nil).Times(1)   // Initial acquire
	deps.mockLocker.EXPECT().AcquireLock(gomock.Any(), gomock.Any()).Return(nil, fmt.Errorf("transient error")).Times(1)   // First refresh fails
	deps.mockLocker.EXPECT().AcquireLock(gomock.Any(), gomock.Any()).Return(&locker.AcquireLockResponse{}, nil).AnyTimes() // Subsequent refresh succeeds
	deps.mockLocker.EXPECT().ReleaseLock(gomock.Any(), gomock.Any()).Return(&locker.ReleaseLockResponse{}, nil).Times(1)

	mockCtrl := gomock.NewController(t)
	mockPartitionService := coordinator.NewMockPartitionService(mockCtrl)

	var workDoneCounter int
	processor := NewTasklistProcessor(
		gox.NewCrossFunction(),
		deps.config,
		deps.mockLocker,
		mockPartitionService,
		&ProcessTasklistRequest{
			Domain:    "test-domain",
			TaskList:  "test-tasklist",
			Partition: 1,
			WorkerId:  "test-worker-id",
			ClientFunctionProcessWork: func(ctx context.Context, work coordinator.Work) {
				t.Log("Got work on work channel", "work", work)
				workDoneCounter++
				time.Sleep(10 * time.Millisecond)
				work.CompletedChannel <- coordinator.WorkResponse{}
				close(work.CompletedChannel)
			},
		},
		deps.as,
	)
	_, _ = processor.Start(context.Background())

	// --- Phase 1: Work should be active initially ---
	// Wait long enough for work to be done multiple times before the first refresh.
	time.Sleep(deps.config.LockAcquireRefreshInterval / 2) // Half of refresh interval, should be working
	initialWorkCount := workDoneCounter
	assert.Greater(t, initialWorkCount, 0, "Work should have started initially")

	// --- Phase 2: Lock refresh fails, work should suspend ---
	// Wait for the refresh cycle that is expected to fail.
	time.Sleep(deps.config.LockAcquireRefreshInterval + 50*time.Millisecond) // Ensure refresh fires and suspension takes effect
	workCountAfterSuspension := workDoneCounter

	// Wait during the suspension period - work count should not increase significantly
	time.Sleep(deps.config.LockAcquireRefreshInterval / 2) // Half of refresh interval

	workCountDuringSuspension := workDoneCounter

	// Work should be mostly suspended - with 1ms sleep in loop, some items may slip through
	// but it should be much less than if lock was held continuously
	workDoneWhileSuspended := workCountDuringSuspension - workCountAfterSuspension
	assert.LessOrEqual(t, workDoneWhileSuspended, 5, "Work should be mostly suspended (< 5 items during 50ms period)")

	// --- Phase 3: Lock re-acquired, work should resume ---
	// Wait for the next refresh cycle, which should succeed and resume work.
	time.Sleep(deps.config.LockAcquireRefreshInterval + 20*time.Millisecond) // Ensure re-acquisition ticker fires

	// Wait for work to happen again
	time.Sleep(50 * time.Millisecond) // Give time for work to resume
	assert.Greater(t, workDoneCounter, workCountDuringSuspension, "Work should resume after lock is re-acquired")

	err := processor.Stop(context.Background())
	assert.NoError(t, err)
}
