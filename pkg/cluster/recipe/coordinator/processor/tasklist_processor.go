package processor

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/common"
	"log/slog"
	"sync"
	"time"

	"github.com/devlibx/gox-base/v2"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
)

type ProcessTasklistRequest struct {
	Domain                    string
	TaskList                  string
	Partition                 int
	WorkerId                  string
	ClientFunctionProcessWork coordinator.ClientFunctionProcessWork
}

// tasklistProcessorImpl is the concrete implementation of the TasklistProcessor.
type tasklistProcessorImpl struct {
	gox.CrossFunction
	config               *coordinator.TasklistProcessorConfig
	lockService          locker.Locker
	partitionService     coordinator.PartitionService
	applicationSingleton *common.ApplicationSingleton
	logger               *slog.Logger

	domain    string
	tasklist  string
	partition int
	ownerId   string

	mutex    sync.Mutex
	running  bool
	stopChan chan struct{}
	wg       sync.WaitGroup

	ClientFunctionProcessWork coordinator.ClientFunctionProcessWork
}

// NewTasklistProcessor creates a new tasklist processor instance.
// It also starts a background goroutine to listen for the application-wide stop signal.
func NewTasklistProcessor(
	cf gox.CrossFunction,
	config *coordinator.TasklistProcessorConfig,
	lockService locker.Locker,
	partitionService coordinator.PartitionService,
	request *ProcessTasklistRequest,
	applicationSingleton *common.ApplicationSingleton,
) coordinator.TasklistProcessor {
	p := &tasklistProcessorImpl{
		CrossFunction:             cf,
		config:                    config,
		lockService:               lockService,
		partitionService:          partitionService,
		domain:                    request.Domain,
		tasklist:                  request.TaskList,
		partition:                 request.Partition,
		ClientFunctionProcessWork: request.ClientFunctionProcessWork,
		ownerId:                   request.WorkerId,
		applicationSingleton:      applicationSingleton,
		logger:                    applicationSingleton.GetModuleLogger("tasklist_processor").With("domain", request.Domain, "tasklist", request.TaskList, "partition", request.Partition),
	}

	return p
}

// Start begins the processor's execution loop.
func (t *tasklistProcessorImpl) Start(ctx context.Context) (*coordinator.TasklistProcessResponse, error) {
	t.mutex.Lock()
	if t.running {
		t.mutex.Unlock()
		t.logger.Debug("already started")
		return &coordinator.TasklistProcessResponse{Status: "ALREADY_RUNNING"}, nil
	}

	t.running = true
	t.stopChan = make(chan struct{})
	t.wg.Add(1)
	t.mutex.Unlock()

	go t.processingLoop()

	t.logger.Debug("started")
	return &coordinator.TasklistProcessResponse{Status: "STARTED"}, nil
}

// processingLoop is the main background routine that acquires a lock and performs work.
func (t *tasklistProcessorImpl) processingLoop() {
	defer t.wg.Done()
	defer func() {
		t.mutex.Lock()
		t.running = false
		t.mutex.Unlock()
	}()

	// Persistently try to acquire the lock. Exit if stopped before acquisition.
	if !t.acquireInitialLock() {
		return
	}

	// Ensure the lock is released when we exit.
	// The lock has expiry of N sec - so just in-case we fail to release it, it will be free after sometime
	defer func() {
		if _, err := t.lockService.ReleaseLock(context.Background(), locker.ReleaseLockRequest{
			Domain:  t.domain,
			LockKey: t.getLockKey(),
			OwnerId: t.ownerId,
		}); err != nil {
			t.logger.Error("failed to release lock on shutdown (no issue - it will expire eventually)", "err", err)
		}
	}()

	refreshTicker := time.NewTicker(t.config.LockAcquireRefreshInterval)
	defer refreshTicker.Stop()
	ownershipTicker := time.NewTicker(time.Second)
	defer ownershipTicker.Stop()

	lockHeld := true
	for {
		select {
		case <-refreshTicker.C:
			// We must hold the lock so we refresh it periodically to make sure we have the lock
			if _, err := t.lockService.AcquireLock(context.Background(), locker.AcquireLockRequest{
				Domain:  t.domain,
				LockKey: t.getLockKey(),
				OwnerId: t.ownerId,
				TTL:     t.config.LockTTL,
			}); err != nil {
				if lockHeld {
					t.logger.Error("failed to refresh lock, suspending work", "err", err)
					lockHeld = false
				} else {
					t.logger.Error("still trying to re-acquire lock, work is still suspended", "err", err)
				}
			} else {
				if !lockHeld {
					t.logger.Error("lock re-acquired, resuming work")
				}
				lockHeld = true
			}

		case <-ownershipTicker.C:
			// To be safe we check if we still are owner - if not then we exit the processing
			if owned, err := t.partitionService.IsPartitionOwnedByOwner(context.Background(), t.domain, t.tasklist, t.ownerId, t.partition); err != nil {
				t.logger.Error("failed to check partition ownership", "err", err)
			} else if !owned {
				t.logger.Error("partition is no longer owned by this worker, stopping tasklist processor")
				return
			}

		case <-t.stopChan:
			t.logger.Error("internal stop signal received, stopping tasklist processor")
			return

		default:
			if lockHeld {
				completedCh := make(chan coordinator.WorkResponse, 1)
				t.ClientFunctionProcessWork(
					context.Background(),
					coordinator.Work{
						Domain:           t.domain,
						Tasklist:         t.tasklist,
						WorkerId:         t.ownerId,
						Partition:        t.partition,
						CompletedChannel: completedCh,
					},
				)
				select {
				case workResponse, ok := <-completedCh:
					if ok && workResponse.Err != nil {
  					s.logger.Error("client failed to do the work", "lockKey", t.lockKey, "err", workResponse.Err)
						time.Sleep(10 * time.Millisecond)
					}
				case <-t.stopChan:
					s.logger.Error("internal stop signal received, stopping tasklist processor", "lockKey", t.lockKey)
					return
				}
			} else {
				time.Sleep(10 * time.Millisecond)
			}
		}
	}
}

// acquireInitialLock contains the logic to persistently try to acquire the distributed lock.
func (t *tasklistProcessorImpl) acquireInitialLock() bool {
	for {
		// If we found that we already stopped then not need to continue
		select {
		case <-t.stopChan:
			t.logger.Error("stop signal received before lock initial acquisition, stopping initial lock acquire for tasklist processor")
			return false
		default:
			// No-OP - otherwise this select will block for event in stopChannel
		}

		// Take a lock first
		if _, err := t.lockService.AcquireLock(context.Background(), locker.AcquireLockRequest{
			Domain:  t.domain,
			LockKey: t.getLockKey(),
			OwnerId: t.ownerId,
			TTL:     t.config.LockTTL,
		}); err == nil {
			return true
		} else {
			t.logger.Error("failed to acquire initial lock for tasklist processor, will retry...", "err", err)
		}

		// Retry loc but also stop if we found we are stopped in middle
		select {
		case <-time.After(t.config.InitialLockAcquireRetryInterval):
			// Sleep before we retry to capture lock again
		case <-t.stopChan:
			t.logger.Error("stop signal received while waiting to retry to take initial acquisition, stopping initial lock acquire for tasklist processor")
			return false
		}
	}
}

// Stop gracefully terminates the processor's execution loop.
func (t *tasklistProcessorImpl) Stop(ctx context.Context) error {

	t.mutex.Lock()
	if !t.running {
		t.mutex.Unlock()
		t.logger.Error("tasklist processor not running, nothing to stop")
		return nil
	}

	close(t.stopChan)
	t.running = false
	t.mutex.Unlock()

	// Wait for "processingLoop" to tidy up and stop
	t.wg.Wait()

	t.logger.Error("tasklist processor stopped")
	return nil
}

func (t *tasklistProcessorImpl) getLockKey() string {
	return fmt.Sprintf("%s--%s--partition-%d", t.domain, t.tasklist, t.partition)
}
