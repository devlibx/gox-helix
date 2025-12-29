package processor

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
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
	config           *coordinator.TasklistProcessorConfig
	lockService      locker.Locker
	partitionService coordinator.PartitionService

	domain    string
	tasklist  string
	partition int
	lockKey   string
	ownerId   string
	logPrefix string

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
		lockKey:                   fmt.Sprintf("%s--%s--partition-%d", request.Domain, request.TaskList, request.Partition),
		ownerId:                   request.WorkerId,
		logPrefix:                 fmt.Sprintf("[domain=%s, tasklist=%s, partition=%d, workerId=%s - tasklist_processor] ", request.Domain, request.TaskList, request.Partition, request.WorkerId),
	}

	return p
}

// Start begins the processor's execution loop.
func (t *tasklistProcessorImpl) Start(ctx context.Context) (*coordinator.TasklistProcessResponse, error) {
	t.mutex.Lock()
	if t.running {
		t.mutex.Unlock()
		slog.Debug(t.logPrefix+"tasklist processor already started", "lockKey", t.lockKey)
		return &coordinator.TasklistProcessResponse{Status: "ALREADY_RUNNING"}, nil
	}

	t.running = true
	t.stopChan = make(chan struct{})
	t.wg.Add(1)
	t.mutex.Unlock()

	go t.processingLoop()

	slog.Info(t.logPrefix+"tasklist processor started", "lockKey", t.lockKey, "ownerId", t.ownerId)
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
			LockKey: t.lockKey,
			OwnerId: t.ownerId,
		}); err != nil {
			slog.Error(t.logPrefix+"failed to release lock on shutdown (no issue - it will expire eventually)", "lockKey", t.lockKey, "err", err)
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
				LockKey: t.lockKey,
				OwnerId: t.ownerId,
				TTL:     t.config.LockTTL,
			}); err != nil {
				if lockHeld {
					slog.Error(t.logPrefix+"failed to refresh lock, suspending work", "lockKey", t.lockKey, "err", err)
					lockHeld = false
				} else {
					slog.Error(t.logPrefix+"still trying to re-acquire lock, work is still suspended", "lockKey", t.lockKey, "err", err)
				}
			} else {
				if !lockHeld {
					slog.Info(t.logPrefix+"lock re-acquired, resuming work", "lockKey", t.lockKey)
				}
				lockHeld = true
			}

		case <-ownershipTicker.C:
			// To be safe we check if we still are owner - if not then we exit the processing
			if owned, err := t.partitionService.IsPartitionOwnedByOwner(context.Background(), t.domain, t.tasklist, t.ownerId, t.partition); err != nil {
				slog.Error(t.logPrefix+"failed to check partition ownership", "lockKey", t.lockKey, "err", err)
			} else if !owned {
				slog.Info(t.logPrefix+"partition is no longer owned by this worker, stopping tasklist processor", "lockKey", t.lockKey)
				return
			}

		case <-t.stopChan:
			slog.Info(t.logPrefix+"internal stop signal received, stopping tasklist processor", "lockKey", t.lockKey)
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
						slog.Error(t.logPrefix+"client failed to do the work", "lockKey", t.lockKey, "err", workResponse.Err)
						time.Sleep(10 * time.Millisecond)
					}
				case <-t.stopChan:
					slog.Info(t.logPrefix+"internal stop signal received, stopping tasklist processor", "lockKey", t.lockKey)
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
			slog.Info(t.logPrefix + "stop signal received before lock initial acquisition, stopping initial lock acquire for tasklist processor")
			return false
		default:
			// No-OP - otherwise this select will block for event in stopChannel
		}

		// Take a lock first
		if _, err := t.lockService.AcquireLock(context.Background(), locker.AcquireLockRequest{
			Domain:  t.domain,
			LockKey: t.lockKey,
			OwnerId: t.ownerId,
			TTL:     t.config.LockTTL,
		}); err == nil {
			return true
		} else {
			slog.Error(t.logPrefix+"failed to acquire initial lock for tasklist processor, will retry...", "err", err)
		}

		// Retry loc but also stop if we found we are stopped in middle
		select {
		case <-time.After(t.config.InitialLockAcquireRetryInterval):
			// Sleep before we retry to capture lock again
		case <-t.stopChan:
			slog.Info(t.logPrefix + "stop signal received while waiting to retry to take initial acquisition, stopping initial lock acquire for tasklist processor")
			return false
		}
	}
}

// Stop gracefully terminates the processor's execution loop.
func (t *tasklistProcessorImpl) Stop(ctx context.Context) error {

	t.mutex.Lock()
	if !t.running {
		t.mutex.Unlock()
		slog.Info(t.logPrefix+"tasklist processor not running, nothing to stop", "lockKey", t.lockKey)
		return nil
	}

	close(t.stopChan)
	t.running = false
	t.mutex.Unlock()

	// Wait for "processingLoop" to tidy up and stop
	t.wg.Wait()

	slog.Info(t.logPrefix+"tasklist processor stopped", "lockKey", t.lockKey)
	return nil
}
