package processor

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/devlibx/gox-base/v2"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/common"
	"github.com/google/uuid"
)

// tasklistProcessorImpl is the concrete implementation of the TasklistProcessor.
type tasklistProcessorImpl struct {
	gox.CrossFunction
	config       *TasklistProcessorConfig
	lockService  locker.Locker
	stopSignal   *common.ApplicationStopSignal
	domain       string
	tasklist     string
	partition    int
	lockKey      string
	ownerId      string
	logPrefix    string
	workCallback func() // For testing purposes

	mutex    sync.Mutex
	running  bool
	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewTasklistProcessor creates a new tasklist processor instance.
// It also starts a background goroutine to listen for the application-wide stop signal.
func NewTasklistProcessor(
	cf gox.CrossFunction,
	config *TasklistProcessorConfig,
	lockService locker.Locker,
	stopSignal *common.ApplicationStopSignal,
	domain string,
	tasklist string,
	partition int,
) TasklistProcessor {
	p := &tasklistProcessorImpl{
		CrossFunction: cf,
		config:        config,
		lockService:   lockService,
		stopSignal:    stopSignal,
		domain:        domain,
		tasklist:      tasklist,
		partition:     partition,
		lockKey:       fmt.Sprintf("%s--%s--partition-%d", domain, tasklist, partition),
		ownerId:       uuid.NewString(),
		logPrefix:     fmt.Sprintf("[domain=%s, tasklist=%s, partition=%d - tasklist_processor] ", domain, tasklist, partition),
	}

	// Start a listener that will stop this processor when the application-wide signal is fired.
	go func() {
		<-p.stopSignal.Ctx.Done()
		slog.Info(p.logPrefix+"application stop signal received, stopping processor", "lockKey", p.lockKey, "ownerId", p.ownerId)
		_ = p.Stop(context.Background())
	}()

	return p
}

// Start begins the processor's execution loop.
func (t *tasklistProcessorImpl) Start(ctx context.Context) (*TasklistProcessResponse, error) {
	t.mutex.Lock()
	if t.running {
		t.mutex.Unlock()
		slog.Info(t.logPrefix+"tasklist processor already started", "lockKey", t.lockKey)
		return &TasklistProcessResponse{Status: "ALREADY_RUNNING"}, nil
	}

	t.running = true
	t.stopChan = make(chan struct{})
	t.wg.Add(1)
	t.mutex.Unlock()

	go t.processingLoop()

	slog.Info(t.logPrefix+"tasklist processor started", "lockKey", t.lockKey, "ownerId", t.ownerId)
	return &TasklistProcessResponse{Status: "STARTED"}, nil
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
	defer func() {
		if _, err := t.lockService.ReleaseLock(context.Background(), locker.ReleaseLockRequest{
			Domain:  t.domain,
			LockKey: t.lockKey,
			OwnerId: t.ownerId,
		}); err != nil {
			slog.Error(t.logPrefix+"failed to release lock on shutdown (no issue - it will expire eventually)", "lockKey", t.lockKey, "err", err)
		}
	}()

	workTicker := time.NewTicker(t.config.WorkInterval)
	defer workTicker.Stop()
	refreshTicker := time.NewTicker(t.config.LockAcquireRefreshInterval)
	defer refreshTicker.Stop()

	lockHeld := true
	for {
		select {
		case <-workTicker.C:
			if lockHeld {
				slog.Info(t.logPrefix + "...processing...")
				if t.workCallback != nil {
					t.workCallback()
				}
			} else {
				slog.Info(t.logPrefix + "...suspended (lock not held)...")
			}

		case <-refreshTicker.C:
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

		case <-t.stopChan:
			slog.Info(t.logPrefix+"internal stop signal received, stopping tasklist processor", "lockKey", t.lockKey)
			return
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
		case <-time.After(t.config.LockAcquireRefreshInterval):
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

	t.wg.Wait()

	slog.Info(t.logPrefix+"tasklist processor stopped", "lockKey", t.lockKey)
	return nil
}
