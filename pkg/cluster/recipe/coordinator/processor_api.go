package coordinator

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

//go:generate mockgen -source=processor_api.go -destination=processor_mocks.go -package=coordinator

// DomainTasklistProcessRequest holds the set of partitions assigned to a worker for a specific domain and tasklist.
type DomainTasklistProcessRequest struct {
	// Partitions is the list of partition numbers that this worker is now responsible for.
	Partitions []int `json:"partitions"`
}

func (d *DomainTasklistProcessRequest) String() string {
	if len(d.Partitions) > 0 {
		return fmt.Sprintf("DomainTasklistProcessRequest: allocated_partitions=%v", d.Partitions)
	}
	return fmt.Sprintf("DomainTasklistProcessRequest: partitions=[]")
}

// DomainTasklistProcessResponse is the response from processing a domain's tasklists.
type DomainTasklistProcessResponse struct {
	Partitions []int `json:"partitions"`
}

// DomainTasklistProcessor is responsible for managing the lifecycle of all tasklist processors
// for a given domain within a single worker. It dynamically starts and stops individual
// partition processors based on the partitions assigned to it by the central coordinator.
type DomainTasklistProcessor interface {
	// Process synchronizes the running tasklist processors with the provided partition assignment.
	// It stops processors for partitions no longer assigned to this worker and starts new
	// processors for newly assigned partitions.
	Process(ctx context.Context, request DomainTasklistProcessRequest) (*DomainTasklistProcessResponse, error)

	// Stop gracefully terminates all active tasklist processors managed by this domain processor.
	Stop(context.Context) error
}

// TasklistProcessor defines the interface for a background processor that manages a
// specific partition of a tasklist. It ensures exclusive execution for that partition
// across the cluster by acquiring and maintaining a distributed lock.
type TasklistProcessor interface {
	// Start begins the processor's lifecycle. It initiates a background loop that
	// attempts to acquire a distributed lock for its assigned partition. Once the lock is
	// acquired, it will start performing work.
	// This method is idempotent; calling it on an already running processor has no effect.
	Start(ctx context.Context) (*TasklistProcessResponse, error)

	// Stop gracefully terminates the processor. It signals the background loop to stop,
	// releases the distributed lock, and waits for the shutdown to complete.
	// This method is idempotent; calling it on a stopped processor has no effect.
	Stop(context.Context) error
}

// TasklistProcessResponse is the response from starting a processor.
type TasklistProcessResponse struct {
	// Status indicates the result of the Start operation, e.g., "STARTED" or "ALREADY_RUNNING".
	Status string `json:"status"`
}

// TasklistProcessorConfig holds the configuration for the TasklistProcessor,
// primarily related to its distributed locking behavior.
type TasklistProcessorConfig struct {
	// InitialLockAcquireRetryInterval is the duration to wait before retrying to acquire the
	// initial lock if the first attempt fails. This is crucial during partition shuffling,
	// as the previous owner might still be releasing its lock.
	InitialLockAcquireRetryInterval time.Duration

	// LockAcquireRefreshInterval is the interval at which the processor will refresh its
	// lock ownership. This must be less than the LockTTL to prevent the lock from expiring
	// while the processor is active.
	LockAcquireRefreshInterval time.Duration

	// LockTTL is the time-to-live for the distributed lock. If the processor fails to refresh
	// the lock within this duration (e.g., due to a crash or network issue), the lock will
	// expire and become available for another worker to claim.
	LockTTL time.Duration
}

// NewDefaultTasklistProcessorConfig creates a new config with sensible default values for locking.
func NewDefaultTasklistProcessorConfig() *TasklistProcessorConfig {
	return &TasklistProcessorConfig{
		InitialLockAcquireRetryInterval: 500 * time.Millisecond,
		LockAcquireRefreshInterval:      5 * time.Second,
		LockTTL:                         15 * time.Second,
	}
}

// Work represents a single unit of work to be processed for a specific tasklist partition.
// It is created by a TasklistProcessor and sent to a work channel for execution.
type Work struct {
	Domain           string              `json:"domain"`
	Tasklist         string              `json:"tasklist"`
	WorkerId         string              `json:"worker_id"`
	Partition        int                 `json:"partition"`
	CompletedChannel chan<- WorkResponse `json:"-"`
	WorkDoneOnce     *sync.Once          `json:"-"`
}

func (w *Work) String() string {
	return fmt.Sprintf("Work{Domain:%s, Tasklist:%s, Partition:%d, WorkerId=%s}", w.Domain, w.Tasklist, w.Partition, w.WorkerId)
}

func (w *Work) DeferFunc() {
	func() {
		if err := recover(); err != nil {
			slog.Error("got error in work processing function (caught in defer)",
				"domain", w.Domain, "tasklist", w.Tasklist, "partition", w.Partition, "worker_id", w.WorkerId, "error", err)
		}
		w.CompletedChannel <- WorkResponse{}
		close(w.CompletedChannel)
	}()
}

func (w *Work) DeferFuncWithError(err error) {
	func() {
		if err := recover(); err != nil {
			slog.Error("got error in work processing function (caught in defer)",
				"domain", w.Domain, "tasklist", w.Tasklist, "partition", w.Partition, "worker_id", w.WorkerId, "error", err)
		}
		w.CompletedChannel <- WorkResponse{Err: err}
		close(w.CompletedChannel)
	}()
}

// WorkResponse is sent over the CompletedChannel to indicate the result of processing a Work unit.
type WorkResponse struct {
	// Err contains any error that occurred during the execution of the work.
	Err error
}
