package processor

import (
	"context"
	"fmt"
	"time"
)

//go:generate mockgen -source=api.go -destination=mocks.go -package=processor

type DomainTasklistProcessRequest struct {
	Partitions []int `json:"partitions"`
}

func (d *DomainTasklistProcessRequest) String() string {
	if len(d.Partitions) > 0 {
		return fmt.Sprintf("DomainTasklistProcessRequest: allocated_partitions=%v", d.Partitions)
	}
	return fmt.Sprintf("DomainTasklistProcessRequest: partitions=[]")
}

type DomainTasklistProcessResponse struct {
}

type DomainTasklistProcessor interface {
	Process(ctx context.Context, request DomainTasklistProcessRequest) (*DomainTasklistProcessResponse, error)

	Stop(context.Context) error
}

// TasklistProcessor defines the interface for a background processor that manages a
// specific partition of a tasklist, ensuring exclusive execution via distributed locking.
type TasklistProcessor interface {
	// Start begins the processor's lifecycle, acquiring a lock and starting background work.
	// This method is idempotent; calling it on an already running processor has no effect.
	Start(ctx context.Context) (*TasklistProcessResponse, error)

	// Stop gracefully terminates the processor, releasing the lock and stopping background work.
	// This method is idempotent; calling it on a stopped processor has no effect.
	Stop(context.Context) error
}

// TasklistProcessResponse is the response from starting a processor.
type TasklistProcessResponse struct {
	Status string `json:"status"`
}

// TasklistProcessorConfig holds the configuration for the TasklistProcessor.
type TasklistProcessorConfig struct {
	// When we start the task list processing, we need to make sure we have the lock on
	// <domain><tasklist><partition>
	// Why - because when shuffle happens the previous worker may still be holding the lock and
	// in process of releasing it
	InitialLockAcquireRetryInterval time.Duration

	// When we are processing the task list - we want to make suer we continue to keep the lock with
	// ourselves
	// So in some interval we re-acquire lock to make sure we continue to own the lock
	LockAcquireRefreshInterval time.Duration

	// How long the lock should be held for. Must be > LockAcquireRefreshInterval
	LockTTL time.Duration
}

// NewDefaultTasklistProcessorConfig creates a new config with default values.
func NewDefaultTasklistProcessorConfig() *TasklistProcessorConfig {
	return &TasklistProcessorConfig{
		InitialLockAcquireRetryInterval: 500 * time.Millisecond,
		LockAcquireRefreshInterval:      5 * time.Second,
		LockTTL:                         15 * time.Second,
	}
}
