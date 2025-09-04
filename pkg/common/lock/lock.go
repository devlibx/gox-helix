package lock

import (
	"context"
	"time"
)

// Locker is the interface that wraps the basic lock and unlock methods.
type Locker interface {
	// Acquire acquires the lock. If the lock is already held by another owner,
	// it will wait until the lock is released.
	Acquire(ctx context.Context, request *AcquireRequest) (*AcquireResponse, error)

	// Release releases the lock.
	Release(ctx context.Context, request *ReleaseRequest) (*ReleaseResponse, error)
}

type AcquireRequest struct {
	LockKey string
	OwnerID string
	TTL     time.Duration
}

type AcquireResponse struct {
	OwnerID  string
	Acquired bool
}

type ReleaseRequest struct {
}

type ReleaseResponse struct {
}
