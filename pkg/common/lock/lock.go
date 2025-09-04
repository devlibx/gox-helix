package lock

import "context"

// Locker is the interface that wraps the basic lock and unlock methods.
type Locker interface {
	// Acquire acquires the lock. If the lock is already held by another owner,
	// it will wait until the lock is released.
	Acquire(ctx context.Context, request *AcquireRequest) (*AcquireResponse, error)

	// Release releases the lock.
	Release(ctx context.Context, request *ReleaseRequest) (*ReleaseResponse, error)
}

type AcquireRequest struct {
	OwnerID string
}

type AcquireResponse struct {
	OwnerID string
}

type ReleaseRequest struct {
}

type ReleaseResponse struct {
}
