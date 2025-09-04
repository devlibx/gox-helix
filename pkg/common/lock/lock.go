package lock

import "context"

// Locker is the interface that wraps the basic lock and unlock methods.
type Locker interface {
	// Acquire acquires the lock. If the lock is already held by another owner,
	// it will wait until the lock is released.
	Acquire(ctx context.Context, key, ownerID string) error

	// Release releases the lock.
	Release(ctx context.Context, key, ownerID string) error

	// Renew renews the lock.
	Renew(ctx context.Context, key, ownerID string) error
}
