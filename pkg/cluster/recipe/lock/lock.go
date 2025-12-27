package locker

import (
	"context"
	"fmt"
	"time"
)

//go:generate mockgen -source=lock.go -destination=mocks.go -package=locker

// LockNotAcquiredError represents an error where a lock could not be acquired because it's already held.
type LockNotAcquiredError struct {
	Domain  string
	LockKey string
	OwnerId string
	Err     error
}

func (e *LockNotAcquiredError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("lock not acquired: domain=%s, lockKey=%s, ownerId=%s, err=%s", e.Domain, e.LockKey, e.OwnerId, e.Err.Error())
	}
	return fmt.Sprintf("lock not acquired: domain=%s, lockKey=%s, ownerId=%s", e.Domain, e.LockKey, e.OwnerId)
}

// LockNotAcquiredWithUnknownError represents an error where a lock could not be acquired due to an underlying unknown error.
type LockNotAcquiredWithUnknownError struct {
	Domain  string
	LockKey string
	OwnerId string
	Err     error
}

func (e *LockNotAcquiredWithUnknownError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("lock not acquired with unknown error: domain=%s, lockKey=%s, ownerId=%s, err=%s", e.Domain, e.LockKey, e.OwnerId, e.Err.Error())
	}
	return fmt.Sprintf("lock not acquired with unknown error: domain=%s, lockKey=%s, ownerId=%s", e.Domain, e.LockKey, e.OwnerId)
}

// AcquireLockRequest encapsulates the parameters required to acquire a distributed lock.
type AcquireLockRequest struct {
	Domain  string
	LockKey string
	OwnerId string
	TTL     time.Duration // Time-to-live for the lock. The lock will expire after this duration if not refreshed.
}

// AcquireLockResponse is the response returned after attempting to acquire a lock.
type AcquireLockResponse struct {
	// Currently empty, but can be extended to include information about the acquired lock (e.g., whether it was reacquired).
}

// ReleaseLockRequest encapsulates the parameters required to release a distributed lock.
type ReleaseLockRequest struct {
	Domain  string
	LockKey string
	OwnerId string
}

// ReleaseLockResponse is the response returned after attempting to release a lock.
type ReleaseLockResponse struct {
	// Currently empty, but can be extended if needed.
}

// Locker defines the interface for a distributed locking service.
// Implementations of this interface provide mechanisms to acquire, refresh, and release locks
// across multiple processes or machines to ensure exclusive access to shared resources.
type Locker interface {
	// AcquireLock attempts to acquire a distributed lock.
	// If the lock is already held by another owner and has not expired, the acquisition will fail.
	// If the lock is held by the same owner, its TTL will be extended (reacquired).
	// If the lock is expired, any owner can acquire it.
	//
	// Parameters:
	// - ctx: The context for the operation, allowing for cancellation and timeouts.
	// - req: AcquireLockRequest containing details about the lock to acquire (domain, key, owner, TTL).
	//
	// Returns:
	// - *AcquireLockResponse: A response object, which may contain details about the acquisition.
	// - error: An error if the lock could not be acquired or if an internal error occurred.
	AcquireLock(ctx context.Context, req AcquireLockRequest) (*AcquireLockResponse, error)

	// ReleaseLock attempts to release a previously acquired distributed lock.
	// A lock can only be released by its current owner. Releasing a lock makes it available
	// for other processes to acquire immediately, even if its TTL has not yet expired.
	//
	// Parameters:
	// - ctx: The context for the operation.
	// - req: ReleaseLockRequest containing details about the lock to release (domain, key, owner).
	//
	// Returns:
	// - *ReleaseLockResponse: A response object.
	// - error: An error if the lock could not be released (e.g., not found, not owner, internal error).
	ReleaseLock(ctx context.Context, req ReleaseLockRequest) (*ReleaseLockResponse, error)
}
