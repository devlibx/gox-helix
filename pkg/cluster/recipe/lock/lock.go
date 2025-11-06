package locker

import (
	"context"
	"fmt"
	"time"
)

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

type LockNotAcquiredWithUnknowError struct {
	Domain  string
	LockKey string
	OwnerId string
	Err     error
}

func (e *LockNotAcquiredWithUnknowError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("lock not acquired with unknown error: domain=%s, lockKey=%s, ownerId=%s, err=%s", e.Domain, e.LockKey, e.OwnerId, e.Err.Error())
	}
	return fmt.Sprintf("lock not acquired with unknown error: domain=%s, lockKey=%s, ownerId=%s", e.Domain, e.LockKey, e.OwnerId)
}

type AcquireLockRequest struct {
	Domain  string
	LockKey string
	OwnerId string
	TTL     time.Duration
}

type AcquireLockResponse struct {
}

type Locker interface {
	AcquireLock(ctx context.Context, req AcquireLockRequest) (*AcquireLockResponse, error)
}
