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
	Epoch    int64 // Version token for optimistic locking
}

type ReleaseRequest struct {
}

type ReleaseResponse struct {
}

// DBLockRecord represents a lock record as stored in the database
type DBLockRecord struct {
	LockKey   string    `json:"lock_key"`
	OwnerID   string    `json:"owner_id"`
	ExpiresAt time.Time `json:"expires_at"`
	Epoch     int64     `json:"epoch"`
	Status    string    `json:"status"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
