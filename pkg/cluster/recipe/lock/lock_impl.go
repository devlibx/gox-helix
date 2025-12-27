package locker

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	helixLockMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock/database"
	"time"
)

type lockImpl struct {
	gox.CrossFunction
	dataLayer *DataLayer
}

func (l *lockImpl) AcquireLock(ctx context.Context, req AcquireLockRequest) (*AcquireLockResponse, error) {
	now := l.Now()
	expiresAt := now.Add(req.TTL)

	result, err := l.dataLayer.AcquireLock(ctx, helixLockMysql.AcquireLockParams{
		Domain:    req.Domain,
		LockKey:   req.LockKey,
		OwnerID:   req.OwnerId,
		ExpiresAt: expiresAt,
		Column5:   now,
		Column6:   now,
		Column7:   now,
	})
	if err != nil {
		return nil, &LockNotAcquiredWithUnknownError{
			Domain:  req.Domain,
			LockKey: req.LockKey,
			OwnerId: req.OwnerId,
			Err:     errors.Wrap(err, "failed to acquire lock at data layer"),
		}
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, &LockNotAcquiredWithUnknownError{
			Domain:  req.Domain,
			LockKey: req.LockKey,
			OwnerId: req.OwnerId,
			Err:     errors.Wrap(err, "failed to get rows affected"),
		}
	}

	if rowsAffected == 0 {
		return nil, &LockNotAcquiredError{
			Domain:  req.Domain,
			LockKey: req.LockKey,
			OwnerId: req.OwnerId,
		}
	}

	return &AcquireLockResponse{}, nil
}

func (l *lockImpl) ReleaseLock(ctx context.Context, req ReleaseLockRequest) (*ReleaseLockResponse, error) {
	_, err := l.dataLayer.ReleaseLock(ctx, helixLockMysql.ReleaseLockParams{
		ExpiresAt: l.Now().Add(-1 * time.Second), // Set to 1 second in the past to ensure it's expired
		Domain:    req.Domain,
		LockKey:   req.LockKey,
		OwnerID:   req.OwnerId,
	})
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("failed to release lock for domain=%s, lockKey=%s, ownerId=%s", req.Domain, req.LockKey, req.OwnerId))
	}
	return &ReleaseLockResponse{}, nil
}

func NewLocker(cf gox.CrossFunction, dataLayer *DataLayer) (Locker, error) {
	return &lockImpl{
			CrossFunction: cf,
			dataLayer:     dataLayer,
		},
		nil
}
