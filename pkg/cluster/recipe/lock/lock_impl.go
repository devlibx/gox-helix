package locker

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	helixLockMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock/database"
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
			Err:     err,
		}
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, &LockNotAcquiredWithUnknownError{
			Domain:  req.Domain,
			LockKey: req.LockKey,
			OwnerId: req.OwnerId,
			Err:     err,
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

func NewLocker(cf gox.CrossFunction, dataLayer *DataLayer) (Locker, error) {
	return &lockImpl{
		CrossFunction: cf,
		dataLayer:     dataLayer,
	}, nil
}
