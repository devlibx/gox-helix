package locker

import (
	"context"
	"database/sql"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/database"
	errors2 "github.com/pkg/errors"
)

type lockImpl struct {
	gox.CrossFunction
	dataLayer *coordinator.DataLayer
}

func (l *lockImpl) AcquireLock(ctx context.Context, req AcquireLockRequest) (*AcquireLockResponse, error) {
	now := l.Now()
	expiresAt := now.Add(req.TTL)

	result, err := l.dataLayer.AcquireLock(ctx, helixClusterMysql.AcquireLockParams{
		Domain:      req.Domain,
		LockKey:     req.LockKey,
		OwnerID:     req.OwnerId,
		ExpiresAt:   expiresAt,
		ExpiresAt_2: now,
		ExpiresAt_3: now,
	})
	if err != nil {
		return nil, &LockNotAcquiredWithUnknowError{
			Domain:  req.Domain,
			LockKey: req.LockKey,
			OwnerId: req.OwnerId,
			Err:     err,
		}
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, &LockNotAcquiredWithUnknowError{
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

	lastInsertId, err := result.LastInsertId()
	if err != nil && !errors2.Is(err, sql.ErrNoRows) {
		return nil, &LockNotAcquiredWithUnknowError{
			Domain:  req.Domain,
			LockKey: req.LockKey,
			OwnerId: req.OwnerId,
			Err:     err,
		}
	}

	// Why row affected is 2 in case of update on existing lock. Given below is from MySQL
	// 1 for the attempted insert, and
	// 1 for the row actually updated.
	reacquired := rowsAffected == 2
	_ = lastInsertId
	return &AcquireLockResponse{
		Reacquired: reacquired,
	}, nil
}

func NewLock(cf gox.CrossFunction, dataLayer *coordinator.DataLayer) (Locker, error) {
	return &lockImpl{
		CrossFunction: cf,
		dataLayer:     dataLayer,
	}, nil
}
