package helixLock

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	"github.com/devlibx/gox-helix/pkg/common/lock"
	helixMysql "github.com/devlibx/gox-helix/pkg/common/lock/mysql/database"
	"time"
)

type service struct {
	gox.CrossFunction
	Querier helixMysql.Querier
	Queries *helixMysql.Queries
}

func (s *service) Acquire(ctx context.Context, request *lock.AcquireRequest) (*lock.AcquireResponse, error) {
	// Calculate expiration time
	now := s.Now()
	expiresAt := now.Add(request.TTL)
	
	// Try to upsert the lock
	err := s.Querier.TryUpsertLock(ctx, helixMysql.TryUpsertLockParams{
		LockKey:     request.LockKey,
		OwnerID:     request.OwnerID,
		ExpiresAt:   expiresAt,
		ExpiresAt_2: expiresAt,
		ExpiresAt_3: expiresAt,
		ExpiresAt_4: expiresAt,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to acquire lock during upsert: lock_key=%s", request.LockKey)
	}

	// Get the lock post upsert
	lockAfterUpsert, err := s.Queries.GetLockByLockKey(ctx, request.LockKey)
	if err != nil {
		return nil, errors.Wrap(err, "failed to acquire lock post upsert: lock_key=%s", request.LockKey)
	}

	if lockAfterUpsert.OwnerID == request.OwnerID {
		// Case where existing owner is the owner
		return &lock.AcquireResponse{
			OwnerID:  lockAfterUpsert.OwnerID,
			Acquired: true,
			Epoch:    lockAfterUpsert.Epoch,
		}, nil
	}

	return &lock.AcquireResponse{
		OwnerID:  lockAfterUpsert.OwnerID,
		Acquired: false,
		Epoch:    lockAfterUpsert.Epoch,
	}, nil
}

func (s *service) Release(ctx context.Context, request *lock.ReleaseRequest) (*lock.ReleaseResponse, error) {
	return nil, nil
}

func newHelixDatasourceUsingSqlDb(db *sql.DB) (helixMysql.Querier, *helixMysql.Queries, error) {
	q, err := helixMysql.Prepare(context.Background(), db)
	return q, q, err
}

func newHelixDatasource(config *MySqlConfig) (helixMysql.Querier, *helixMysql.Queries, error) {

	// Setup default values if missing
	config.SetupDefault()

	url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", config.User, config.Password, config.Host, config.Port, config.Database)
	db, err := sql.Open("mysql", url)
	if err != nil {
		return nil, nil, errors.Wrap(err, "error in connecting to database - failed to call sql.Open: database=[%s]", config.Database)
	}

	// Connection configurations
	db.SetMaxOpenConns(config.MaxOpenConnection)
	db.SetMaxIdleConns(config.MaxIdleConnection)
	db.SetConnMaxLifetime(time.Duration(config.ConnMaxLifetimeInSec) * time.Second)
	db.SetConnMaxIdleTime(time.Duration(config.ConnMaxIdleTimeInSec) * time.Second)

	return newHelixDatasourceUsingSqlDb(db)
}

func NewHelixLockMySQLServiceWithSqlDb(cf gox.CrossFunction, db *sql.DB) (lock.Locker, error) {
	if q1, q2, err := newHelixDatasourceUsingSqlDb(db); err != nil {
		return nil, err
	} else {
		return &service{
			CrossFunction: cf,
			Querier:       q1,
			Queries:       q2,
		}, nil
	}
}

func NewHelixLockMySQLService(cf gox.CrossFunction, mySqlConfig *MySqlConfig) (lock.Locker, error) {
	if q1, q2, err := newHelixDatasource(mySqlConfig); err != nil {
		return nil, err
	} else {
		return &service{
			CrossFunction: cf,
			Querier:       q1,
			Queries:       q2,
		}, nil
	}
}
