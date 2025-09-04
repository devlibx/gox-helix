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

	// Try to acquire the lock using upsert with conditional update
	result, err := s.Queries.TryAcquireLock(ctx, helixMysql.TryAcquireLockParams{
		LockKey:   request.LockKey,
		OwnerID:   request.OwnerID,
		ExpiresAt: expiresAt,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to try acquire lock: %w", err)
	}

	// Check if lock was successfully acquired
	// For INSERT ... ON DUPLICATE KEY UPDATE:
	// - RowsAffected = 1 if new row inserted (lock acquired)
	// - RowsAffected = 2 if existing row updated (lock acquired after expiration)
	// - RowsAffected = 0 if no change made (lock held by another owner and not expired)
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, fmt.Errorf("failed to get rows affected: %w", err)
	}

	acquired := rowsAffected > 0

	return &lock.AcquireResponse{
		OwnerID:  request.OwnerID,
		Acquired: acquired,
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
