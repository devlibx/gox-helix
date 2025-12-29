package coordinator

import (
	"context"
	"database/sql"
	"github.com/devlibx/gox-base/v2/errors"
	helixCoordinatorMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/database"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	"github.com/devlibx/gox-helix/pkg/common"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	"github.com/go-sql-driver/mysql"
	"log/slog"
	"math/rand"
	"time"
)

type PartitionDistributionService interface {
	Process(ctx context.Context, request DistributionRequest) error
}

type PartitionDistributionServiceImpl struct {
	lockService          locker.Locker
	distributor          DistributorStrategy
	partitionService     PartitionService
	applicationSingleton *common.ApplicationSingleton
	logger               *slog.Logger

	workerDataLayer      *worker.DataLayer
	domainDataLayer      *domain.DataLayer
	coordinatorDataLayer *DataLayer
	sqlDb                *sql.DB

	distributorStrategy DistributorStrategy
}

func NewPartitionDistributionService(
	lockService locker.Locker,
	distributor DistributorStrategy,
	partitionService PartitionService,
	applicationSingleton *common.ApplicationSingleton,
	workerDataLayer *worker.DataLayer,
	coordinatorDataLayer *DataLayer,
	domainDataLayer *domain.DataLayer,
	distributorStrategy DistributorStrategy,
	ch databaseCommon.ConnectionHolder,
) (PartitionDistributionService, error) {
	return &PartitionDistributionServiceImpl{
		lockService:          lockService,
		distributor:          distributor,
		partitionService:     partitionService,
		applicationSingleton: applicationSingleton,
		workerDataLayer:      workerDataLayer,
		coordinatorDataLayer: coordinatorDataLayer,
		domainDataLayer:      domainDataLayer,
		distributorStrategy:  distributorStrategy,
		sqlDb:                ch.GetHelixMasterDbConnection(),
		logger:               applicationSingleton.GetModuleLogger("partition_distributor"),
	}, nil
}

func (p *PartitionDistributionServiceImpl) Process(ctx context.Context, request DistributionRequest) error {
	ticker := time.NewTicker(time.Duration(200+rand.Intn(2000)) * time.Millisecond)
	defer ticker.Stop()

	p.logger.Info("process started")
	ownerId := p.applicationSingleton.GetWorkerId()
	for {
		select {
		case <-ctx.Done():
			goto exit
		case <-ticker.C:
			// Acquire a cluster-wide lock to ensure only one instance runs the distribution
			lockKey := "partition-distributor-" + request.DomainName + "-" + request.TaskList
			if _, err := p.lockService.AcquireLock(ctx, locker.AcquireLockRequest{
				Domain:  request.DomainName,
				LockKey: lockKey,
				OwnerId: ownerId,
				TTL:     10 * time.Second,
			}); err == nil {
				if err = p.internalProcess(ctx, request); err != nil {
					p.logger.Error("failed to run partition distributor periodic process", "err", err.Error())
				}
			} else {
				p.logger.Debug("(expected - not all nodes will get lock) lock not acquired")
			}

			// Reset tick after 5-10 sec
			delay := randomDelay(5*time.Second, 10*time.Second)
			ticker.Reset(delay)
		}
	}

exit:
	p.logger.Info("[SHUTDOWN] partition distributor process stopped (context done)")
	return nil
}

func (p *PartitionDistributionServiceImpl) internalProcessWithRetries(ctx context.Context, request DistributionRequest) error {
	var err error
	for i := 0; i < 10; i++ {
		if err = p.internalProcess(ctx, request); err == nil {
			return nil
		} else {
			if mySqlErr, ok := errors.AsTyped[*mysql.MySQLError](err); ok && mySqlErr.Number == 1213 {
				p.logger.Warn("(expected if unfrequent or at boot time) got error while persisting distribution (retry)", "err", err.Error(), "retry", i)
				time.Sleep(100 * time.Millisecond)
			} else {
				return errors.Wrap(err, "got error while persisting distribution: domain=%s, tasklist=%s", request.DomainName, request.TaskList)
			}
		}
	}
	return err
}

func (p *PartitionDistributionServiceImpl) internalProcess(ctx context.Context, request DistributionRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	domainName := request.DomainName
	tasklist := request.TaskList

	// Make sure we do this in transaction to ensure a single write
	tx, err := p.sqlDb.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction for persisting distribution: domainName=%s, tasklist=%s", domainName, tasklist)
	}
	defer func() {
		if err != nil {
			if e := tx.Rollback(); e != nil {
				err = errors.Wrap(err, "error in rolling back in partition distribution write to DB: domain=%s, tasklist=%s, rollbackError=%s", domainName, tasklist, e.Error())
			}
		} else {
			if e := tx.Commit(); e != nil {
				err = errors.Wrap(e, "failed to commit the transaction for persisting distribution write to DB: domainName=%s, tasklist=%s", domainName, tasklist)
			}
		}
	}()

	// Make queries to use for single transaction
	workerTx := p.workerDataLayer.Queries.WithTx(tx)
	domainTx := p.domainDataLayer.Queries.WithTx(tx)
	coordinatorTx := p.coordinatorDataLayer.Queries.WithTx(tx)

	// We pass all these queries in the ctx - the respective code will use these if seen
	ctx = context.WithValue(ctx, "*helixWorkerMysql.Queries", workerTx)
	ctx = context.WithValue(ctx, "*helixCoordinatorMysql.Queries", coordinatorTx)
	ctx = context.WithValue(ctx, "*helixDomainMysql.Queries", domainTx)

	// Step 1: Calculate new distribution
	response, err := p.distributor.Distribute(ctx, request)
	if err != nil {
		return errors.Wrap(err, "failed to run distribution algorithm for domain=%s, tasklist=%s", request.DomainName, request.TaskList)
	}

	// Step 2: Mark all existing records for this task list as inactive.
	// This handles workers that no longer own any partitions.
	if err = coordinatorTx.MarkPartitionInactive(ctx, helixCoordinatorMysql.MarkPartitionInactiveParams{Domain: domainName, Tasklist: tasklist}); err != nil {
		return errors.Wrap(err, "failed to mark old partitions as inactive: domainName=%s, tasklist=%s", domainName, tasklist)
	}

	// Step 3: Upsert the new state for each worker.
	for ownerId, partitionIds := range response.GetMappingsAsString() {
		params := helixCoordinatorMysql.UpsertPartitionParams{
			Domain:   domainName,
			Tasklist: tasklist,
			OwnerID:  ownerId,
			Metadata: sql.NullString{String: string(partitionIds), Valid: true},
			Status:   int8(databaseCommon.PartitionAssignmentStatusAssigned),
		}
		if err = coordinatorTx.UpsertPartition(ctx, params); err != nil {
			return errors.Wrap(err, "failed to upsert partition for owner: domainName=%s, tasklist=%s, ownerId=%s", domainName, tasklist, ownerId)
		}
	}

	return nil
}

func randomDelay(min, max time.Duration) time.Duration {
	return min + time.Duration(rand.Int63n(int64(max-min)))
}
