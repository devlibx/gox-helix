package coordinator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/devlibx/gox-base/v2/errors"
	helixCoordinatorMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/database"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	helixDomainMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/domain/database"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	"github.com/devlibx/gox-helix/pkg/common"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	"github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"log/slog"
	"math/rand"
	"time"
)

type PartitionDistributionServiceV1Impl struct {
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

func NewPartitionDistributionServiceV1(
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
	return &PartitionDistributionServiceV1Impl{
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

func (p *PartitionDistributionServiceV1Impl) Process(ctx context.Context, request DistributionRequest) error {
	ticker := time.NewTicker(time.Duration(200+rand.Intn(2000)) * time.Millisecond)
	defer ticker.Stop()

	p.logger.Info("partition distributor process started", "domain", request.DomainName, "tasklist", request.TaskList)
	for {
		select {
		case <-ctx.Done():
			goto exit
		case <-ticker.C:
			// Acquire a cluster-wide lock to ensure only one instance runs the distribution
			ownerId := uuid.NewString()
			lockKey := "partition-distributor-" + request.DomainName + "-" + request.TaskList
			if _, err := p.lockService.AcquireLock(ctx, locker.AcquireLockRequest{
				Domain:  request.DomainName,
				LockKey: lockKey,
				OwnerId: ownerId,
				TTL:     30 * time.Second,
			}); err == nil {
				// If lock is acquired, run the internal process
				if err = p.internalProcess(ctx, request); err != nil {
					p.logger.Error("failed to run partition distributor internal process", "err", err.Error(), "domain", request.DomainName, "tasklist", request.TaskList)
				}
			} else {
				p.logger.Debug("(expected - not all nodes will get lock) lock not acquired for partition distributor", "domain", request.DomainName, "tasklist", request.TaskList)
			}

			// Reset tick after 5-10 sec
			delay := randomDelay(5*time.Second, 10*time.Second)
			ticker.Reset(delay)
		}
	}

exit:
	p.logger.Info("[SHUTDOWN] partition distributor process stopped (context done)", "domain", request.DomainName, "tasklist", request.TaskList)
	return nil
}

func (p *PartitionDistributionServiceV1Impl) internalProcessWithRetries(ctx context.Context, request DistributionRequest) error {
	var err error
	for i := 0; i < 10; i++ {
		err = p.internalProcess(ctx, request)
		if err != nil {
			if mySqlErr, ok := errors.AsTyped[*mysql.MySQLError](err); ok && mySqlErr.Number == 1213 {
				slog.Warn("(expected if unfrequent or at boot time) got error while persisting distribution (retry)", "err", err.Error(), "domain", request.DomainName, "tasklist", request.TaskList, "retry", i)
				time.Sleep(100 * time.Millisecond)
			} else {
				return errors.Wrap(err, "got error while persisting distribution: domain=%s, tasklist=%s", request.DomainName, request.TaskList)
			}
		} else {
			break
		}
	}
	return err
}

func (p *PartitionDistributionServiceV1Impl) internalProcess(ctx context.Context, request DistributionRequest) error {
	domainName := request.DomainName
	tasklist := request.TaskList

	tx, err := p.sqlDb.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction for persisting distribution: domainName=%s, tasklist=%s", domainName, tasklist)
	}
	defer func() {
		if err != nil {
			if e := tx.Rollback(); e != nil {
				slog.Error("error in rolling back in partition distribution write to DB", "domainName", domainName, "tasklist", tasklist, "error", e)
			}
		} else {
			if err = tx.Commit(); err != nil {
				p.logger.Error("failed to commit the transaction for persisting distribution", "error", err.Error())
			}
		}
	}()

	workerTx := p.workerDataLayer.Queries.WithTx(tx)
	domainTx := p.domainDataLayer.Queries.WithTx(tx)
	coordinatorTx := p.coordinatorDataLayer.Queries.WithTx(tx)

	// Get all workers which can take partitions
	activeWorkersToAssignPartitions, err := workerTx.GetAllActiveWorkersByDomain(ctx, domainName)
	if err != nil {
		return errors.Wrap(err, "get active workers failed for domainName "+domainName)
	}

	// Get partition count
	taskListToHandle, err := domainTx.GetDomainByDomainAndTasklist(ctx, helixDomainMysql.GetDomainByDomainAndTasklistParams{
		Domain:   domainName,
		Tasklist: tasklist,
	})
	if err != nil {
		return errors.Wrap(err, "get task list info failed for domainName "+domainName)
	}
	partitionCount := int(taskListToHandle.PartitionCount)

	params := helixCoordinatorMysql.GetAllValidPartitionForDomainAndTaskListParams{
		Domain:   domainName,
		Tasklist: tasklist,
	}
	dbMappings, err := coordinatorTx.GetAllValidPartitionForDomainAndTaskList(ctx, params)

	partitionMappings := make([]WorkerPartitionMapping, 0, len(dbMappings))
	for _, row := range dbMappings {
		if !row.Metadata.Valid || row.Metadata.String == "" {
			slog.Warn("worker has partition mapping row with no metadata", "owner_id", row.OwnerID, "domainName", domainName, "tasklist", tasklist)
			continue
		}
		var partitionIds []int
		if err := json.Unmarshal([]byte(row.Metadata.String), &partitionIds); err != nil {
			slog.Error("failed to unmarshal partition metadata for worker", "owner_id", row.OwnerID, "metadata", row.Metadata.String, "err", err)
			continue
		}
		mapping := make(map[int]DistributionMapping, len(partitionIds))
		for _, pId := range partitionIds {
			var assignmentStatus databaseCommon.PartitionAssignmentStatus
			if row.Status == 1 {
				assignmentStatus = databaseCommon.PartitionAssignmentStatusAssigned
			} else {
				assignmentStatus = databaseCommon.PartitionAssignmentStatusUnassigned
			}
			mapping[pId] = DistributionMapping{
				OwnerId: row.OwnerID,
				Status:  assignmentStatus,
			}
		}
		partitionMappings = append(partitionMappings, WorkerPartitionMapping{
			OwnerID: row.OwnerID,
			Mapping: mapping,
		})
	}

	d := p.distributorStrategy.(*distributorStrategyV1Impl)

	// Step 1 - Build what is the current mapping of existing partition
	existingPartitionDistribution := d.buildExisting(partitionMappings, partitionCount)

	// Step 2 - make buckets (empty for now) with max no of partitions in each bucket
	resultMapping := d.buildBucket(activeWorkersToAssignPartitions, partitionCount)

	// Step 3 - distribute partitions
	d.assignPartitions(resultMapping, existingPartitionDistribution)

	resp := &DistributionResponse{
		DomainName: request.DomainName,
		TaskList:   request.TaskList,
		Mapping:    make(map[int]DistributionMapping),
	}

	for _, v := range resultMapping {
		for _, p := range v.Partitions {
			resp.Mapping[p.Partition] = DistributionMapping{
				OwnerId: p.OwnerId,
				Status:  p.Status,
			}
		}
	}

	// Step 1: Mark all existing records for this task list as inactive.
	// This handles workers that no longer own any partitions.
	if err = coordinatorTx.MarkPartitionInactive(ctx, helixCoordinatorMysql.MarkPartitionInactiveParams{Domain: domainName, Tasklist: tasklist}); err != nil {
		return errors.Wrap(err, "failed to mark old partitions as inactive: domainName=%s, tasklist=%s", domainName, tasklist)
	}

	newState := make(map[string][]int)
	for partitionId, mapping := range resp.Mapping {
		if mapping.OwnerId != "" { // Only consider partitions with an owner
			newState[mapping.OwnerId] = append(newState[mapping.OwnerId], partitionId)
		}
	}

	// Step 2: Upsert the new state for each worker.
	for ownerId, partitionIds := range newState {
		// Marshal the list of partitions into a JSON string for the metadata column.
		var metadataJson []byte
		metadataJson, err = json.Marshal(partitionIds)
		if err != nil {
			return errors.Wrap(err, "failed to marshal partition list for owner: domainName=%s, tasklist=%s, ownerId=%s", domainName, tasklist, ownerId)
		}

		// Use the UpsertPartition query to insert/update the record.
		params := helixCoordinatorMysql.UpsertPartitionParams{
			Domain:   domainName,
			Tasklist: tasklist,
			OwnerID:  ownerId,
			Metadata: sql.NullString{String: string(metadataJson), Valid: true},
			Status:   1, // Status 1 = Assigned
		}
		if err = coordinatorTx.UpsertPartition(ctx, params); err != nil {
			return errors.Wrap(err, "failed to upsert partition for owner: domainName=%s, tasklist=%s, ownerId=%s", domainName, tasklist, ownerId)
		}
		fmt.Println("---->>>", "domain", request.DomainName, "tasklist", tasklist, "partitionIds", partitionIds, "workerId", ownerId)
	}

	return nil
}
