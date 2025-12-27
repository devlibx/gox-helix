package coordinator

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/devlibx/gox-base/v2/errors"
	"log/slog"
	"time"

	"github.com/devlibx/gox-base/v2"
	helixCoordinatorMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/database"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
)

// DataLayer provides the database access layer for coordinator functions.
// It embeds the sqlc-generated concrete Queries struct and implements the PartitionService interface.
type DataLayer struct {
	gox.CrossFunction
	*helixCoordinatorMysql.Queries // Embed the concrete Queries struct
	db                             *sql.DB
}

// NewCoordinatorDataLayer creates a new DataLayer for the coordinator.
func NewCoordinatorDataLayer(cf gox.CrossFunction, ch databaseCommon.ConnectionHolder) (*DataLayer, error) {
	db := ch.GetHelixMasterDbConnection()
	if db == nil {
		return nil, errors.New("database connection is nil for coordinator data layer")
	}
	q, err := helixCoordinatorMysql.Prepare(context.Background(), db)
	if err != nil {
		return nil, errors.Wrap(err, "failed to prepare coordinator database queries")
	}
	return &DataLayer{
		CrossFunction: cf,
		Queries:       q,
		db:            db,
	}, nil
}

// GetActivePartitionMappings implements the PartitionService interface.
func (d *DataLayer) GetActivePartitionMappings(ctx context.Context, domain string, tasklist string) ([]WorkerPartitionMapping, error) {
	params := helixCoordinatorMysql.GetAllValidPartitionForDomainAndTaskListParams{
		Domain:   domain,
		Tasklist: tasklist,
	}
	dbMappings, err := d.GetAllValidPartitionForDomainAndTaskList(ctx, params)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []WorkerPartitionMapping{}, nil
		}
		return nil, errors.Wrap(err, "failed to get active partition mappings from db: domain=%s, tasklist=%s", domain, tasklist)
	}

	partitionMappings := make([]WorkerPartitionMapping, 0, len(dbMappings))
	for _, row := range dbMappings {
		if !row.Metadata.Valid || row.Metadata.String == "" {
			slog.Warn("worker has partition mapping row with no metadata", "owner_id", row.OwnerID, "domain", domain, "tasklist", tasklist)
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
	return partitionMappings, nil
}

func (d *DataLayer) PersistDistribution(ctx context.Context, domain string, tasklist string, response *DistributionResponse) error {
	var err error
	for i := 0; i < 3; i++ {
		err = d.internalPersistDistribution(ctx, domain, tasklist, response)
		if err != nil {
			slog.Warn("got error while persisting distribution (retry)", "err", err.Error(), "domain", domain, "tasklist", tasklist, "retry", i)
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	return err
}

// PersistDistribution saves the new partition distribution to the database.
func (d *DataLayer) internalPersistDistribution(ctx context.Context, domain string, tasklist string, response *DistributionResponse) error {
	newState := make(map[string][]int)
	for partitionId, mapping := range response.Mapping {
		if mapping.OwnerId != "" { // Only consider partitions with an owner
			newState[mapping.OwnerId] = append(newState[mapping.OwnerId], partitionId)
		}
	}
	tx, err := d.db.BeginTx(ctx, nil)
	if err != nil {
		return errors.Wrap(err, "failed to begin transaction for persisting distribution: domain=%s, tasklist=%s", domain, tasklist)
	}
	defer func() {
		if err != nil {
			if e := tx.Rollback(); e != nil {
				slog.Error("error in rolling back in partition distribution write to DB", "domain", domain, "tasklist", tasklist, "error", e)
			}
		} else {
			err = tx.Commit()
		}
	}()

	qtx := d.WithTx(tx) // Get a transactional querier

	// Step 1: Mark all existing records for this task list as inactive.
	// This handles workers that no longer own any partitions.
	if err = qtx.MarkPartitionInactive(ctx, helixCoordinatorMysql.MarkPartitionInactiveParams{Domain: domain, Tasklist: tasklist}); err != nil {
		return errors.Wrap(err, "failed to mark old partitions as inactive: domain=%s, tasklist=%s", domain, tasklist)
	}

	// Step 2: Upsert the new state for each worker.
	for ownerId, partitionIds := range newState {
		// Marshal the list of partitions into a JSON string for the metadata column.
		var metadataJson []byte
		metadataJson, err = json.Marshal(partitionIds)
		if err != nil {
			return errors.Wrap(err, "failed to marshal partition list for owner: domain=%s, tasklist=%s, ownerId=%s", domain, tasklist, ownerId)
		}

		// Use the UpsertPartition query to insert/update the record.
		params := helixCoordinatorMysql.UpsertPartitionParams{
			Domain:   domain,
			Tasklist: tasklist,
			OwnerID:  ownerId,
			Metadata: sql.NullString{String: string(metadataJson), Valid: true},
			Status:   1, // Status 1 = Assigned
		}
		if err = qtx.UpsertPartition(ctx, params); err != nil {
			return errors.Wrap(err, "failed to upsert partition for owner: domain=%s, tasklist=%s, ownerId=%s", domain, tasklist, ownerId)
		}
	}

	return err
}

func (d *DataLayer) GetValidPartitionByOwnerId(ctx context.Context, domain string, tasklist string) ([]WorkerPartitionMapping, error) {

	result, err := d.Queries.GetValidPartitionByOwnerId(ctx, helixCoordinatorMysql.GetValidPartitionByOwnerIdParams{
		Domain:   domain,
		Tasklist: tasklist,
	})
	if err != nil {
		return nil, err
	}

	m := make([]WorkerPartitionMapping, 0)
	for _, r := range result {
		var partitions []int
		if err := json.Unmarshal([]byte(r.Metadata.String), &partitions); err == nil {
			t := map[int]DistributionMapping{}
			for _, partition := range partitions {
				t[partition] = DistributionMapping{
					OwnerId: r.OwnerID,
					Status:  databaseCommon.PartitionAssignmentStatus(r.Status),
				}
			}
			m = append(m, WorkerPartitionMapping{
				OwnerID: r.OwnerID,
				Mapping: t,
			})
		}
	}
	return m, nil
}

func (d *DataLayer) GetValidPartitionByOwnerIdV1(ctx context.Context, domain string, tasklist string, ownerId string) (*WorkerPartitionMapping, error) {
	result, err := d.Queries.GetValidPartitionByOwnerIdV1(ctx, helixCoordinatorMysql.GetValidPartitionByOwnerIdV1Params{
		Domain:   domain,
		Tasklist: tasklist,
		OwnerID:  ownerId,
	})
	if err != nil {
		return nil, err
	}

	out := &WorkerPartitionMapping{
		OwnerID: ownerId,
		Mapping: make(map[int]DistributionMapping),
	}
	for _, r := range result {
		var partitions []int
		if err := json.Unmarshal([]byte(r.Metadata.String), &partitions); err == nil {
			for _, partition := range partitions {
				out.Mapping[partition] = DistributionMapping{
					OwnerId: ownerId,
					Status:  databaseCommon.PartitionAssignmentStatus(r.Status),
				}
			}
		}
	}
	return out, nil
}
