package coordinator

import (
	"context"
	"database/sql"
	"encoding/json"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	helixCoordinatorMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/database"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	"log/slog" // Use slog for logging
)

// DataLayer provides the database access layer for coordinator functions.
// It embeds the sqlc-generated Querier and implements the PartitionService interface.
type DataLayer struct {
	gox.CrossFunction
	helixCoordinatorMysql.Querier
}

// NewCoordinatorDataLayer creates a new DataLayer for the coordinator.
func NewCoordinatorDataLayer(cf gox.CrossFunction, ch databaseCommon.ConnectionHolder) (*DataLayer, error) {
	// Ensure we have a valid database connection
	db := ch.GetHelixMasterDbConnection()
	if db == nil {
		return nil, errors.New("database connection is nil for coordinator data layer")
	}

	// Prepare the queries from the sqlc-generated code
	q, err := helixCoordinatorMysql.Prepare(context.Background(), db)
	if err != nil {
		return nil, errors.Wrap(err, "failed to prepare coordinator database queries")
	}

	return &DataLayer{
			CrossFunction: cf,
			Querier:       q,
		},
		nil
}

// GetActivePartitionMappings implements the PartitionService interface.
// It fetches the current partition assignments from the database and transforms them
// into the format required by the distribution algorithm.
func (d *DataLayer) GetActivePartitionMappings(ctx context.Context, domain string, tasklist string) ([]WorkerPartitionMapping, error) {
	// Use the existing sqlc query to get all rows for the given domain and tasklist.
	// Each row represents a worker and its set of assigned partitions.
	params := helixCoordinatorMysql.GetAllValidPartitionForDomainAndTaskListParams{
		Domain:   domain,
		Tasklist: tasklist,
	}
	dbMappings, err := d.GetAllValidPartitionForDomainAndTaskList(ctx, params)
	if err != nil {
		if err == sql.ErrNoRows {
			return []WorkerPartitionMapping{}, nil // No active partitions, return empty slice
		}
		return nil, errors.Wrap(err, "failed to get active partition mappings from db")
	}

	// The final slice to be returned
	partitionMappings := make([]WorkerPartitionMapping, 0, len(dbMappings))

	// Process each row (one per worker)
	for _, row := range dbMappings {
		// The list of partition IDs is stored in the metadata column as a JSON array.
		if !row.Metadata.Valid || row.Metadata.String == "" {
			slog.Warn("worker has partition mapping row with no metadata", "owner_id", row.OwnerID)
			continue
		}

		var partitionIds []int
		if err := json.Unmarshal([]byte(row.Metadata.String), &partitionIds); err != nil {
			slog.Error("failed to unmarshal partition metadata for worker", "owner_id", row.OwnerID, "metadata", row.Metadata.String, "err", err)
			continue // Skip corrupted metadata
		}

		// Build the inner map[int]DistributionMapping for this worker
		mapping := make(map[int]DistributionMapping, len(partitionIds))
		for _, pId := range partitionIds {

			// Note: The database uses 1 for Assigned and 2 for Unassigned, 
			// whereas the enum uses 0 and 1. We must map them correctly.
			var assignmentStatus databaseCommon.PartitionAssignmentStatus
			if row.Status == 1 { // 1 = Assigned in DB
				assignmentStatus = databaseCommon.PartitionAssignmentStatusAssigned
			} else { // 2 = Unassigned in DB
				assignmentStatus = databaseCommon.PartitionAssignmentStatusUnassigned
			}

			mapping[pId] = DistributionMapping{
				OwnerId: row.OwnerID,
				Status:  assignmentStatus,
			}
		}

		// Add this worker's complete mapping to our result slice
		partitionMappings = append(partitionMappings, WorkerPartitionMapping{
			OwnerID: row.OwnerID,
			Mapping: mapping,
		})
	}

	return partitionMappings, nil
}
