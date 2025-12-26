package coordinator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"sort" // Added import for sort package
	"testing"

	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
)

type CoordinatorDataLayerTestSuite struct {
	suite.Suite
	dataLayer *DataLayer
	db        *sql.DB
}

func (s *CoordinatorDataLayerTestSuite) SetupSuite() {
	if os.Getenv("INTEGRATION_TESTS") != "true" {
		s.T().Skip("Skipping integration tests: Set INTEGRATION_TESTS=true to run")
	}

	// Setup test env and sql connection
	helix.SetupTestEnv()
	app := fx.New(
		fx.Provide(gox.NewCrossFunction),
		fx.Provide(func() (*sql.DB, error) {
			return sql.Open("mysql", helix.GetDefaultSqlUrl())
		}),
		fx.Provide(databaseCommon.NewConnectionHolder),
		fx.Provide(NewCoordinatorDataLayer),
		fx.Populate(&s.dataLayer, &s.db),
	)

	err := app.Start(context.Background())
	s.Require().NoError(err, "fx app failed to start")
}

func (s *CoordinatorDataLayerTestSuite) SetupTest() {
	// Clean the table before each test
	_, err := s.db.Exec("DELETE FROM helix_worker_partition_mapping WHERE domain LIKE 'dev-automation-%'")
	s.Require().NoError(err)
}

func (s *CoordinatorDataLayerTestSuite) TestGetActivePartitionMappings() {
	domain := "dev-automation-test-domain"
	tasklist := "dev-automation-test-tasklist"
	ctx := context.Background()

	// Arrange: Insert test data using explicit status values for clarity
	// Status 1 = Assigned, Status 0 = Inactive. The query looks for status 1 or 2.
	s.seedPartition(ctx, domain, tasklist, "worker-A", []int{0, 1, 5}, 1) // Assigned
	s.seedPartition(ctx, domain, tasklist, "worker-B", []int{2, 3}, 1)    // Assigned
	s.seedPartition(ctx, domain, tasklist, "worker-C", []int{8, 9}, 0)    // Inactive - Should be ignored
	s.seedPartition(ctx, domain, tasklist, "worker-D", []int{10}, 1)   // Assigned
	// Seed a record with corrupted metadata
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO helix_worker_partition_mapping(domain, tasklist, owner_id, metadata, status) VALUES (?, ?, ?, ?, ?)`,
		domain, tasklist, "worker-E", "corrupted-json", 1, // Assigned
	)
	s.Require().NoError(err)

	// Act: Call the method under test
	mappings, err := s.dataLayer.GetActivePartitionMappings(ctx, domain, tasklist)
	s.NoError(err)
	s.NotNil(mappings)

	// Assert: Check the results
	s.Len(mappings, 3, "Expected mappings for 3 active workers (A, B, D)")

	// Convert to a map for easier lookup
	resultMap := make(map[string]WorkerPartitionMapping)
	for _, m := range mappings {
		resultMap[m.OwnerID] = m
	}

	// Verify Worker A
	workerAMapping, ok := resultMap["worker-A"]
	s.True(ok, "worker-A should be in the result")
	s.Len(workerAMapping.Mapping, 3, "worker-A should have 3 partitions")
	s.Contains(workerAMapping.Mapping, 0)
	s.Contains(workerAMapping.Mapping, 1)
	s.Contains(workerAMapping.Mapping, 5)
	s.Equal(databaseCommon.PartitionAssignmentStatusAssigned, workerAMapping.Mapping[0].Status)

	// Verify Worker B
	workerBMapping, ok := resultMap["worker-B"]
	s.True(ok, "worker-B should be in the result")
	s.Len(workerBMapping.Mapping, 2, "worker-B should have 2 partitions")
	s.Contains(workerBMapping.Mapping, 2)
	s.Contains(workerBMapping.Mapping, 3)

	// Verify Worker D
	workerDMapping, ok := resultMap["worker-D"]
	s.True(ok, "worker-D should be in the result")
	s.Len(workerDMapping.Mapping, 1, "worker-D should have 1 partition")
	s.Contains(workerDMapping.Mapping, 10)

	// Verify ignored workers are not present
	_, ok = resultMap["worker-C"]
	s.False(ok, "worker-C should not be in the result as it is inactive")
	_, ok = resultMap["worker-E"]
	s.False(ok, "worker-E should not be in the result as its metadata is corrupted")
}

func (s *CoordinatorDataLayerTestSuite) TestPersistDistribution() {
	domain := "dev-automation-test-domain-persist"
	tasklist := "dev-automation-test-tasklist-persist"
	ctx := context.Background()

	// 1. Arrange: Seed an initial state
	s.seedPartition(ctx, domain, tasklist, "worker-A", []int{0, 1, 2}, 1) // Assigned
	s.seedPartition(ctx, domain, tasklist, "worker-B", []int{3, 4}, 1)    // Assigned
	s.seedPartition(ctx, domain, tasklist, "worker-C", []int{5}, 0)    // Inactive

	// 2. Arrange: Define the new target state
	// Worker-A keeps 0, 1
	// Worker-B loses all partitions
	// Worker-D (a new worker) gets partitions 2, 3, 4
	response := &DistributionResponse{
		DomainName: domain,
		TaskList:   tasklist,
		Mapping: map[int]DistributionMapping{
			0: {OwnerId: "worker-A"},
			1: {OwnerId: "worker-A"},
			2: {OwnerId: "worker-D"},
			3: {OwnerId: "worker-D"},
			4: {OwnerId: "worker-D"},
		},
	}

	// 3. Act: Call the method under test
	err := s.dataLayer.PersistDistribution(ctx, domain, tasklist, response)
	s.Require().NoError(err)

	// 4. Assert: Read the data back and verify it matches the new state
	mappings, err := s.dataLayer.GetActivePartitionMappings(ctx, domain, tasklist)
	s.Require().NoError(err)
	s.Require().Len(mappings, 2, "Expected 2 active workers (A and D)")

	// Convert to a map for easier lookup and verification
	persistedState := make(map[string][]int)
	for _, m := range mappings {
		for pId := range m.Mapping {
			persistedState[m.OwnerID] = append(persistedState[m.OwnerID], pId)
		}
	}
	// Sort slices to ensure ElementsMatch works correctly
	for owner := range persistedState {
		sort.Ints(persistedState[owner])
	}
	
	// Verify assignments for Worker-A
	s.Contains(persistedState, "worker-A")
	s.ElementsMatch([]int{0, 1}, persistedState["worker-A"])

	// Verify assignments for Worker-D
	s.Contains(persistedState, "worker-D")
	s.ElementsMatch([]int{2, 3, 4}, persistedState["worker-D"])

	// Verify that Worker-B and Worker-C have no active partitions
	s.NotContains(persistedState, "worker-B")
	s.NotContains(persistedState, "worker-C")

	// Also check the DB directly to ensure worker-B's old record is now inactive (status 0)
	var status int8
	err = s.db.QueryRowContext(ctx, "SELECT status FROM helix_worker_partition_mapping WHERE domain = ? AND tasklist = ? AND owner_id = ?", domain, tasklist, "worker-B").Scan(&status)
	s.Require().NoError(err)
	s.Equal(int8(0), status, "Worker-B's record should have been marked as inactive (status 0)")
}

func (s *CoordinatorDataLayerTestSuite) TestGetActivePartitionMappings_NoRows() {
	// Act
	mappings, err := s.dataLayer.GetActivePartitionMappings(context.Background(), "dev-automation-domain-no-rows", "dev-automation-tasklist-no-rows")

	// Assert
	s.NoError(err)
	s.NotNil(mappings)
	s.Len(mappings, 0)
}

func (s *CoordinatorDataLayerTestSuite) seedPartition(ctx context.Context, domain, tasklist, ownerId string, partitions []int, status int8) {
	metadata, err := json.Marshal(partitions)
	s.Require().NoError(err)

	q := `INSERT INTO helix_worker_partition_mapping(domain, tasklist, owner_id, metadata, status) VALUES (?, ?, ?, ?, ?)
             ON DUPLICATE KEY UPDATE metadata=VALUES(metadata), status=VALUES(status), owner_id=VALUES(owner_id)`
	_, err = s.db.ExecContext(ctx, q, domain, tasklist, ownerId, string(metadata), status)
	s.Require().NoError(err, fmt.Sprintf("failed to seed partition for owner %s", ownerId))
}

func TestCoordinatorDataLayer(t *testing.T) {
	suite.Run(t, new(CoordinatorDataLayerTestSuite))
}
