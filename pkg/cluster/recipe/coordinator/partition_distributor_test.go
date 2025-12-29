package coordinator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/devlibx/gox-helix/pkg/common"
	"os"
	"testing"

	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
)

type PartitionDistributorTestSuite struct {
	suite.Suite
	dataLayer       *DataLayer
	db              *sql.DB
	mockDistributor *MockDistributorStrategy
	mockCtrl        *gomock.Controller
	service         *PartitionDistributionServiceImpl // Use the concrete implementation for testing
}

func (s *PartitionDistributorTestSuite) SetupSuite() {
	if os.Getenv("INTEGRATION_TESTS") != "true" {
		s.T().Skip("Skipping integration tests: Set INTEGRATION_TESTS=true to run")
	}

	s.mockCtrl = gomock.NewController(s.T())
	s.mockDistributor = NewMockDistributorStrategy(s.mockCtrl)

	// Setup test env and sql connection
	helix.SetupTestEnv()
	app := fx.New(
		fx.Provide(gox.NewCrossFunction),
		fx.Provide(func() (*sql.DB, error) {
			return sql.Open("mysql", helix.GetDefaultSqlUrl())
		}),
		fx.Provide(func() *common.ApplicationSingleton {
			return common.NewApplicationSingletonWithContext(context.Background())
		}),
		fx.Provide(databaseCommon.NewConnectionHolder),
		fx.Provide(NewCoordinatorDataLayer),
		fx.Provide(locker.NewLockerDataLayer), // Provide real locker dependencies
		fx.Provide(locker.NewLocker),
		fx.Provide(func() DistributorStrategy { return s.mockDistributor }), // Provide the mock distributor
		fx.Provide(func(dataLayer *DataLayer) PartitionService { return dataLayer }),
		// Provide the concrete struct, not the interface
		fx.Provide(func(lockService locker.Locker, distributor DistributorStrategy, partitionService PartitionService, as *common.ApplicationSingleton) (*PartitionDistributionServiceImpl, error) {
			p, err := NewPartitionDistributionService(lockService, distributor, partitionService, as)
			return p.(*PartitionDistributionServiceImpl), err
		}),
		fx.Populate(&s.dataLayer, &s.db, &s.service),
	)

	err := app.Start(context.Background())
	s.Require().NoError(err, "fx app failed to start")
}

func (s *PartitionDistributorTestSuite) SetupTest() {
	// Clean the table before each test
	_, err := s.db.Exec("DELETE FROM helix_worker_partition_mapping WHERE domain like 'dev-automation-%'")
	s.Require().NoError(err)
	_, err = s.db.Exec("DELETE FROM helix_domain WHERE domain like 'dev-automation-%'")
	s.Require().NoError(err)
}

func (s *PartitionDistributorTestSuite) TearDownSuite() {
	s.mockCtrl.Finish()
}

func (s *PartitionDistributorTestSuite) TestInternalProcess_ColdStart() {
	domainName := "dev-automation-cold-start-domain"
	tasklist := "dev-automation-cold-start-tasklist"
	ctx := context.Background()

	// 1. Arrange: Mock the distributor to return a new assignment
	coldStartResponse := &DistributionResponse{
		Mapping: map[int]DistributionMapping{
			0: {OwnerId: "worker-A"}, 1: {OwnerId: "worker-A"}, 2: {OwnerId: "worker-A"},
			3: {OwnerId: "worker-B"}, 4: {OwnerId: "worker-B"},
		},
	}
	s.mockDistributor.EXPECT().Distribute(gomock.Any(), gomock.Any()).Return(coldStartResponse, nil)

	// 2. Act: Run the internal process
	request := DistributionRequest{DomainName: domainName, TaskList: tasklist}
	err := s.service.internalProcess(ctx, request) // Call the unexported method
	s.Require().NoError(err)

	// 3. Assert: Verify the database state
	s.verifyDbState(ctx, domainName, tasklist, coldStartResponse, 5)
}

func (s *PartitionDistributorTestSuite) TestInternalProcess_Rebalance() {
	domainName := "dev-automation-rebalance-domain"
	tasklist := "dev-automation-rebalance-tasklist"
	ctx := context.Background()

	// 1. Arrange: Seed an initial state
	s.seedPartition(ctx, domainName, tasklist, "worker-A", []int{0, 1, 2, 3, 4}, 1) // 5 partitions

	// 2. Arrange: Mock the distributor to return a rebalanced assignment
	rebalanceResponse := &DistributionResponse{
		Mapping: map[int]DistributionMapping{
			0: {OwnerId: "worker-A"}, 1: {OwnerId: "worker-A"}, // worker-A loses 3 partitions
			2: {OwnerId: "worker-B"}, 3: {OwnerId: "worker-B"}, 4: {OwnerId: "worker-B"}, // worker-B gets 3
		},
	}
	s.mockDistributor.EXPECT().Distribute(gomock.Any(), gomock.Any()).Return(rebalanceResponse, nil)

	// 3. Act: Run the internal process
	request := DistributionRequest{DomainName: domainName, TaskList: tasklist}
	err := s.service.internalProcess(ctx, request) // Call the unexported method
	s.Require().NoError(err)

	// 4. Assert: Verify the new database state
	s.verifyDbState(ctx, domainName, tasklist, rebalanceResponse, 5)

	// 4.1 Assert: Check that worker-A's record was updated and not just re-inserted
	var count int
	err = s.db.QueryRow("SELECT count(*) FROM helix_worker_partition_mapping WHERE domain=? AND tasklist=? AND status=1", domainName, tasklist).Scan(&count)
	s.Require().NoError(err)
	s.Equal(2, count, "Should only have 2 active worker rows in the DB")
}

// verifyDbState is a helper to read data back and check for consistency
func (s *PartitionDistributorTestSuite) verifyDbState(ctx context.Context, domain, tasklist string, expected *DistributionResponse, partitionCount int) {
	// Read the data back using the real DataLayer
	mappings, err := s.dataLayer.GetActivePartitionMappings(ctx, domain, tasklist)
	s.Require().NoError(err)

	// Flatten the result into a map[partition]owner for easy comparison
	persistedState := make(map[int]string)
	for _, m := range mappings {
		for pId := range m.Mapping {
			// Check for duplicates
			_, exists := persistedState[pId]
			s.False(exists, "partition %d is assigned to more than one worker", pId)
			persistedState[pId] = m.OwnerID
		}
	}

	// Verify all partitions are assigned
	s.Len(persistedState, partitionCount, "all partitions should be assigned")
	for i := 0; i < partitionCount; i++ {
		_, exists := persistedState[i]
		s.True(exists, "partition %d should be assigned", i)
	}

	// Verify the assignment matches the expected response
	for pId, owner := range persistedState {
		s.Equal(expected.Mapping[pId].OwnerId, owner)
	}
}

func (s *PartitionDistributorTestSuite) seedPartition(ctx context.Context, domain, tasklist, ownerId string, partitions []int, status int8) {
	metadata, err := json.Marshal(partitions)
	s.Require().NoError(err)
	q := `INSERT INTO helix_worker_partition_mapping(domain, tasklist, owner_id, metadata, status) VALUES (?, ?, ?, ?, ?)`
	_, err = s.db.ExecContext(ctx, q, domain, tasklist, ownerId, string(metadata), status)
	s.Require().NoError(err, fmt.Sprintf("failed to seed partition for owner %s", ownerId))

	// Also ensure a domain exists for these tests
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO helix_domain (domain, tasklist, partition_count) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE partition_count=VALUES(partition_count)`,
		domain, tasklist, 10,
	)
	s.Require().NoError(err)
}

func TestPartitionDistributor(t *testing.T) {
	suite.Run(t, new(PartitionDistributorTestSuite))
}
