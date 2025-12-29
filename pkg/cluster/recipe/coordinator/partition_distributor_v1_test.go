package coordinator

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/devlibx/gox-helix"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	"os"
	"testing"

	"github.com/devlibx/gox-base/v2"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/common"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"go.uber.org/mock/gomock"
)

type PartitionDistributorV1TestSuite struct {
	suite.Suite
	coordinatorDataLayer *DataLayer
	workerDataLayer      *worker.DataLayer
	domainDataLayer      *domain.DataLayer
	db                   *sql.DB
	mockDistributor      *MockDistributorStrategy
	mockCtrl             *gomock.Controller
	service              *PartitionDistributionServiceV1Impl
}

func (s *PartitionDistributorV1TestSuite) SetupSuite() {
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
			as := common.NewApplicationSingletonWithContext(context.Background())
			return as
		}),
		fx.Provide(databaseCommon.NewConnectionHolder),
		fx.Provide(NewCoordinatorDataLayer),
		fx.Provide(worker.NewWorkerDataLayer),
		fx.Provide(domain.NewDomainDataLayer),
		fx.Provide(locker.NewLockerDataLayer),
		fx.Provide(locker.NewLocker),
		fx.Provide(func() DistributorStrategy { return s.mockDistributor }),
		fx.Provide(func(dataLayer *DataLayer) PartitionService { return dataLayer }),
		fx.Provide(func(
			lockService locker.Locker,
			distributor DistributorStrategy,
			partitionService PartitionService,
			applicationSingleton *common.ApplicationSingleton,
			workerDataLayer *worker.DataLayer,
			coordinatorDataLayer *DataLayer,
			domainDataLayer *domain.DataLayer,
			ch databaseCommon.ConnectionHolder,
		) (*PartitionDistributionServiceV1Impl, error) {
			p, err := NewPartitionDistributionServiceV1(
				lockService,
				distributor,
				partitionService,
				applicationSingleton,
				workerDataLayer,
				coordinatorDataLayer,
				domainDataLayer,
				distributor, // Pass mock distributor again for the unused field
				ch,
			)
			return p.(*PartitionDistributionServiceV1Impl), err
		}),
		fx.Populate(
			&s.coordinatorDataLayer,
			&s.workerDataLayer,
			&s.domainDataLayer,
			&s.db,
			&s.service,
		),
	)

	err := app.Start(context.Background())
	s.Require().NoError(err, "fx app failed to start")
}

func (s *PartitionDistributorV1TestSuite) SetupTest() {
	tables := []string{"helix_worker_partition_mapping", "helix_domain", "helix_workers", "helix_locks"}
	for _, table := range tables {
		_, err := s.db.Exec(fmt.Sprintf("DELETE FROM %s", table))
		s.Require().NoError(err)
	}
}

func (s *PartitionDistributorV1TestSuite) TearDownSuite() {
	s.mockCtrl.Finish()
}

func (s *PartitionDistributorV1TestSuite) TestInternalProcess_ColdStart() {
	domainName := "dev-automation-cold-start-domain"
	tasklist := "dev-automation-cold-start-tasklist"
	ctx := context.Background()

	// 1. Arrange: Mock the distributor to return a new assignment
	coldStartResponse := &DistributionResponse{
		DomainName: domainName,
		TaskList:   tasklist,
		Mapping: map[int]DistributionMapping{
			0: {OwnerId: "worker-A", Status: databaseCommon.PartitionAssignmentStatusAssigned},
			1: {OwnerId: "worker-A", Status: databaseCommon.PartitionAssignmentStatusAssigned},
			2: {OwnerId: "worker-A", Status: databaseCommon.PartitionAssignmentStatusAssigned},
			3: {OwnerId: "worker-B", Status: databaseCommon.PartitionAssignmentStatusAssigned},
			4: {OwnerId: "worker-B", Status: databaseCommon.PartitionAssignmentStatusAssigned},
		},
	}
	s.mockDistributor.EXPECT().Distribute(gomock.Any(), gomock.Any()).Return(coldStartResponse, nil)

	// 2. Act: Run the internal process
	request := DistributionRequest{DomainName: domainName, TaskList: tasklist}
	err := s.service.internalProcess(ctx, request)
	s.Require().NoError(err)

	// 3. Assert: Verify the database state
	s.verifyDbState(ctx, domainName, tasklist, coldStartResponse, 5, 2)
}

func (s *PartitionDistributorV1TestSuite) TestInternalProcess_Rebalance() {
	domainName := "dev-automation-rebalance-domain"
	tasklist := "dev-automation-rebalance-tasklist"
	ctx := context.Background()

	// 1. Arrange: Seed an initial state with 3 workers
	s.seedPartition(ctx, domainName, tasklist, "worker-A", []int{0, 1}, 1)
	s.seedPartition(ctx, domainName, tasklist, "worker-B", []int{2, 3}, 1)
	s.seedPartition(ctx, domainName, tasklist, "worker-C", []int{4, 5}, 1) // worker-C will be removed

	// 2. Arrange: Mock the distributor for rebalancing (worker-C's partitions go to A and B)
	rebalanceResponse := &DistributionResponse{
		DomainName: domainName,
		TaskList:   tasklist,
		Mapping: map[int]DistributionMapping{
			0: {OwnerId: "worker-A", Status: databaseCommon.PartitionAssignmentStatusAssigned},
			1: {OwnerId: "worker-A", Status: databaseCommon.PartitionAssignmentStatusAssigned},
			2: {OwnerId: "worker-B", Status: databaseCommon.PartitionAssignmentStatusAssigned},
			3: {OwnerId: "worker-B", Status: databaseCommon.PartitionAssignmentStatusAssigned},
			4: {OwnerId: "worker-A", Status: databaseCommon.PartitionAssignmentStatusAssigned}, // Re-assigned
			5: {OwnerId: "worker-B", Status: databaseCommon.PartitionAssignmentStatusAssigned}, // Re-assigned
		},
	}
	s.mockDistributor.EXPECT().Distribute(gomock.Any(), gomock.Any()).Return(rebalanceResponse, nil)

	// 3. Act: Run the internal process
	request := DistributionRequest{DomainName: domainName, TaskList: tasklist}
	err := s.service.internalProcess(ctx, request)
	s.Require().NoError(err)

	// 4. Assert: Verify the new database state
	s.verifyDbState(ctx, domainName, tasklist, rebalanceResponse, 6, 2)

	// 5. Assert: Check that worker-C's old record is now inactive
	var status int8
	err = s.db.QueryRow("SELECT status FROM helix_worker_partition_mapping WHERE domain=? AND tasklist=? AND owner_id=?", domainName, tasklist, "worker-C").Scan(&status)
	s.Require().NoError(err)
	s.Equal(int8(0), status, "worker-C's record should be marked inactive")
}

// verifyDbState is a helper to read data back and check for consistency
func (s *PartitionDistributorV1TestSuite) verifyDbState(ctx context.Context, domain, tasklist string, expected *DistributionResponse, partitionCount, expectedWorkerCount int) {
	// Read the data back using the real DataLayer
	mappings, err := s.coordinatorDataLayer.GetActivePartitionMappings(ctx, domain, tasklist)
	s.Require().NoError(err)

	s.Len(mappings, expectedWorkerCount, "should have the correct number of active worker rows")

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

func (s *PartitionDistributorV1TestSuite) seedPartition(ctx context.Context, domain, tasklist, ownerId string, partitions []int, status int8) {
	metadata, err := json.Marshal(partitions)
	s.Require().NoError(err)
	q := `INSERT INTO helix_worker_partition_mapping(domain, tasklist, owner_id, metadata, status) VALUES (?, ?, ?, ?, ?)`
	_, err = s.db.ExecContext(ctx, q, domain, tasklist, ownerId, string(metadata), status)
	s.Require().NoError(err, fmt.Sprintf("failed to seed partition for owner %s", ownerId))
}

func TestPartitionDistributorV1(t *testing.T) {
	suite.Run(t, new(PartitionDistributorV1TestSuite))
}
