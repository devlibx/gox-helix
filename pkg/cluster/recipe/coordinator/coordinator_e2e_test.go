package coordinator

// End-to-end tests for coordinator performPartitionAllocation method
//
// These tests validate the complete flow of partition allocation coordination:
// - Cluster leadership election and validation
// - Database queries for domains and tasks
// - Integration with AllocationManager for actual partition calculation
// - Error handling and edge cases
// - Time-based testing with MockCrossFunction
//
// Test scenarios:
// 1. Not coordinator - should return error when not cluster leader
// 2. No domains/tasks - should succeed with empty result set
// 3. Single domain/task - should call AllocationManager once
// 4. Multiple domains/tasks - should call AllocationManager for each combination
// 5. AllocationManager failure - should propagate errors correctly
// 6. Database query failure - should handle database connection issues
// 7. Coordinator status change - should handle leadership transitions
// 8. Time advancement - should handle time-based logic correctly
//
// Uses fx dependency injection framework with real database connections
// and mock implementations for cluster management and allocation services.

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix/pkg/common/lock"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	helixLock "github.com/devlibx/gox-helix/pkg/common/lock/mysql"
	"github.com/devlibx/gox-helix/pkg/util"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"os"

	_ "github.com/go-sql-driver/mysql"
)

// CoordinatorE2ETestSuite provides end-to-end testing for coordinator performPartitionAllocation
type CoordinatorE2ETestSuite struct {
	suite.Suite
	helixLockMySqlConfig *helixLock.MySqlConfig
}

// SetupSuite initializes the test environment with database configuration
func (s *CoordinatorE2ETestSuite) SetupSuite() {
	// Load environment variables from .env file
	err := util.LoadDevEnv()
	s.Require().NoError(err, "Failed to load dev environment")

	// Create MySQL configuration from environment variables
	db := os.Getenv("MYSQL_DB")
	dbHost := os.Getenv("MYSQL_HOST")
	dbUser := os.Getenv("MYSQL_USER")
	dbPassword := os.Getenv("MYSQL_PASSWORD")
	s.helixLockMySqlConfig = &helixLock.MySqlConfig{
		Database: db,
		Host:     dbHost,
		User:     dbUser,
		Password: dbPassword,
	}
}

// CoordinatorTestSetup holds all dependencies needed for E2E testing
type CoordinatorTestSetup struct {
	app        *fx.App
	mockCf     *util.MockCrossFunction
	coordinator *coordinatorImpl
	db         *helixClusterMysql.Queries
	sqlDb      *sql.DB

	// Test identifiers for isolation
	clusterName string
}

// makeCoordinatorApp creates a new test setup with real database dependencies
func (s *CoordinatorE2ETestSuite) makeCoordinatorApp() *CoordinatorTestSetup {
	ts := &CoordinatorTestSetup{
		mockCf:      util.NewMockCrossFunction(time.Now()),
		clusterName: "coordinator-test-" + uuid.NewString(),
	}

	ts.app = fx.New(
		fx.Supply(s.helixLockMySqlConfig),
		fx.Provide(func() gox.CrossFunction {
			return ts.mockCf
		}),
		fx.Provide(func(config *helixLock.MySqlConfig) (*sql.DB, error) {
			config.SetupDefault()
			url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
				config.User, config.Password, config.Host, config.Port, config.Database)
			return sql.Open("mysql", url)
		}),
		fx.Provide(func(sqlDb *sql.DB) *helixClusterMysql.Queries {
			return helixClusterMysql.New(sqlDb)
		}),
		// Mock cluster manager that returns coordinator status
		fx.Provide(func(cf gox.CrossFunction) managment.ClusterManager {
			return &MockClusterManager{clusterName: ts.clusterName}
		}),
		// Mock allocation manager
		fx.Provide(func(cf gox.CrossFunction) managment.AllocationManager {
			return &MockAllocationManager{}
		}),
		// Mock locker service
		fx.Provide(func(cf gox.CrossFunction) (lock.Locker, error) {
			return helixLock.NewHelixLockMySQLService(cf, s.helixLockMySqlConfig)
		}),
		fx.Provide(func(
			cf gox.CrossFunction,
			clusterManager managment.ClusterManager,
			allocationManager managment.AllocationManager,
			db *helixClusterMysql.Queries,
			locker lock.Locker,
		) *coordinatorImpl {
			return &coordinatorImpl{
				CrossFunction:      cf,
				clusterManager:     clusterManager,
				allocationManager:  allocationManager,
				clusterDbInterface: db,
				locker:             locker,
			}
		}),
		fx.Populate(
			&ts.db,
			&ts.sqlDb,
			&ts.coordinator,
		),
		fx.NopLogger, // Disable logging during tests
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s.Require().NoError(ts.app.Start(ctx), "Failed to start fx app")

	return ts
}

// TearDownTest cleans up after each test
func (s *CoordinatorE2ETestSuite) TearDownTest() {
	// We'll clean up in individual tests since we need access to the setup
}

// Database helper utilities for coordinator testing
func (s *CoordinatorE2ETestSuite) cleanupClusterData(db *sql.DB, clusterName string) {
	// Clean up in reverse dependency order - ignore errors if tables don't exist
	db.Exec("DELETE FROM helix_allocations WHERE cluster = ?", clusterName)
	db.Exec("DELETE FROM helix_cluster WHERE cluster = ?", clusterName)
	db.Exec("DELETE FROM helix_nodes WHERE cluster_name = ?", clusterName)
}

func (s *CoordinatorE2ETestSuite) insertTestClusterData(db *sql.DB, clusterName, domain, tasklist string, partitionCount int) {
	_, err := db.Exec(`
		INSERT INTO helix_cluster (cluster, domain, tasklist, metadata, partition_count, status, created_at, updated_at)
		VALUES (?, ?, ?, '{}', ?, 1, NOW(), NOW())
	`, clusterName, domain, tasklist, partitionCount)
	if err != nil {
		s.T().Skipf("Skipping test due to missing database schema: %v", err)
	}
}

func (s *CoordinatorE2ETestSuite) insertTestNode(db *sql.DB, clusterName, nodeId string) {
	_, err := db.Exec(`
		INSERT INTO helix_nodes (cluster_name, node_uuid, node_metadata, last_hb_time, status, version, created_at, updated_at)
		VALUES (?, ?, '{}', NOW(), 1, 1, NOW(), NOW())
	`, clusterName, nodeId)
	if err != nil {
		s.T().Skipf("Skipping test due to missing database schema: %v", err)
	}
}

func (s *CoordinatorE2ETestSuite) countAllocations(db *sql.DB, clusterName string) int {
	row := db.QueryRow("SELECT COUNT(*) FROM helix_allocations WHERE cluster = ?", clusterName)
	var count int
	err := row.Scan(&count)
	if err != nil {
		return 0 // Table might not exist
	}
	return count
}

// Mock implementations for testing
type MockClusterManager struct {
	clusterName string
	isCoordinator bool
}

func (m *MockClusterManager) BecomeClusterCoordinator(ctx context.Context) *lock.AcquireResponse {
	return &lock.AcquireResponse{
		Acquired: m.isCoordinator,
	}
}

func (m *MockClusterManager) GetClusterName() string {
	return m.clusterName
}

func (m *MockClusterManager) RegisterNode(ctx context.Context, request managment.NodeRegisterRequest) (*managment.NodeRegisterResponse, error) {
	return &managment.NodeRegisterResponse{
		NodeId: "mock-node-" + uuid.NewString(),
	}, nil
}

func (m *MockClusterManager) GetActiveNodes(ctx context.Context) ([]managment.Node, error) {
	return []managment.Node{
		{
			CrossFunction: gox.NewNoOpCrossFunction(),
			Cluster:       m.clusterName,
			Id:            "mock-node-1",
			Status:        1,
		},
	}, nil
}

func (m *MockClusterManager) GetClusterManagerConfig() managment.ClusterManagerConfig {
	return managment.ClusterManagerConfig{
		Name:                  m.clusterName,
		NodeHeartbeatInterval: time.Second,
		ControllerTtl:         30 * time.Second,
	}
}

type MockAllocationManager struct {
	shouldFail bool
	callCount  int
}

func (m *MockAllocationManager) CalculateAllocation(ctx context.Context, taskListInfo managment.TaskListInfo) (*managment.AllocationResponse, error) {
	m.callCount++
	if m.shouldFail {
		return nil, fmt.Errorf("mock allocation manager failure")
	}
	return &managment.AllocationResponse{}, nil
}

// Test Cases

func (s *CoordinatorE2ETestSuite) TestPerformPartitionAllocation_NotCoordinator() {
	setup := s.makeCoordinatorApp()
	defer func() {
		s.cleanupClusterData(setup.sqlDb, setup.clusterName)
		setup.app.Stop(context.Background())
	}()

	ctx := context.Background()

	// Set coordinator to NOT be leader
	mockClusterManager := setup.coordinator.clusterManager.(*MockClusterManager)
	mockClusterManager.isCoordinator = false

	// Call performPartitionAllocation
	err := setup.coordinator.performPartitionAllocation(ctx)

	// Should return error since not coordinator
	s.Assert().Error(err)
	s.Assert().Contains(err.Error(), "not leader so avoid partition allocation")

	// Should not have called allocation manager
	mockAllocationManager := setup.coordinator.allocationManager.(*MockAllocationManager)
	s.Assert().Equal(0, mockAllocationManager.callCount, "AllocationManager should not be called when not coordinator")
}

func (s *CoordinatorE2ETestSuite) TestPerformPartitionAllocation_NoDomainsAndTasks() {
	setup := s.makeCoordinatorApp()
	defer func() {
		s.cleanupClusterData(setup.sqlDb, setup.clusterName)
		setup.app.Stop(context.Background())
	}()

	ctx := context.Background()

	// Set coordinator to be leader
	mockClusterManager := setup.coordinator.clusterManager.(*MockClusterManager)
	mockClusterManager.isCoordinator = true

	// No cluster data inserted, so GetAllDomainsAndTaskListsByClusterCname should return empty

	// Call performPartitionAllocation
	err := setup.coordinator.performPartitionAllocation(ctx)

	// Should succeed with no domains/tasks to process
	s.Assert().NoError(err)

	// Should not have called allocation manager since no domains/tasks
	mockAllocationManager := setup.coordinator.allocationManager.(*MockAllocationManager)
	s.Assert().Equal(0, mockAllocationManager.callCount, "AllocationManager should not be called with no domains/tasks")
}

func (s *CoordinatorE2ETestSuite) TestPerformPartitionAllocation_SingleDomainTask_Success() {
	setup := s.makeCoordinatorApp()
	defer func() {
		s.cleanupClusterData(setup.sqlDb, setup.clusterName)
		setup.app.Stop(context.Background())
	}()

	ctx := context.Background()
	domain := "test-domain-" + uuid.NewString()
	taskList := "test-tasklist-" + uuid.NewString()
	partitionCount := 10

	// Set coordinator to be leader
	mockClusterManager := setup.coordinator.clusterManager.(*MockClusterManager)
	mockClusterManager.isCoordinator = true

	// Insert test cluster data
	s.insertTestClusterData(setup.sqlDb, setup.clusterName, domain, taskList, partitionCount)
	
	// Insert test node
	s.insertTestNode(setup.sqlDb, setup.clusterName, "test-node-1")

	// Call performPartitionAllocation
	err := setup.coordinator.performPartitionAllocation(ctx)

	// Should succeed
	s.Assert().NoError(err)

	// Should have called allocation manager once
	mockAllocationManager := setup.coordinator.allocationManager.(*MockAllocationManager)
	s.Assert().Equal(1, mockAllocationManager.callCount, "AllocationManager should be called once")
}

func (s *CoordinatorE2ETestSuite) TestPerformPartitionAllocation_MultipleDomainTasks_Success() {
	setup := s.makeCoordinatorApp()
	defer func() {
		s.cleanupClusterData(setup.sqlDb, setup.clusterName)
		setup.app.Stop(context.Background())
	}()

	ctx := context.Background()

	// Set coordinator to be leader
	mockClusterManager := setup.coordinator.clusterManager.(*MockClusterManager)
	mockClusterManager.isCoordinator = true

	// Insert multiple test cluster data entries
	s.insertTestClusterData(setup.sqlDb, setup.clusterName, "domain1", "tasklist1", 10)
	s.insertTestClusterData(setup.sqlDb, setup.clusterName, "domain1", "tasklist2", 15)
	s.insertTestClusterData(setup.sqlDb, setup.clusterName, "domain2", "tasklist1", 8)
	
	// Insert test nodes
	s.insertTestNode(setup.sqlDb, setup.clusterName, "test-node-1")
	s.insertTestNode(setup.sqlDb, setup.clusterName, "test-node-2")

	// Call performPartitionAllocation
	err := setup.coordinator.performPartitionAllocation(ctx)

	// Should succeed
	s.Assert().NoError(err)

	// Should have called allocation manager for each domain/task combination (3 times)
	mockAllocationManager := setup.coordinator.allocationManager.(*MockAllocationManager)
	s.Assert().Equal(3, mockAllocationManager.callCount, "AllocationManager should be called for each domain/task combination")
}

func (s *CoordinatorE2ETestSuite) TestPerformPartitionAllocation_AllocationManagerFailure() {
	setup := s.makeCoordinatorApp()
	defer func() {
		s.cleanupClusterData(setup.sqlDb, setup.clusterName)
		setup.app.Stop(context.Background())
	}()

	ctx := context.Background()
	domain := "test-domain-" + uuid.NewString()
	taskList := "test-tasklist-" + uuid.NewString()
	partitionCount := 10

	// Set coordinator to be leader
	mockClusterManager := setup.coordinator.clusterManager.(*MockClusterManager)
	mockClusterManager.isCoordinator = true

	// Set allocation manager to fail
	mockAllocationManager := setup.coordinator.allocationManager.(*MockAllocationManager)
	mockAllocationManager.shouldFail = true

	// Insert test cluster data
	s.insertTestClusterData(setup.sqlDb, setup.clusterName, domain, taskList, partitionCount)
	
	// Insert test node
	s.insertTestNode(setup.sqlDb, setup.clusterName, "test-node-1")

	// Call performPartitionAllocation
	err := setup.coordinator.performPartitionAllocation(ctx)

	// Should return error from allocation manager
	s.Assert().Error(err)
	s.Assert().Contains(err.Error(), "fail to calculate allocation for domain test-domain")
	s.Assert().Contains(err.Error(), "mock allocation manager failure")

	// Should have attempted to call allocation manager once
	s.Assert().Equal(1, mockAllocationManager.callCount, "AllocationManager should be called once before failing")
}

func (s *CoordinatorE2ETestSuite) TestPerformPartitionAllocation_DatabaseQueryFailure() {
	setup := s.makeCoordinatorApp()
	defer func() {
		s.cleanupClusterData(setup.sqlDb, setup.clusterName)
		setup.app.Stop(context.Background())
	}()

	ctx := context.Background()

	// Set coordinator to be leader
	mockClusterManager := setup.coordinator.clusterManager.(*MockClusterManager)
	mockClusterManager.isCoordinator = true

	// Close database connection to simulate database failure
	setup.sqlDb.Close()

	// Call performPartitionAllocation
	err := setup.coordinator.performPartitionAllocation(ctx)

	// Should return error from database query
	s.Assert().Error(err)
	s.Assert().Contains(err.Error(), "fail to get all domains and tasks for cluster")
}

func (s *CoordinatorE2ETestSuite) TestPerformPartitionAllocation_CoordinatorStatusChange() {
	setup := s.makeCoordinatorApp()
	defer func() {
		s.cleanupClusterData(setup.sqlDb, setup.clusterName)
		setup.app.Stop(context.Background())
	}()

	ctx := context.Background()
	domain := "test-domain-" + uuid.NewString()
	taskList := "test-tasklist-" + uuid.NewString()
	partitionCount := 10

	// Insert test cluster data
	s.insertTestClusterData(setup.sqlDb, setup.clusterName, domain, taskList, partitionCount)
	s.insertTestNode(setup.sqlDb, setup.clusterName, "test-node-1")

	mockClusterManager := setup.coordinator.clusterManager.(*MockClusterManager)
	mockAllocationManager := setup.coordinator.allocationManager.(*MockAllocationManager)

	// First call - not coordinator
	mockClusterManager.isCoordinator = false
	err1 := setup.coordinator.performPartitionAllocation(ctx)
	s.Assert().Error(err1)
	s.Assert().Equal(0, mockAllocationManager.callCount, "Should not call allocation manager when not coordinator")

	// Second call - become coordinator
	mockClusterManager.isCoordinator = true
	err2 := setup.coordinator.performPartitionAllocation(ctx)
	s.Assert().NoError(err2)
	s.Assert().Equal(1, mockAllocationManager.callCount, "Should call allocation manager once when coordinator")

	// Third call - lose coordinator status
	mockClusterManager.isCoordinator = false
	err3 := setup.coordinator.performPartitionAllocation(ctx)
	s.Assert().Error(err3)
	s.Assert().Equal(1, mockAllocationManager.callCount, "Call count should remain same after losing coordinator status")
}

func (s *CoordinatorE2ETestSuite) TestPerformPartitionAllocation_TimeAdvancement() {
	setup := s.makeCoordinatorApp()
	defer func() {
		s.cleanupClusterData(setup.sqlDb, setup.clusterName)
		setup.app.Stop(context.Background())
	}()

	ctx := context.Background()
	domain := "test-domain-" + uuid.NewString()
	taskList := "test-tasklist-" + uuid.NewString()
	partitionCount := 10

	// Set coordinator to be leader
	mockClusterManager := setup.coordinator.clusterManager.(*MockClusterManager)
	mockClusterManager.isCoordinator = true

	// Insert test data
	s.insertTestClusterData(setup.sqlDb, setup.clusterName, domain, taskList, partitionCount)
	s.insertTestNode(setup.sqlDb, setup.clusterName, "test-node-1")

	// Set initial time
	initialTime := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	setup.mockCf.SetTime(initialTime)

	// First allocation
	err := setup.coordinator.performPartitionAllocation(ctx)
	s.Assert().NoError(err)

	// Advance time and run again (simulating periodic execution)
	setup.mockCf.AdvanceTime(30 * time.Second)
	err = setup.coordinator.performPartitionAllocation(ctx)
	s.Assert().NoError(err)

	// Verify allocation manager was called twice
	mockAllocationManager := setup.coordinator.allocationManager.(*MockAllocationManager)
	s.Assert().Equal(2, mockAllocationManager.callCount, "Should handle time advancement correctly")
}

func TestCoordinatorE2ETestSuite(t *testing.T) {
	suite.Run(t, new(CoordinatorE2ETestSuite))
}