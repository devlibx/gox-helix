package allocation

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/devlibx/gox-base/v2"
	goxJsonUtils "github.com/devlibx/gox-base/v2/serialization/utils/json"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	helixLock "github.com/devlibx/gox-helix/pkg/common/lock/mysql"
	"github.com/devlibx/gox-helix/pkg/util"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"os"
)

// SimpleAlgorithmE2ETestSuite provides end-to-end testing for SimpleAllocationAlgorithm with real database
type SimpleAlgorithmE2ETestSuite struct {
	suite.Suite
	helixLockMySqlConfig *helixLock.MySqlConfig
}

type SimpleAlgorithmE2ETestSetup struct {
	mockCf      gox.CrossFunction
	app         *fx.App
	db          *helixClusterMysql.Queries
	sqlDb       *sql.DB
	clusterName string
	domain      string
	taskList    string
}

type SimpleAlgorithmE2ETestHelper struct {
	suite *SimpleAlgorithmE2ETestSuite
	setup *SimpleAlgorithmE2ETestSetup
	ctx   context.Context
}

// SetupSuite initializes the test environment with database configuration
func (s *SimpleAlgorithmE2ETestSuite) SetupSuite() {
	// Load environment variables from .env file
	err := util.LoadDevEnv()
	s.Require().NoError(err, "Failed to load dev environment")

	// Create MySQL configuration from environment variables
	db := os.Getenv("MYSQL_DB")
	if db == "" {
		db = "automation" // default to automation database
	}
	dbHost := os.Getenv("MYSQL_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}
	dbUser := os.Getenv("MYSQL_USER")
	if dbUser == "" {
		dbUser = "root"
	}
	dbPassword := os.Getenv("MYSQL_PASSWORD")
	if dbPassword == "" {
		dbPassword = "credroot"
	}
	
	s.helixLockMySqlConfig = &helixLock.MySqlConfig{
		Database: db,
		Host:     dbHost,
		User:     dbUser,
		Password: dbPassword,
	}
	s.helixLockMySqlConfig.SetupDefault()
}

func (s *SimpleAlgorithmE2ETestSuite) makeApp() *SimpleAlgorithmE2ETestSetup {
	ts := &SimpleAlgorithmE2ETestSetup{
		mockCf:      util.NewMockCrossFunction(time.Now()),
		clusterName: "test-simple-v2-" + uuid.NewString(),
		domain:      "test-domain-" + uuid.NewString(),
		taskList:    "test-tasklist-" + uuid.NewString(),
	}
	
	ts.app = fx.New(
		fx.Supply(s.helixLockMySqlConfig),
		fx.Provide(func() gox.CrossFunction {
			return ts.mockCf
		}),
		fx.Provide(func(config *helixLock.MySqlConfig) (*sql.DB, error) {
			connectionString := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true",
				config.User, config.Password, config.Host, config.Port, config.Database)
			return sql.Open("mysql", connectionString)
		}),
		fx.Provide(func(sqlDb *sql.DB) *helixClusterMysql.Queries {
			return helixClusterMysql.New(sqlDb)
		}),
		fx.Populate(&ts.db, &ts.sqlDb),
		fx.NopLogger,
	)
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := ts.app.Start(ctx); err != nil {
		panic(err)
	}
	
	return ts
}

func (s *SimpleAlgorithmE2ETestSuite) NewE2ETestHelper() *SimpleAlgorithmE2ETestHelper {
	return &SimpleAlgorithmE2ETestHelper{
		suite: s,
		setup: s.makeApp(),
		ctx:   context.Background(),
	}
}

// setupCluster creates a cluster in the database
func (h *SimpleAlgorithmE2ETestHelper) setupCluster(partitionCount int) error {
	return h.setup.db.UpsertCluster(h.ctx, helixClusterMysql.UpsertClusterParams{
		Cluster:        h.setup.clusterName,
		Domain:         h.setup.domain,
		Tasklist:       h.setup.taskList,
		PartitionCount: uint32(partitionCount),
		Metadata:       sql.NullString{Valid: true, String: "{}"},
	})
}

// setupNodes creates nodes in the database
func (h *SimpleAlgorithmE2ETestHelper) setupNodes(nodeIds []string) error {
	for _, nodeId := range nodeIds {
		err := h.setup.db.UpsertNode(h.ctx, helixClusterMysql.UpsertNodeParams{
			ClusterName:  h.setup.clusterName,
			NodeUuid:     nodeId,
			NodeMetadata: sql.NullString{Valid: true, String: "{}"},
			LastHbTime:   h.setup.mockCf.Now(),
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// createInitialAllocations creates initial allocation state in database
func (h *SimpleAlgorithmE2ETestHelper) createInitialAllocations(allocations map[string][]string) error {
	for nodeId, partitionIds := range allocations {
		if len(partitionIds) == 0 {
			continue
		}
		
		allocation := &managment.Allocation{
			Cluster:                  h.setup.clusterName,
			Domain:                   h.setup.domain,
			TaskList:                 h.setup.taskList,
			NodeId:                   nodeId,
			PartitionAllocationInfos: make([]managment.PartitionAllocationInfo, 0),
		}
		
		for _, partitionId := range partitionIds {
			allocation.PartitionAllocationInfos = append(allocation.PartitionAllocationInfos, managment.PartitionAllocationInfo{
				PartitionId:      partitionId,
				AllocationStatus: managment.PartitionAllocationAssigned,
				UpdatedTime:      time.Now(),
			})
		}
		
		allocationJSON, err := goxJsonUtils.ObjectToString(allocation)
		if err != nil {
			return err
		}
		
		err = h.setup.db.UpsertAllocation(h.ctx, helixClusterMysql.UpsertAllocationParams{
			Cluster:       h.setup.clusterName,
			Domain:        h.setup.domain,
			Tasklist:      h.setup.taskList,
			NodeID:        nodeId,
			PartitionInfo: allocationJSON,
			Metadata:      sql.NullString{Valid: true, String: "{}"},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// runAlgorithm executes the SimpleAllocationAlgorithm
func (h *SimpleAlgorithmE2ETestHelper) runAlgorithm(partitionCount int) error {
	algorithm := &SimpleAllocationAlgorithm{
		CrossFunction:   h.setup.mockCf,
		dbInterface:     h.setup.db,
		AlgorithmConfig: &managment.AlgorithmConfig{},
	}
	
	taskListInfo := managment.TaskListInfo{
		Cluster:        h.setup.clusterName,
		Domain:         h.setup.domain,
		TaskList:       h.setup.taskList,
		PartitionCount: partitionCount,
	}
	
	_, err := algorithm.CalculateAllocation(h.ctx, taskListInfo)
	return err
}

// validateNoDuplicates checks that no partition is assigned to multiple nodes
func (h *SimpleAlgorithmE2ETestHelper) validateNoDuplicates(t *testing.T) {
	allocations, err := h.setup.db.GetAllocationsForTasklist(h.ctx, helixClusterMysql.GetAllocationsForTasklistParams{
		Cluster:  h.setup.clusterName,
		Domain:   h.setup.domain,
		Tasklist: h.setup.taskList,
	})
	assert.NoError(t, err, "Failed to get allocations")
	
	partitionToNodes := make(map[string][]string)
	
	for _, allocation := range allocations {
		allocationInfo, err := goxJsonUtils.BytesToObject[struct {
			PartitionAllocationInfos []struct {
				PartitionId      string `json:"partition_id"`
				AllocationStatus string `json:"allocation_status"`
			} `json:"partition_allocation_infos"`
		}]([]byte(allocation.PartitionInfo))
		assert.NoError(t, err, "Failed to parse allocation JSON")
		
		for _, partitionInfo := range allocationInfo.PartitionAllocationInfos {
			if partitionInfo.AllocationStatus == "assigned" {
				partitionToNodes[partitionInfo.PartitionId] = append(
					partitionToNodes[partitionInfo.PartitionId], 
					allocation.NodeID,
				)
			}
		}
	}
	
	// Check for duplicates
	duplicates := 0
	for partitionId, nodes := range partitionToNodes {
		if len(nodes) > 1 {
			duplicates++
			t.Errorf("Partition %s is assigned to multiple nodes: %v", partitionId, nodes)
		}
	}
	
	assert.Equal(t, 0, duplicates, "Found duplicate partition assignments")
}

// validateBalancedDistribution checks that partitions are evenly distributed
func (h *SimpleAlgorithmE2ETestHelper) validateBalancedDistribution(t *testing.T, expectedNodes []string, totalPartitions int) {
	allocations, err := h.setup.db.GetAllocationsForTasklist(h.ctx, helixClusterMysql.GetAllocationsForTasklistParams{
		Cluster:  h.setup.clusterName,
		Domain:   h.setup.domain,
		Tasklist: h.setup.taskList,
	})
	assert.NoError(t, err, "Failed to get allocations")
	
	nodePartitionCounts := make(map[string]int)
	for _, nodeId := range expectedNodes {
		nodePartitionCounts[nodeId] = 0
	}
	
	for _, allocation := range allocations {
		allocationInfo, err := goxJsonUtils.BytesToObject[struct {
			PartitionAllocationInfos []struct {
				PartitionId      string `json:"partition_id"`
				AllocationStatus string `json:"allocation_status"`
			} `json:"partition_allocation_infos"`
		}]([]byte(allocation.PartitionInfo))
		assert.NoError(t, err, "Failed to parse allocation JSON")
		
		for _, partitionInfo := range allocationInfo.PartitionAllocationInfos {
			if partitionInfo.AllocationStatus == "assigned" {
				nodePartitionCounts[allocation.NodeID]++
			}
		}
	}
	
	// Validate balanced distribution
	expectedPerNode := totalPartitions / len(expectedNodes)
	remainder := totalPartitions % len(expectedNodes)
	
	for nodeId, count := range nodePartitionCounts {
		if remainder > 0 {
			assert.True(t, count == expectedPerNode || count == expectedPerNode+1, 
				"Node %s has %d partitions, expected %d or %d", nodeId, count, expectedPerNode, expectedPerNode+1)
		} else {
			assert.Equal(t, expectedPerNode, count, 
				"Node %s has %d partitions, expected %d", nodeId, count, expectedPerNode)
		}
	}
}

func (h *SimpleAlgorithmE2ETestHelper) cleanup() {
	if h.setup != nil && h.setup.app != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		h.setup.app.Stop(ctx)
	}
}

// TestSimpleAlgorithmBalancedAllocationE2E tests basic balanced allocation
func (s *SimpleAlgorithmE2ETestSuite) TestSimpleAlgorithmBalancedAllocationE2E() {
	helper := s.NewE2ETestHelper()
	defer helper.cleanup()
	
	t := s.T()
	partitionCount := 15
	nodeIds := []string{"node1", "node2", "node3", "node4", "node5"}
	
	fmt.Printf("\n" + strings.Repeat("=", 80))
	fmt.Printf("\nE2E TEST: Simple Algorithm Balanced Allocation")
	fmt.Printf("\n" + strings.Repeat("=", 80))
	
	// Setup cluster and nodes
	err := helper.setupCluster(partitionCount)
	assert.NoError(t, err, "Failed to setup cluster")
	
	err = helper.setupNodes(nodeIds)
	assert.NoError(t, err, "Failed to setup nodes")
	
	// Run algorithm (no initial allocations - fresh start)
	err = helper.runAlgorithm(partitionCount)
	assert.NoError(t, err, "Algorithm execution failed")
	
	// Validate results
	helper.validateNoDuplicates(t)
	helper.validateBalancedDistribution(t, nodeIds, partitionCount)
	
	fmt.Printf("\n✅ Test passed: Balanced allocation with no duplicates")
}

// TestSimpleAlgorithmConflictResolutionE2E tests conflict resolution with duplicates
func (s *SimpleAlgorithmE2ETestSuite) TestSimpleAlgorithmConflictResolutionE2E() {
	helper := s.NewE2ETestHelper()
	defer helper.cleanup()
	
	t := s.T()
	partitionCount := 12
	nodeIds := []string{"node1", "node2", "node3", "node4"}
	
	fmt.Printf("\n" + strings.Repeat("=", 80))
	fmt.Printf("\nE2E TEST: Simple Algorithm Conflict Resolution")
	fmt.Printf("\n" + strings.Repeat("=", 80))
	
	// Setup cluster and nodes
	err := helper.setupCluster(partitionCount)
	assert.NoError(t, err, "Failed to setup cluster")
	
	err = helper.setupNodes(nodeIds)
	assert.NoError(t, err, "Failed to setup nodes")
	
	// Create initial state with conflicts (multiple nodes claiming same partitions)
	initialAllocations := map[string][]string{
		"node1": {"0", "1", "2", "3"},  // node1 has 4 partitions
		"node2": {"2", "3", "4", "5"},  // node2 conflicts with node1 on partitions 2,3
		"node3": {"4", "5", "6", "7"},  // node3 conflicts with node2 on partitions 4,5
		"node4": {"8", "9", "10", "11"}, // node4 has unique partitions
	}
	
	err = helper.createInitialAllocations(initialAllocations)
	assert.NoError(t, err, "Failed to create initial allocations")
	
	// Run algorithm - should resolve conflicts
	err = helper.runAlgorithm(partitionCount)
	assert.NoError(t, err, "Algorithm execution failed")
	
	// Validate results - no duplicates and balanced distribution
	helper.validateNoDuplicates(t)
	helper.validateBalancedDistribution(t, nodeIds, partitionCount)
	
	fmt.Printf("\n✅ Test passed: Conflicts resolved with balanced distribution")
}

// TestSimpleAlgorithmRebalancingE2E tests rebalancing with existing allocations
func (s *SimpleAlgorithmE2ETestSuite) TestSimpleAlgorithmRebalancingE2E() {
	helper := s.NewE2ETestHelper()
	defer helper.cleanup()
	
	t := s.T()
	partitionCount := 15
	initialNodes := []string{"node1", "node2", "node3", "node4", "node5"}
	
	fmt.Printf("\n" + strings.Repeat("=", 80))
	fmt.Printf("\nE2E TEST: Simple Algorithm Rebalancing")
	fmt.Printf("\n" + strings.Repeat("=", 80))
	
	// Setup cluster and nodes
	err := helper.setupCluster(partitionCount)
	assert.NoError(t, err, "Failed to setup cluster")
	
	err = helper.setupNodes(initialNodes)
	assert.NoError(t, err, "Failed to setup nodes")
	
	// Create balanced initial allocation
	initialAllocations := map[string][]string{
		"node1": {"0", "1", "2"},
		"node2": {"3", "4", "5"},
		"node3": {"6", "7", "8"},
		"node4": {"9", "10", "11"},
		"node5": {"12", "13", "14"},
	}
	
	err = helper.createInitialAllocations(initialAllocations)
	assert.NoError(t, err, "Failed to create initial allocations")
	
	// For this test, let's just run the algorithm to rebalance the existing allocations
	// Run algorithm - should maintain balanced distribution
	err = helper.runAlgorithm(partitionCount)
	assert.NoError(t, err, "Algorithm execution failed")
	
	// Validate results - should maintain balanced distribution across all nodes
	helper.validateNoDuplicates(t)
	helper.validateBalancedDistribution(t, initialNodes, partitionCount)
	
	fmt.Printf("\n✅ Test passed: Maintained balanced distribution")
}

func TestSimpleAlgorithmE2ETestSuite(t *testing.T) {
	suite.Run(t, new(SimpleAlgorithmE2ETestSuite))
}