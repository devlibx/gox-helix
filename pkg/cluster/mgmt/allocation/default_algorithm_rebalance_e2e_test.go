package allocation

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
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

// E2ETestSuite provides end-to-end testing for CalculateAllocation with real database operations
type E2ETestSuite struct {
	suite.Suite
	helixLockMySqlConfig *helixLock.MySqlConfig
}

// SetupSuite initializes the test environment with database configuration
func (s *E2ETestSuite) SetupSuite() {
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

// E2ETestSetup holds all dependencies needed for E2E testing
type E2ETestSetup struct {
	app       *fx.App
	mockCf    *util.MockCrossFunction
	algorithm *defaultAlgorithm
	db        *helixClusterMysql.Queries
	sqlDb     *sql.DB

	// Test identifiers for isolation
	clusterName string
	domain      string
	taskList    string
}

// makeApp creates a new test setup with real database dependencies
func (s *E2ETestSuite) makeApp() *E2ETestSetup {
	ts := &E2ETestSetup{
		mockCf:      util.NewMockCrossFunction(time.Now()),
		clusterName: "automation-" + uuid.NewString(),
		domain:      "automation-" + uuid.NewString(),
		taskList:    "automation-" + uuid.NewString(),
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
		fx.Provide(func(cf gox.CrossFunction, db *helixClusterMysql.Queries) *defaultAlgorithm {
			return &defaultAlgorithm{
				CrossFunction: cf,
				dbInterface:   db,
				AlgorithmConfig: &managment.AlgorithmConfig{
					TimeToWaitForPartitionReleaseBeforeForceRelease: 5 * time.Minute,
				},
			}
		}),
		fx.Populate(
			&ts.db,
			&ts.sqlDb,
			&ts.algorithm,
		),
	)

	err := ts.app.Start(context.Background())
	s.Require().NoError(err, "Failed to start test application")
	return ts
}

// TearDownTest cleans up after each test
func (s *E2ETestSuite) TearDownTest() {
	// Database cleanup will be handled by the helper methods
}

// E2ETestHelper provides utilities for E2E testing with database operations
type E2ETestHelper struct {
	suite *E2ETestSuite
	setup *E2ETestSetup
	ctx   context.Context
}

// NewE2ETestHelper creates a new E2E test helper
func (s *E2ETestSuite) NewE2ETestHelper() *E2ETestHelper {
	return &E2ETestHelper{
		suite: s,
		setup: s.makeApp(),
		ctx:   context.Background(),
	}
}

// setupCluster creates a cluster in the database
func (h *E2ETestHelper) setupCluster(partitionCount int) error {
	return h.setup.db.UpsertCluster(h.ctx, helixClusterMysql.UpsertClusterParams{
		Cluster:        h.setup.clusterName,
		Domain:         h.setup.domain,
		Tasklist:       h.setup.taskList,
		PartitionCount: uint32(partitionCount),
		Metadata:       sql.NullString{Valid: true, String: "{}"},
	})
}

// setupNodes creates nodes in the database
func (h *E2ETestHelper) setupNodes(nodeIds []string) error {
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

// PartitionInfo represents a partition with its allocation status
type PartitionInfo struct {
	PartitionId string
	Status      managment.PartitionAllocationStatus
	UpdatedTime time.Time
}

// setupInitialAllocations creates initial partition allocations in the database
func (h *E2ETestHelper) setupInitialAllocations(allocations map[string][]PartitionInfo) error {
	for nodeId, partitions := range allocations {
		if len(partitions) == 0 {
			continue
		}

		// Convert to the expected format
		partitionAllocationInfos := make([]managment.PartitionAllocationInfo, 0)
		for _, partition := range partitions {
			partitionAllocationInfos = append(partitionAllocationInfos, managment.PartitionAllocationInfo{
				PartitionId:      partition.PartitionId,
				AllocationStatus: partition.Status,
				UpdatedTime:      partition.UpdatedTime,
			})
		}

		dbInfo := dbPartitionAllocationInfos{
			PartitionAllocationInfos: partitionAllocationInfos,
		}

		// Marshal to JSON
		info, err := goxJsonUtils.ObjectToString(dbInfo)
		if err != nil {
			return err
		}

		// Store in database
		err = h.setup.db.UpsertAllocation(h.ctx, helixClusterMysql.UpsertAllocationParams{
			Cluster:       h.setup.clusterName,
			Domain:        h.setup.domain,
			Tasklist:      h.setup.taskList,
			NodeID:        nodeId,
			PartitionInfo: info,
			Metadata:      sql.NullString{Valid: true, String: "{}"},
		})
		if err != nil {
			return err
		}
	}
	return nil
}

// executeCalculateAllocation runs the full E2E CalculateAllocation method
func (h *E2ETestHelper) executeCalculateAllocation(partitionCount int) (*managment.AllocationResponse, error) {
	taskListInfo := managment.TaskListInfo{
		Cluster:        h.setup.clusterName,
		Domain:         h.setup.domain,
		TaskList:       h.setup.taskList,
		PartitionCount: partitionCount,
	}

	return h.setup.algorithm.CalculateAllocation(h.ctx, taskListInfo)
}

// getAllocationsFromDB retrieves all current allocations from the database
func (h *E2ETestHelper) getAllocationsFromDB() (map[string][]PartitionInfo, error) {
	allocations, err := h.setup.db.GetAllocationsForTasklist(h.ctx, helixClusterMysql.GetAllocationsForTasklistParams{
		Cluster:  h.setup.clusterName,
		Domain:   h.setup.domain,
		Tasklist: h.setup.taskList,
	})
	if err != nil {
		return nil, err
	}

	result := make(map[string][]PartitionInfo)
	for _, allocation := range allocations {
		dbInfo, err := goxJsonUtils.BytesToObject[dbPartitionAllocationInfos]([]byte(allocation.PartitionInfo))
		if err != nil {
			return nil, err
		}

		partitions := make([]PartitionInfo, 0)
		for _, info := range dbInfo.PartitionAllocationInfos {
			partitions = append(partitions, PartitionInfo{
				PartitionId: info.PartitionId,
				Status:      info.AllocationStatus,
				UpdatedTime: info.UpdatedTime,
			})
		}
		result[allocation.NodeID] = partitions
	}

	return result, nil
}

// printDBAllocations provides human-friendly visualization of database allocations
func (h *E2ETestHelper) printDBAllocations(title string, allocations map[string][]PartitionInfo, activeNodes []string) {
	fmt.Printf("\n=== %s ===\n", title)

	// Sort node IDs for consistent output
	nodeIds := make([]string, 0, len(allocations))
	for nodeId := range allocations {
		nodeIds = append(nodeIds, nodeId)
	}

	// Add active nodes that might not have allocations
	for _, nodeId := range activeNodes {
		found := false
		for _, existing := range nodeIds {
			if existing == nodeId {
				found = true
				break
			}
		}
		if !found {
			nodeIds = append(nodeIds, nodeId)
		}
	}

	sort.Strings(nodeIds)

	totalActivePartitions := 0
	totalReleasePartitions := 0

	for _, nodeId := range nodeIds {
		partitions := allocations[nodeId]
		assignments := make([]string, 0)
		activeCount := 0
		releaseCount := 0

		// Sort partitions by ID
		sort.Slice(partitions, func(i, j int) bool {
			return partitions[i].PartitionId < partitions[j].PartitionId
		})

		for _, partition := range partitions {
			switch partition.Status {
			case managment.PartitionAllocationAssigned:
				assignments = append(assignments, fmt.Sprintf("P%s(A)", partition.PartitionId))
				activeCount++
			case managment.PartitionAllocationRequestedRelease:
				assignments = append(assignments, fmt.Sprintf("P%s(RR)", partition.PartitionId))
				releaseCount++
			case managment.PartitionAllocationPendingRelease:
				assignments = append(assignments, fmt.Sprintf("P%s(PR)", partition.PartitionId))
				releaseCount++
			case managment.PartitionAllocationUnassigned:
				assignments = append(assignments, fmt.Sprintf("P%s(U)", partition.PartitionId))
			}
		}

		totalCount := activeCount + releaseCount
		fmt.Printf("Node %s: [%s] (Active: %d, Release: %d, Total: %d)\n",
			nodeId,
			strings.Join(assignments, ", "),
			activeCount,
			releaseCount,
			totalCount)

		totalActivePartitions += activeCount
		totalReleasePartitions += releaseCount
	}

	totalPartitions := totalActivePartitions + totalReleasePartitions
	fmt.Printf("Summary: Active=%d, Release=%d, Total=%d\n",
		totalActivePartitions, totalReleasePartitions, totalPartitions)

	// Legend
	fmt.Printf("Legend: (A)=Assigned, (RR)=RequestedRelease, (PR)=PendingRelease, (U)=Unassigned\n")
}

// runE2ETest executes a complete E2E test scenario
func (h *E2ETestHelper) runE2ETest(t *testing.T, testName string, partitionCount int, nodeIds []string,
	initialAllocations map[string][]PartitionInfo) {

	fmt.Printf("\n" + strings.Repeat("=", 80))
	fmt.Printf("\nE2E TEST: %s", testName)
	fmt.Printf("\n" + strings.Repeat("=", 80))

	// Setup cluster
	err := h.setupCluster(partitionCount)
	assert.NoError(t, err, "Failed to setup cluster")

	// Setup nodes
	err = h.setupNodes(nodeIds)
	assert.NoError(t, err, "Failed to setup nodes")

	// Setup initial allocations
	err = h.setupInitialAllocations(initialAllocations)
	assert.NoError(t, err, "Failed to setup initial allocations")

	// Get and print before state
	beforeAllocations, err := h.getAllocationsFromDB()
	assert.NoError(t, err, "Failed to get before allocations")
	h.printDBAllocations("BEFORE E2E CALCULATION", beforeAllocations, nodeIds)

	// Execute CalculateAllocation (full E2E)
	response, err := h.executeCalculateAllocation(partitionCount)
	assert.NoError(t, err, "Failed to execute CalculateAllocation")
	assert.NotNil(t, response, "Response should not be nil")

	// Get and print after state
	afterAllocations, err := h.getAllocationsFromDB()
	assert.NoError(t, err, "Failed to get after allocations")
	h.printDBAllocations("AFTER E2E CALCULATION", afterAllocations, nodeIds)

	// Validate no partitions were lost
	h.validateNoPartitionsLost(t, beforeAllocations, afterAllocations, partitionCount)
}

// validateAllPartitionsAssigned ensures every partition is assigned to a node (critical safety check)
func (h *E2ETestHelper) validateAllPartitionsAssigned(t *testing.T, allocations map[string][]PartitionInfo, partitionCount int) {
	assignedPartitions := make(map[string]bool)
	
	// Collect all assigned partitions (including release states - they still have owners)
	for _, partitions := range allocations {
		for _, partition := range partitions {
			// All statuses except unassigned should have an owner
			if partition.Status != managment.PartitionAllocationUnassigned {
				assignedPartitions[partition.PartitionId] = true
			}
		}
	}
	
	// Verify every partition from 0 to partitionCount-1 is assigned to a node
	for i := 0; i < partitionCount; i++ {
		partitionId := fmt.Sprintf("%d", i)
		assert.True(t, assignedPartitions[partitionId], 
			"SAFETY CHECK FAILED: Partition %s is not assigned to any node! This must never happen.", partitionId)
	}
	
	// Also verify we have exactly the expected number of assigned partitions
	assert.Equal(t, partitionCount, len(assignedPartitions), 
		"SAFETY CHECK FAILED: Expected %d assigned partitions, but found %d assigned partitions", 
		partitionCount, len(assignedPartitions))
}

// validateNoPartitionsLost ensures all partitions are accounted for in the database
func (h *E2ETestHelper) validateNoPartitionsLost(t *testing.T, before, after map[string][]PartitionInfo, expectedTotal int) {
	beforePartitions := make(map[string]bool)
	afterPartitions := make(map[string]bool)

	// Collect all partition IDs from before state
	for _, partitions := range before {
		for _, partition := range partitions {
			beforePartitions[partition.PartitionId] = true
		}
	}

	// Collect all partition IDs from after state
	for _, partitions := range after {
		for _, partition := range partitions {
			afterPartitions[partition.PartitionId] = true
		}
	}

	// If we started with no partitions, we should end up with all expected partitions
	if len(beforePartitions) == 0 {
		assert.Equal(t, expectedTotal, len(afterPartitions), "Should have all expected partitions when starting from empty")
	} else {
		// Check no partitions were lost
		for partitionId := range beforePartitions {
			assert.True(t, afterPartitions[partitionId], "Partition %s was lost during E2E calculation", partitionId)
		}

		// The algorithm may add missing partitions to reach the expected total count
		// This is correct behavior - if we have fewer partitions than expected, it should add them
		expectedFinalCount := expectedTotal
		if len(beforePartitions) < expectedTotal {
			expectedFinalCount = expectedTotal
		} else {
			expectedFinalCount = len(beforePartitions)
		}

		// Verify total partition count matches expected final count
		assert.Equal(t, expectedFinalCount, len(afterPartitions), "Total partition count should match expected final count")
	}
}

// cleanup removes test data from the database
func (h *E2ETestHelper) cleanup() {
	// Clean up allocations for this test
	h.setup.db.MarkNodeInactive(h.ctx, h.setup.clusterName)

	// Stop the fx application
	if h.setup.app != nil {
		h.setup.app.Stop(h.ctx)
	}
}

// =====================================================================================
// CORE E2E TEST SCENARIOS (Based on unit tests but with real database operations)
// =====================================================================================

// Test 1: Basic even distribution - 15 partitions across 5 nodes (3 each)
func (s *E2ETestSuite) TestCalculateAllocation_BasicEvenDistribution() {
	helper := s.NewE2ETestHelper()
	defer helper.cleanup()

	nodeIds := []string{"node1", "node2", "node3", "node4", "node5"}
	partitionCount := 15

	// Start with no initial allocations (all partitions will be created as unassigned)
	initialAllocations := make(map[string][]PartitionInfo)

	helper.runE2ETest(s.T(), "Basic Even Distribution", partitionCount, nodeIds, initialAllocations)

	// Verify results from database
	allocations, err := helper.getAllocationsFromDB()
	s.Require().NoError(err, "Failed to get allocations from DB")

	// SAFETY CHECK: Ensure every partition is assigned to at least one node
	helper.validateAllPartitionsAssigned(s.T(), allocations, partitionCount)

	// Count assigned partitions per node
	nodeCounts := make(map[string]int)
	for _, partitions := range allocations {
		for _, partition := range partitions {
			if partition.Status == managment.PartitionAllocationAssigned {
				// Find which node this partition belongs to
				for nodeId, nodePartitions := range allocations {
					for _, nodePartition := range nodePartitions {
						if nodePartition.PartitionId == partition.PartitionId &&
							nodePartition.Status == managment.PartitionAllocationAssigned {
							nodeCounts[nodeId]++
							break
						}
					}
				}
			}
		}
	}

	// Each node should have exactly 3 partitions
	for _, nodeId := range nodeIds {
		s.Equal(3, nodeCounts[nodeId], "Node %s should have exactly 3 partitions", nodeId)
	}
	s.Equal(5, len(nodeCounts), "All 5 nodes should have partitions")
}

// Test 2: Uneven distribution with remainder - 16 partitions across 5 nodes (3-4 each)
func (s *E2ETestSuite) TestCalculateAllocation_UnevenDistributionWithRemainder() {
	helper := s.NewE2ETestHelper()
	defer helper.cleanup()

	nodeIds := []string{"node1", "node2", "node3", "node4", "node5"}
	partitionCount := 16

	// Start with no initial allocations
	initialAllocations := make(map[string][]PartitionInfo)

	helper.runE2ETest(s.T(), "Uneven Distribution With Remainder", partitionCount, nodeIds, initialAllocations)

	// Verify results from database
	allocations, err := helper.getAllocationsFromDB()
	s.Require().NoError(err, "Failed to get allocations from DB")

	// SAFETY CHECK: Ensure every partition is assigned to at least one node
	helper.validateAllPartitionsAssigned(s.T(), allocations, partitionCount)

	// Count assigned partitions per node
	nodeCounts := make(map[string]int)
	for nodeId, partitions := range allocations {
		for _, partition := range partitions {
			if partition.Status == managment.PartitionAllocationAssigned {
				nodeCounts[nodeId]++
			}
		}
	}

	// Count nodes with 3 and 4 partitions
	nodes4 := 0
	nodes3 := 0
	for _, count := range nodeCounts {
		if count == 4 {
			nodes4++
		} else if count == 3 {
			nodes3++
		} else {
			s.Failf("Unexpected partition count", "Node has %d partitions, expected 3 or 4", count)
		}
	}

	s.Equal(1, nodes4, "Exactly 1 node should have 4 partitions")
	s.Equal(4, nodes3, "Exactly 4 nodes should have 3 partitions")
}

// Test 3: Rebalancing over-allocated nodes
func (s *E2ETestSuite) TestCalculateAllocation_RebalanceOverAllocatedNodes() {
	helper := s.NewE2ETestHelper()
	defer helper.cleanup()

	nodeIds := []string{"node1", "node2", "node3", "node4", "node5"}
	partitionCount := 15

	// Initial state: node1 and node2 have 5 partitions each, others have 0, remaining 5 unassigned
	now := helper.setup.mockCf.Now()
	initialAllocations := map[string][]PartitionInfo{
		"node1": {
			{PartitionId: "0", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "1", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "2", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "3", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "4", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
		},
		"node2": {
			{PartitionId: "5", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "6", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "7", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "8", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "9", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
		},
	}

	helper.runE2ETest(s.T(), "Rebalance Over-allocated Nodes", partitionCount, nodeIds, initialAllocations)

	// Verify balanced distribution in database
	allocations, err := helper.getAllocationsFromDB()
	s.Require().NoError(err, "Failed to get allocations from DB")

	// SAFETY CHECK: Ensure every partition is assigned to at least one node
	helper.validateAllPartitionsAssigned(s.T(), allocations, partitionCount)

	nodeCounts := make(map[string]int)
	for nodeId, partitions := range allocations {
		for _, partition := range partitions {
			if partition.Status == managment.PartitionAllocationAssigned {
				nodeCounts[nodeId]++
			}
		}
	}

	// All nodes should have exactly 3 partitions
	for _, nodeId := range nodeIds {
		s.Equal(3, nodeCounts[nodeId], "Node %s should have exactly 3 partitions", nodeId)
	}
}

// Test 4: Partitions in release states are not counted or reassigned
func (s *E2ETestSuite) TestCalculateAllocation_ReleaseStatePartitions() {
	helper := s.NewE2ETestHelper()
	defer helper.cleanup()

	nodeIds := []string{"node1", "node2", "node3", "node4", "node5"}
	partitionCount := 15

	now := helper.setup.mockCf.Now()
	initialAllocations := map[string][]PartitionInfo{
		"node1": {
			// 3 assigned + 2 pending release (should count as having 3 active)
			{PartitionId: "0", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "1", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "2", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "3", Status: managment.PartitionAllocationPendingRelease, UpdatedTime: now},
			{PartitionId: "4", Status: managment.PartitionAllocationPendingRelease, UpdatedTime: now},
		},
		"node2": {
			// 2 assigned + 3 requested release (should count as having 2 active)
			{PartitionId: "5", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "6", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "7", Status: managment.PartitionAllocationRequestedRelease, UpdatedTime: now},
			{PartitionId: "8", Status: managment.PartitionAllocationRequestedRelease, UpdatedTime: now},
			{PartitionId: "9", Status: managment.PartitionAllocationRequestedRelease, UpdatedTime: now},
		},
	}

	helper.runE2ETest(s.T(), "Release State Partitions", partitionCount, nodeIds, initialAllocations)

	// Verify release state partitions stayed with their original nodes
	allocations, err := helper.getAllocationsFromDB()
	s.Require().NoError(err, "Failed to get allocations from DB")

	// SAFETY CHECK: Ensure every partition is assigned to at least one node
	helper.validateAllPartitionsAssigned(s.T(), allocations, partitionCount)

	// Check that release partitions stayed with original nodes
	releasePartitions := []string{"3", "4", "7", "8", "9"}
	for _, partitionId := range releasePartitions {
		found := false
		expectedNode := "node1"
		if partitionId >= "7" {
			expectedNode = "node2"
		}

		if nodePartitions, exists := allocations[expectedNode]; exists {
			for _, partition := range nodePartitions {
				if partition.PartitionId == partitionId {
					s.Equal(expectedNode, expectedNode,
						"Release partition %s should stay with original node %s", partitionId, expectedNode)
					s.True(partition.Status == managment.PartitionAllocationPendingRelease ||
						partition.Status == managment.PartitionAllocationRequestedRelease,
						"Release partition %s should maintain its release status", partitionId)
					found = true
					break
				}
			}
		}
		s.True(found, "Release partition %s should be found", partitionId)
	}
}

// Test 5: Inactive node partitions get reassigned
func (s *E2ETestSuite) TestCalculateAllocation_InactiveNodePartitions() {
	helper := s.NewE2ETestHelper()
	defer helper.cleanup()

	// Only 3 active nodes (will setup node4 and node5 as inactive by not including them)
	activeNodeIds := []string{"node1", "node2", "node3"}
	partitionCount := 12

	// Setup active nodes
	err := helper.setupCluster(partitionCount)
	s.Require().NoError(err, "Failed to setup cluster")

	err = helper.setupNodes(activeNodeIds)
	s.Require().NoError(err, "Failed to setup nodes")

	// Create initial allocations that include inactive nodes
	now := helper.setup.mockCf.Now()
	initialAllocations := map[string][]PartitionInfo{
		"node1": {
			{PartitionId: "0", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "5", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "10", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
		},
		"node2": {
			{PartitionId: "1", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "6", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "11", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
		},
		"node3": {
			{PartitionId: "2", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "7", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
		},
		// These represent partitions that were assigned to inactive nodes
		"node4": {
			{PartitionId: "3", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "8", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
		},
		"node5": {
			{PartitionId: "4", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "9", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
		},
	}

	err = helper.setupInitialAllocations(initialAllocations)
	s.Require().NoError(err, "Failed to setup initial allocations")

	// Clean up any stale allocations for inactive nodes before test
	err = helper.setup.db.MarkNodeInactive(helper.ctx, "node4")
	if err != nil {
		s.T().Logf("Note: Could not cleanup node4: %v", err)
	}
	err = helper.setup.db.MarkNodeInactive(helper.ctx, "node5")
	if err != nil {
		s.T().Logf("Note: Could not cleanup node5: %v", err)
	}

	// Execute the test
	fmt.Printf("\n" + strings.Repeat("=", 80))
	fmt.Printf("\nE2E TEST: Inactive Node Partitions")
	fmt.Printf("\n" + strings.Repeat("=", 80))

	beforeAllocations, err := helper.getAllocationsFromDB()
	s.Require().NoError(err, "Failed to get before allocations")
	helper.printDBAllocations("BEFORE E2E CALCULATION", beforeAllocations, activeNodeIds)

	response, err := helper.executeCalculateAllocation(partitionCount)
	s.Require().NoError(err, "Failed to execute CalculateAllocation")
	s.Require().NotNil(response, "Response should not be nil")

	afterAllocations, err := helper.getAllocationsFromDB()
	s.Require().NoError(err, "Failed to get after allocations")
	helper.printDBAllocations("AFTER E2E CALCULATION", afterAllocations, activeNodeIds)

	// SAFETY CHECK: Ensure every partition is assigned to at least one node
	helper.validateAllPartitionsAssigned(s.T(), afterAllocations, partitionCount)

	// Verify no partitions are assigned to inactive nodes
	for nodeId, partitions := range afterAllocations {
		if !contains(activeNodeIds, nodeId) {
			// This node should not have any assigned partitions
			for _, partition := range partitions {
				s.NotEqual(managment.PartitionAllocationAssigned, partition.Status,
					"Inactive node %s should not have assigned partition %s", nodeId, partition.PartitionId)
			}
		}
	}

	// Verify even distribution among active nodes (4 partitions each)
	nodeCounts := make(map[string]int)
	for _, nodeId := range activeNodeIds {
		if partitions, exists := afterAllocations[nodeId]; exists {
			for _, partition := range partitions {
				if partition.Status == managment.PartitionAllocationAssigned {
					nodeCounts[nodeId]++
				}
			}
		}
	}

	for _, nodeId := range activeNodeIds {
		s.Equal(4, nodeCounts[nodeId], "Active node %s should have exactly 4 partitions", nodeId)
	}
}

// Helper function to check if slice contains string
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

// Test 6: Single node gets all partitions
func (s *E2ETestSuite) TestCalculateAllocation_SingleNode() {
	helper := s.NewE2ETestHelper()
	defer helper.cleanup()

	nodeIds := []string{"node1"}
	partitionCount := 10

	initialAllocations := make(map[string][]PartitionInfo)

	helper.runE2ETest(s.T(), "Single Node", partitionCount, nodeIds, initialAllocations)

	// Verify single node gets all partitions
	allocations, err := helper.getAllocationsFromDB()
	s.Require().NoError(err, "Failed to get allocations from DB")

	// SAFETY CHECK: Ensure every partition is assigned to at least one node
	helper.validateAllPartitionsAssigned(s.T(), allocations, partitionCount)

	nodeCount := 0
	for _, partitions := range allocations {
		for _, partition := range partitions {
			if partition.Status == managment.PartitionAllocationAssigned {
				nodeCount++
			}
		}
	}

	s.Equal(10, nodeCount, "All 10 partitions should be assigned to the single node")

	// Verify all partitions are assigned to node1
	if node1Partitions, exists := allocations["node1"]; exists {
		assignedCount := 0
		for _, partition := range node1Partitions {
			if partition.Status == managment.PartitionAllocationAssigned {
				assignedCount++
			}
		}
		s.Equal(10, assignedCount, "Node1 should have all 10 partitions assigned")
	}
}

// RunE2ETestSuite runs the complete E2E test suite
func TestE2ETestSuite(t *testing.T) {
	suite.Run(t, new(E2ETestSuite))
}
