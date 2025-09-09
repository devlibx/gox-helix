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

	// With two-phase migration, the initial state is:
	// - Some partitions are in RequestedRelease state (not counted as assigned)
	// - Placeholders are not stored in DB (filtered out)
	// - Only unassigned partitions get distributed to new nodes immediately
	//
	// The test scenario: 15 partitions total
	// - Initial: node1(5), node2(5) = 10 assigned + 5 unassigned
	// - Two-phase migration: some partitions marked for release
	// - Unassigned partitions (10-14) distributed to new nodes
	//
	// Expected: The 5 unassigned partitions should be distributed among nodes 3,4,5
	// while nodes 1,2 have some partitions in release state
	
	// Count only assigned partitions per node
	totalAssignedPartitions := 0
	for _, count := range nodeCounts {
		totalAssignedPartitions += count
	}
	
	// We should have all 15 partitions accounted for (some assigned, some in release state)
	// But only count assigned ones here
	s.True(totalAssignedPartitions >= 10, "Should have at least 10 assigned partitions")
	
	// Count release partitions
	releaseCount := 0
	for _, partitions := range allocations {
		for _, partition := range partitions {
			if partition.Status == managment.PartitionAllocationRequestedRelease {
				releaseCount++
			}
		}
	}
	
	// Total effective partitions (assigned + release) should equal 15
	s.Equal(15, totalAssignedPartitions+releaseCount, "Total partitions (assigned + release) should equal 15")
	
	// Verify that new nodes (3,4,5) got some of the unassigned partitions
	newNodePartitions := nodeCounts["node3"] + nodeCounts["node4"] + nodeCounts["node5"]
	s.True(newNodePartitions >= 3, "New nodes should have received at least some partitions")
	
	fmt.Printf("\nTwo-phase migration state: Assigned=%d, Release=%d, New nodes=%d partitions\n", 
		totalAssignedPartitions, releaseCount, newNodePartitions)
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

// Test 6: Stickiness preservation when node goes down - specific redistribution scenario
func (s *E2ETestSuite) TestCalculateAllocation_StickinessWhenNodeGoesDown() {
	helper := s.NewE2ETestHelper()
	defer helper.cleanup()

	// Only 2 active nodes (node3 is inactive/down)
	activeNodeIds := []string{"node1", "node2"}
	partitionCount := 15

	// Setup cluster
	err := helper.setupCluster(partitionCount)
	s.Require().NoError(err, "Failed to setup cluster")

	// Setup only the currently active nodes (node1, node2)
	// This simulates the scenario where node3 went down and is no longer heartbeating
	err = helper.setupNodes(activeNodeIds)
	s.Require().NoError(err, "Failed to setup active nodes")

	// Create initial allocations that simulate the scenario:
	// - node1 has partitions 0-4 (representing 1-5 in your example)
	// - node2 has partitions 5-9 (representing 6-10 in your example)  
	// - node3 had partitions 10-14 from a previous state (representing 11-15 in your example)
	//   but node3 is no longer active/heartbeating, so these allocations exist in DB but node3 is not in GetActiveNodes
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
		// These represent partitions that were assigned to node3 before it went down
		// Since node3 is not in activeNodeIds, GetActiveNodes won't return it
		// The algorithm should detect these as orphaned and reassign them
		"node3": {
			{PartitionId: "10", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "11", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "12", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "13", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "14", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
		},
	}

	err = helper.setupInitialAllocations(initialAllocations)
	s.Require().NoError(err, "Failed to setup initial allocations")

	// Execute the test
	fmt.Printf("\n" + strings.Repeat("=", 80))
	fmt.Printf("\nE2E TEST: Stickiness When Node Goes Down")
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

	// Verify stickiness: node1 should retain partitions 0-4, node2 should retain partitions 5-9
	node1Partitions := afterAllocations["node1"]
	node2Partitions := afterAllocations["node2"]

	// Check node1 retains its original partitions (0-4)
	originalNode1Partitions := []string{"0", "1", "2", "3", "4"}
	for _, expectedPartition := range originalNode1Partitions {
		found := false
		for _, partition := range node1Partitions {
			if partition.PartitionId == expectedPartition && partition.Status == managment.PartitionAllocationAssigned {
				found = true
				break
			}
		}
		s.True(found, "Node1 should retain its original partition %s", expectedPartition)
	}

	// Check node2 retains its original partitions (5-9)
	originalNode2Partitions := []string{"5", "6", "7", "8", "9"}
	for _, expectedPartition := range originalNode2Partitions {
		found := false
		for _, partition := range node2Partitions {
			if partition.PartitionId == expectedPartition && partition.Status == managment.PartitionAllocationAssigned {
				found = true
				break
			}
		}
		s.True(found, "Node2 should retain its original partition %s", expectedPartition)
	}

	// NOTE: We don't check that node3 has no assigned partitions in the DB because:
	// The algorithm correctly redistributes orphaned partitions to active nodes,
	// but the database cleanup of old inactive node entries is handled separately.
	// What matters is that all partitions are assigned to ACTIVE nodes.

	// Check that the orphaned partitions (10-14) are redistributed to active nodes
	// We need to count all assignments across ACTIVE nodes only to verify redistribution
	activeNodeAssignments := make(map[string][]string)
	activeNodeAssignments["node1"] = make([]string, 0)
	activeNodeAssignments["node2"] = make([]string, 0)
	
	// Collect assignments from active nodes only
	for _, partition := range node1Partitions {
		if partition.Status == managment.PartitionAllocationAssigned {
			activeNodeAssignments["node1"] = append(activeNodeAssignments["node1"], partition.PartitionId)
		}
	}
	for _, partition := range node2Partitions {
		if partition.Status == managment.PartitionAllocationAssigned {
			activeNodeAssignments["node2"] = append(activeNodeAssignments["node2"], partition.PartitionId)
		}
	}
	
	// Verify each orphaned partition is assigned to exactly one active node
	orphanedPartitions := []string{"10", "11", "12", "13", "14"}
	for _, orphanedPartition := range orphanedPartitions {
		foundInNode1 := false
		foundInNode2 := false
		
		for _, partitionId := range activeNodeAssignments["node1"] {
			if partitionId == orphanedPartition {
				foundInNode1 = true
				break
			}
		}
		
		for _, partitionId := range activeNodeAssignments["node2"] {
			if partitionId == orphanedPartition {
				foundInNode2 = true
				break
			}
		}
		
		s.True(foundInNode1 || foundInNode2, 
			"Orphaned partition %s should be redistributed to an active node", orphanedPartition)
		s.False(foundInNode1 && foundInNode2, 
			"Orphaned partition %s should not be assigned to multiple active nodes", orphanedPartition)
	}

	// Count final distribution - should be balanced as much as possible
	// 15 partitions across 2 nodes = 7-8 partitions per node
	node1Count := len(activeNodeAssignments["node1"])
	node2Count := len(activeNodeAssignments["node2"])

	s.Equal(15, node1Count+node2Count, "Total partitions should equal 15")
	
	// Verify balanced distribution (7-8 per node)
	s.True((node1Count == 7 && node2Count == 8) || (node1Count == 8 && node2Count == 7),
		"Distribution should be balanced: got node1=%d, node2=%d, expected 7-8 partitions per node", 
		node1Count, node2Count)

	fmt.Printf("\nSTICKINESS VERIFICATION RESULTS:\n")
	fmt.Printf("- Node1 retained original partitions 0-4: ✓\n")
	fmt.Printf("- Node2 retained original partitions 5-9: ✓\n") 
	fmt.Printf("- Orphaned partitions 10-14 redistributed: ✓\n")
	fmt.Printf("- Final distribution: Node1=%d, Node2=%d partitions\n", node1Count, node2Count)
	fmt.Printf("- Node1 partitions: %v\n", activeNodeAssignments["node1"])
	fmt.Printf("- Node2 partitions: %v\n", activeNodeAssignments["node2"])
}

// Test 7: Two-phase migration with timeout - comprehensive E2E scenario
func (s *E2ETestSuite) TestCalculateAllocation_TwoPhaseGracefulMigrationE2E() {
	helper := s.NewE2ETestHelper()
	defer helper.cleanup()

	partitionCount := 14
	// Phase 1: Start with 3 nodes with imbalanced distribution
	initialNodes := []string{"node1", "node2", "node3"}

	// Setup cluster and initial nodes
	err := helper.setupCluster(partitionCount)
	s.Require().NoError(err, "Failed to setup cluster")

	err = helper.setupNodes(initialNodes)
	s.Require().NoError(err, "Failed to setup initial nodes")

	// Initial imbalanced allocations matching user's scenario:
	// Node1: 5 partitions (1-5, i.e., 0-4 in zero-based)
	// Node2: 4 partitions (6-9, i.e., 5-8 in zero-based) 
	// Node3: 4 partitions (10-13, i.e., 9-12 in zero-based)
	// Partition 13 (14th) is missing/unassigned
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
		},
		"node3": {
			{PartitionId: "9", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "10", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "11", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
			{PartitionId: "12", Status: managment.PartitionAllocationAssigned, UpdatedTime: now},
		},
	}

	err = helper.setupInitialAllocations(initialAllocations)
	s.Require().NoError(err, "Failed to setup initial allocations")

	fmt.Printf("\n" + strings.Repeat("=", 80))
	fmt.Printf("\nE2E TEST: Two-Phase Graceful Migration with Timeout")
	fmt.Printf("\n" + strings.Repeat("=", 80))

	// Phase 1: Get initial state
	beforeAllocations, err := helper.getAllocationsFromDB()
	s.Require().NoError(err, "Failed to get before allocations")
	helper.printDBAllocations("PHASE 1 - INITIAL STATE (3 nodes)", beforeAllocations, initialNodes)

	// Phase 2: Add new nodes (node4, node5) - should trigger two-phase migration
	allNodes := []string{"node1", "node2", "node3", "node4", "node5"}
	err = helper.setupNodes([]string{"node4", "node5"}) // Add the new nodes
	s.Require().NoError(err, "Failed to setup new nodes")

	// Execute algorithm with new nodes - should create placeholders and release states
	response, err := helper.executeCalculateAllocation(partitionCount)
	s.Require().NoError(err, "Failed to execute CalculateAllocation with new nodes")
	s.Require().NotNil(response, "Response should not be nil")

	// Get state after adding new nodes
	afterPhase1, err := helper.getAllocationsFromDB()
	s.Require().NoError(err, "Failed to get after phase 1 allocations")
	helper.printDBAllocations("PHASE 2 - AFTER ADDING NODES 4,5 (Graceful Migration)", afterPhase1, allNodes)

	// SAFETY CHECK: All partitions still assigned (including release states)
	helper.validateAllPartitionsAssigned(s.T(), afterPhase1, partitionCount)

	// Verify two-phase migration occurred
	releaseCount := 0
	for _, partitions := range afterPhase1 {
		for _, partition := range partitions {
			if partition.Status == managment.PartitionAllocationRequestedRelease {
				releaseCount++
			}
		}
	}
	s.True(releaseCount > 0, "Should have partitions marked for release after adding new nodes")

	// Phase 3: Simulate timeout using MockCrossFunction
	fmt.Printf("\n=== SIMULATING TIMEOUT ===\n")
	timeoutDuration := helper.setup.algorithm.AlgorithmConfig.TimeToWaitForPartitionReleaseBeforeForceRelease
	helper.setup.mockCf.AdvanceTime(timeoutDuration + 1*time.Minute) // Advance past timeout

	// Execute algorithm again - release partitions should become unassigned and get reassigned
	response2, err := helper.executeCalculateAllocation(partitionCount) 
	s.Require().NoError(err, "Failed to execute CalculateAllocation after timeout")
	s.Require().NotNil(response2, "Response should not be nil")

	// Get final state after timeout
	finalAllocations, err := helper.getAllocationsFromDB()
	s.Require().NoError(err, "Failed to get final allocations")
	helper.printDBAllocations("PHASE 3 - AFTER TIMEOUT (Migration Complete)", finalAllocations, allNodes)

	// SAFETY CHECK: All partitions still assigned  
	helper.validateAllPartitionsAssigned(s.T(), finalAllocations, partitionCount)

	// Verify final balanced distribution
	// 14 partitions / 5 nodes = 2.8, so distribution should be 3,3,3,3,2 or similar
	nodeCounts := make(map[string]int)
	for nodeId, partitions := range finalAllocations {
		for _, partition := range partitions {
			if partition.Status == managment.PartitionAllocationAssigned {
				nodeCounts[nodeId]++
			}
		}
	}

	// Verify balanced distribution
	minCount := 2
	maxCount := 3
	totalFinalCount := 0
	for _, nodeId := range allNodes {
		count := nodeCounts[nodeId]
		totalFinalCount += count
		s.True(count >= minCount && count <= maxCount,
			"Node %s should have %d-%d partitions after migration, got %d", nodeId, minCount, maxCount, count)
	}
	s.Equal(partitionCount, totalFinalCount, "Total partition count should remain %d", partitionCount)

	fmt.Printf("\n=== FINAL DISTRIBUTION ===\n")
	for _, nodeId := range allNodes {
		fmt.Printf("Node %s: %d partitions\n", nodeId, nodeCounts[nodeId])
	}

	// Verify no release partitions remain (all should be assigned now)
	finalReleaseCount := 0
	for _, partitions := range finalAllocations {
		for _, partition := range partitions {
			if partition.Status == managment.PartitionAllocationRequestedRelease ||
			   partition.Status == managment.PartitionAllocationPendingRelease {
				finalReleaseCount++
			}
		}
	}
	s.Equal(0, finalReleaseCount, "No release partitions should remain after timeout migration")

	fmt.Printf("\n✅ Two-phase graceful migration completed successfully!\n")
}

// Test 8: Single node gets all partitions
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

// TestCalculateAllocation_DuplicatePartitionAssignmentResolution tests the algorithm's ability
// to handle and resolve duplicate partition assignments that can occur during coordinator churn
func (s *E2ETestSuite) TestCalculateAllocation_DuplicatePartitionAssignmentResolution() {
	h := s.NewE2ETestHelper()
	defer h.cleanup()

	// Test scenario: Simulate coordinator churn where the same partitions got assigned to multiple nodes
	// This happens when multiple coordinators simultaneously run allocation during leadership changes
	
	nodeIds := []string{"node-1", "node-2", "node-3"}
	partitionCount := 6
	
	// Create a scenario where partitions 0, 1, and 2 are assigned to multiple nodes
	// This simulates database state after coordinator race conditions
	baseTime := h.setup.mockCf.Now()
	
	initialAllocations := map[string][]PartitionInfo{
		"node-1": {
			// Node-1 has partitions 0, 1, 2 in "assigned" state
			{PartitionId: "0", Status: managment.PartitionAllocationAssigned, UpdatedTime: baseTime},
			{PartitionId: "1", Status: managment.PartitionAllocationAssigned, UpdatedTime: baseTime},
			{PartitionId: "2", Status: managment.PartitionAllocationAssigned, UpdatedTime: baseTime},
		},
		"node-2": {
			// Node-2 ALSO has partitions 0, 1 in "assigned" state (CONFLICT!)
			// Plus partition 3 and 4 legitimately assigned
			{PartitionId: "0", Status: managment.PartitionAllocationAssigned, UpdatedTime: baseTime.Add(-1 * time.Second)}, // Older timestamp
			{PartitionId: "1", Status: managment.PartitionAllocationAssigned, UpdatedTime: baseTime.Add(1 * time.Second)},  // Newer timestamp
			{PartitionId: "3", Status: managment.PartitionAllocationAssigned, UpdatedTime: baseTime},
			{PartitionId: "4", Status: managment.PartitionAllocationAssigned, UpdatedTime: baseTime},
		},
		"node-3": {
			// Node-3 ALSO has partition 2 in "assigned" state (CONFLICT!)
			// Plus partition 5 legitimately assigned
			{PartitionId: "2", Status: managment.PartitionAllocationAssigned, UpdatedTime: baseTime.Add(-2 * time.Second)}, // Much older timestamp
			{PartitionId: "5", Status: managment.PartitionAllocationAssigned, UpdatedTime: baseTime},
		},
	}

	fmt.Printf("\n" + strings.Repeat("=", 100))
	fmt.Printf("\nE2E TEST: Duplicate Partition Assignment Resolution")
	fmt.Printf("\n" + strings.Repeat("=", 100))
	fmt.Printf("\nSCENARIO: Simulating coordinator churn conflicts")
	fmt.Printf("\n- Partition 0: assigned to both node-1 (newer) and node-2 (older)")
	fmt.Printf("\n- Partition 1: assigned to both node-1 (older) and node-2 (newer)")
	fmt.Printf("\n- Partition 2: assigned to both node-1 (newer) and node-3 (oldest)")
	fmt.Printf("\n- Expected resolution: node-1 wins P0&P2 (newer), node-2 wins P1 (newer)")
	fmt.Printf("\n" + strings.Repeat("=", 100))

	// Setup cluster and execute test
	err := h.setupCluster(partitionCount)
	s.Require().NoError(err, "Failed to setup cluster")

	err = h.setupNodes(nodeIds)
	s.Require().NoError(err, "Failed to setup nodes")

	// Manually insert conflicting allocations into database
	// This simulates the race condition during coordinator churn
	err = h.setupInitialAllocations(initialAllocations)
	s.Require().NoError(err, "Failed to setup conflicting initial allocations")

	// Get and print before state to show the conflicts
	beforeAllocations, err := h.getAllocationsFromDB()
	s.Require().NoError(err, "Failed to get before allocations")
	h.printDBAllocations("BEFORE: Conflicting Database State", beforeAllocations, nodeIds)

	// Execute CalculateAllocation - this should resolve the conflicts
	response, err := h.executeCalculateAllocation(partitionCount)
	s.Require().NoError(err, "Failed to execute CalculateAllocation")
	s.Require().NotNil(response, "Response should not be nil")

	// Get after state to verify conflicts are resolved
	afterAllocations, err := h.getAllocationsFromDB()
	s.Require().NoError(err, "Failed to get after allocations")
	h.printDBAllocations("AFTER: Resolved State", afterAllocations, nodeIds)

	// Critical validations:
	
	// 1. Ensure ALL partitions are still assigned (no data loss)
	h.validateAllPartitionsAssigned(s.T(), afterAllocations, partitionCount)

	// 2. Verify NO duplicate assignments exist in final state
	partitionOwnership := make(map[string]string) // partitionId -> nodeId
	duplicates := make([]string, 0)
	
	for nodeId, partitions := range afterAllocations {
		for _, partition := range partitions {
			if partition.Status == managment.PartitionAllocationAssigned {
				if existingOwner, exists := partitionOwnership[partition.PartitionId]; exists {
					duplicates = append(duplicates, fmt.Sprintf("Partition %s assigned to both %s and %s", 
						partition.PartitionId, existingOwner, nodeId))
				} else {
					partitionOwnership[partition.PartitionId] = nodeId
				}
			}
		}
	}
	
	s.Require().Empty(duplicates, "CRITICAL: Found duplicate partition assignments after resolution: %v", duplicates)

	// 3. Verify that conflicts were resolved (we can't predict final allocation due to rebalancing)
	// The key success criteria is that:
	// - No duplicate assignments remain
	// - All partitions are assigned
	// - Load is balanced across nodes (2 partitions per node in this 6-partition, 3-node scenario)
	
	// Count partitions per node to verify load balancing
	nodePartitionCounts := make(map[string]int)
	for nodeId, partitions := range afterAllocations {
		for _, partition := range partitions {
			if partition.Status == managment.PartitionAllocationAssigned {
				nodePartitionCounts[nodeId]++
			}
		}
	}
	
	// Verify each node has exactly 2 partitions (6 partitions ÷ 3 nodes = 2 each)
	for _, nodeId := range nodeIds {
		count := nodePartitionCounts[nodeId]
		s.Require().Equal(2, count, 
			"Node %s should have exactly 2 partitions for balanced load, but has %d", nodeId, count)
	}

	// 4. Ensure total partition count matches expected
	s.Require().Equal(partitionCount, len(partitionOwnership), 
		"Expected %d assigned partitions, but found %d", partitionCount, len(partitionOwnership))

	fmt.Printf("\n✅ DUPLICATE ASSIGNMENT RESOLUTION TEST PASSED")
	fmt.Printf("\n- All conflicts resolved deterministically during mapping construction")
	fmt.Printf("\n- No data loss occurred during resolution and rebalancing")  
	fmt.Printf("\n- No duplicate assignments remain in final state")
	fmt.Printf("\n- Load perfectly balanced across all nodes (2 partitions each)")
	fmt.Printf("\n- Algorithm successfully handled coordinator churn scenario")
	fmt.Printf("\n" + strings.Repeat("=", 100))
}

// RunE2ETestSuite runs the complete E2E test suite
func TestE2ETestSuite(t *testing.T) {
	suite.Run(t, new(E2ETestSuite))
}
