package coordinator

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	helixDomainMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/domain/database"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"testing"
)

func TestDistributorStrategyV1Impl(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockWorkerService := NewMockWorkerService(ctrl)
	mockPartitionService := NewMockPartitionService(ctrl)
	mockDomainService := NewMockDomainService(ctrl)

	d := distributorStrategyV1Impl{
		CrossFunction: gox.NewCrossFunction(),
		ws:            mockWorkerService,
		ps:            mockPartitionService,
		ds:            mockDomainService,
	}

	mockDomainService.EXPECT().GetTaskListInfo(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			&helixDomainMysql.HelixDomain{PartitionCount: 10},
			nil,
		)
	mockWorkerService.EXPECT().GetActiveWorkers(gomock.Any(), gomock.Any()).Return([]string{"node-1", "node-2"}, nil)
	mockPartitionService.EXPECT().GetActivePartitionMappings(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(
			[]WorkerPartitionMapping{
				{
					OwnerID: "1",
					Mapping: map[int]DistributionMapping{
						0: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
						1: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
						2: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
					},
				},
			},
			nil,
		)

	distributionResponse, err := d.Distribute(context.Background(), DistributionRequest{DomainName: "test", TaskList: "test"})
	assert.NoError(t, err)
	// assert.Equal(t, []string{"node-1", "node-2"}, distributionResponse.TaskList)
	_ = distributionResponse
}

func TestBuildExisting(t *testing.T) {
	d := distributorStrategyV1Impl{}

	// Test 1 - all assinged
	activePartitionMapping := make([]WorkerPartitionMapping, 0)
	activePartitionMapping = append(activePartitionMapping,
		WorkerPartitionMapping{"owner-1",
			map[int]DistributionMapping{
				0: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
				1: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
			}},
	)

	result := d.buildExisting(activePartitionMapping, 2)
	assert.Equal(t, 2, len(result))
	assert.Equal(t, databaseCommon.PartitionAssignmentStatusAssigned, result[0].Status)
	assert.Equal(t, "owner-1", result[0].OwnerId)
	assert.Equal(t, databaseCommon.PartitionAssignmentStatusAssigned, result[1].Status)
	assert.Equal(t, "owner-1", result[1].OwnerId)

	// Test 2 - all assigned and some unassigned
	activePartitionMapping = make([]WorkerPartitionMapping, 0)
	activePartitionMapping = append(activePartitionMapping,
		WorkerPartitionMapping{"owner-1",
			map[int]DistributionMapping{
				0: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
				1: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
				2: {Status: databaseCommon.PartitionAssignmentStatusUnassigned},
			}},
	)

	result = d.buildExisting(activePartitionMapping, 3)
	assert.Equal(t, 3, len(result))
	assert.Equal(t, databaseCommon.PartitionAssignmentStatusAssigned, result[0].Status)
	assert.Equal(t, "owner-1", result[0].OwnerId)
	assert.Equal(t, databaseCommon.PartitionAssignmentStatusAssigned, result[1].Status)
	assert.Equal(t, "owner-1", result[1].OwnerId)
	assert.Equal(t, databaseCommon.PartitionAssignmentStatusUnassigned, result[2].Status)
	assert.Equal(t, "owner-1", result[2].OwnerId)

	// Test 3 - all assigned and some unassigned
	// But we have modified partition count
	activePartitionMapping = make([]WorkerPartitionMapping, 0)
	activePartitionMapping = append(activePartitionMapping,
		WorkerPartitionMapping{"owner-1",
			map[int]DistributionMapping{
				0: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
				1: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
				2: {Status: databaseCommon.PartitionAssignmentStatusUnassigned},
			}},
	)

	result = d.buildExisting(activePartitionMapping, 4)
	assert.Equal(t, 4, len(result))
	assert.Equal(t, databaseCommon.PartitionAssignmentStatusAssigned, result[0].Status)
	assert.Equal(t, "owner-1", result[0].OwnerId)
	assert.Equal(t, databaseCommon.PartitionAssignmentStatusAssigned, result[1].Status)
	assert.Equal(t, "owner-1", result[1].OwnerId)
	assert.Equal(t, databaseCommon.PartitionAssignmentStatusUnassigned, result[2].Status)
	assert.Equal(t, "owner-1", result[2].OwnerId)
	assert.Equal(t, databaseCommon.PartitionAssignmentStatusUnassigned, result[3].Status)
	assert.Equal(t, "", result[3].OwnerId)

	// Test 4 - all assigned and some unassigned and 3 owners
	// Also missing partitions which is 6
	// But we have modified partition count
	activePartitionMapping = make([]WorkerPartitionMapping, 0)
	activePartitionMapping = append(activePartitionMapping,
		WorkerPartitionMapping{"owner-1",
			map[int]DistributionMapping{
				0: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
				1: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
				2: {Status: databaseCommon.PartitionAssignmentStatusUnassigned},
			},
		},
		WorkerPartitionMapping{"owner-2",
			map[int]DistributionMapping{
				3: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
				5: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
				8: {Status: databaseCommon.PartitionAssignmentStatusUnassigned},
			},
		},
		WorkerPartitionMapping{"owner-3",
			map[int]DistributionMapping{
				4: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
				7: {Status: databaseCommon.PartitionAssignmentStatusAssigned},
			},
		},
	)

	result = d.buildExisting(activePartitionMapping, 9)
	assert.Equal(t, 9, len(result))
}

func TestAssignPartitions(t *testing.T) {
	d := distributorStrategyV1Impl{}

	// Test 1: All partitions sticky assignment
	// All 10 partitions are already assigned to existing workers, should stick to original owners
	t.Run("all partitions sticky assignment", func(t *testing.T) {
		buckets := map[string]*algorithmV1Bucket{
			"worker-1": {OwnerId: "worker-1", MaxPartitionsAllowed: 5, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-2": {OwnerId: "worker-2", MaxPartitionsAllowed: 5, Partitions: []algorithmV1OwnerPartitionMapping{}},
		}

		existingMapping := map[int]algorithmV1OwnerPartitionMapping{
			0: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 0},
			1: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 1},
			2: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 2},
			3: {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 3},
			4: {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 4},
			5: {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 5},
		}

		d.assignPartitions(buckets, existingMapping)

		// Verify worker-1 has 3 partitions
		assert.Equal(t, 3, len(buckets["worker-1"].Partitions))
		assert.True(t, len(buckets["worker-1"].Partitions) <= buckets["worker-1"].MaxPartitionsAllowed)

		// Verify worker-2 has 3 partitions
		assert.Equal(t, 3, len(buckets["worker-2"].Partitions))
		assert.True(t, len(buckets["worker-2"].Partitions) <= buckets["worker-2"].MaxPartitionsAllowed)

		// Verify sticky assignment - partitions stayed with original owners
		for _, p := range buckets["worker-1"].Partitions {
			assert.Equal(t, "worker-1", p.OwnerId)
			assert.Contains(t, []int{0, 1, 2}, p.Partition)
		}
		for _, p := range buckets["worker-2"].Partitions {
			assert.Equal(t, "worker-2", p.OwnerId)
			assert.Contains(t, []int{3, 4, 5}, p.Partition)
		}
	})

	// Test 2: Mix of assigned and unassigned
	t.Run("mix of assigned and unassigned", func(t *testing.T) {
		buckets := map[string]*algorithmV1Bucket{
			"worker-1": {OwnerId: "worker-1", MaxPartitionsAllowed: 4, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-2": {OwnerId: "worker-2", MaxPartitionsAllowed: 4, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-3": {OwnerId: "worker-3", MaxPartitionsAllowed: 4, Partitions: []algorithmV1OwnerPartitionMapping{}},
		}

		existingMapping := map[int]algorithmV1OwnerPartitionMapping{
			0: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 0},
			1: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 1},
			2: {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 2},
			3: {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 3},
			4: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 4},
			5: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 5},
			6: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 6},
			7: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 7},
		}

		d.assignPartitions(buckets, existingMapping)

		// Verify total assignments
		total := len(buckets["worker-1"].Partitions) + len(buckets["worker-2"].Partitions) + len(buckets["worker-3"].Partitions)
		assert.Equal(t, 8, total, "all 8 partitions should be assigned")

		// Verify capacity limits
		assert.True(t, len(buckets["worker-1"].Partitions) <= buckets["worker-1"].MaxPartitionsAllowed)
		assert.True(t, len(buckets["worker-2"].Partitions) <= buckets["worker-2"].MaxPartitionsAllowed)
		assert.True(t, len(buckets["worker-3"].Partitions) <= buckets["worker-3"].MaxPartitionsAllowed)

		// Verify sticky assignments
		assert.GreaterOrEqual(t, len(buckets["worker-1"].Partitions), 2, "worker-1 should have at least its 2 original partitions")
		assert.GreaterOrEqual(t, len(buckets["worker-2"].Partitions), 2, "worker-2 should have at least its 2 original partitions")
	})

	// Test 3: Original owner no longer active
	t.Run("original owner no longer active", func(t *testing.T) {
		buckets := map[string]*algorithmV1Bucket{
			"new-worker-1": {OwnerId: "new-worker-1", MaxPartitionsAllowed: 5, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"new-worker-2": {OwnerId: "new-worker-2", MaxPartitionsAllowed: 5, Partitions: []algorithmV1OwnerPartitionMapping{}},
		}

		existingMapping := map[int]algorithmV1OwnerPartitionMapping{
			0: {OwnerId: "old-worker", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 0},
			1: {OwnerId: "old-worker", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 1},
			2: {OwnerId: "old-worker", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 2},
			3: {OwnerId: "old-worker", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 3},
			4: {OwnerId: "old-worker", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 4},
			5: {OwnerId: "old-worker", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 5},
		}

		d.assignPartitions(buckets, existingMapping)

		// Verify all partitions redistributed to new workers
		total := len(buckets["new-worker-1"].Partitions) + len(buckets["new-worker-2"].Partitions)
		assert.Equal(t, 6, total, "all 6 partitions should be redistributed")

		// Verify capacity limits
		assert.True(t, len(buckets["new-worker-1"].Partitions) <= buckets["new-worker-1"].MaxPartitionsAllowed)
		assert.True(t, len(buckets["new-worker-2"].Partitions) <= buckets["new-worker-2"].MaxPartitionsAllowed)
	})

	// Test 4: More partitions than total capacity (overflow)
	t.Run("more partitions than capacity - overflow", func(t *testing.T) {
		buckets := map[string]*algorithmV1Bucket{
			"worker-1": {OwnerId: "worker-1", MaxPartitionsAllowed: 5, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-2": {OwnerId: "worker-2", MaxPartitionsAllowed: 5, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-3": {OwnerId: "worker-3", MaxPartitionsAllowed: 5, Partitions: []algorithmV1OwnerPartitionMapping{}},
		}

		existingMapping := make(map[int]algorithmV1OwnerPartitionMapping)
		for i := 0; i < 20; i++ {
			existingMapping[i] = algorithmV1OwnerPartitionMapping{
				OwnerId:   "",
				Status:    databaseCommon.PartitionAssignmentStatusUnassigned,
				Partition: i,
			}
		}

		d.assignPartitions(buckets, existingMapping)

		// Verify only 15 partitions assigned (3 workers * 5 capacity)
		total := len(buckets["worker-1"].Partitions) + len(buckets["worker-2"].Partitions) + len(buckets["worker-3"].Partitions)
		assert.Equal(t, 15, total, "only 15 partitions should be assigned (capacity limit)")

		// Verify capacity limits strictly enforced
		assert.Equal(t, 5, len(buckets["worker-1"].Partitions))
		assert.Equal(t, 5, len(buckets["worker-2"].Partitions))
		assert.Equal(t, 5, len(buckets["worker-3"].Partitions))
	})

	// Test 5: Fewer partitions than capacity (underflow)
	t.Run("fewer partitions than capacity - underflow", func(t *testing.T) {
		buckets := map[string]*algorithmV1Bucket{
			"worker-1": {OwnerId: "worker-1", MaxPartitionsAllowed: 10, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-2": {OwnerId: "worker-2", MaxPartitionsAllowed: 10, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-3": {OwnerId: "worker-3", MaxPartitionsAllowed: 10, Partitions: []algorithmV1OwnerPartitionMapping{}},
		}

		existingMapping := map[int]algorithmV1OwnerPartitionMapping{
			0: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 0},
			1: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 1},
			2: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 2},
			3: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 3},
			4: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 4},
		}

		d.assignPartitions(buckets, existingMapping)

		// Verify all 5 partitions assigned
		total := len(buckets["worker-1"].Partitions) + len(buckets["worker-2"].Partitions) + len(buckets["worker-3"].Partitions)
		assert.Equal(t, 5, total, "all 5 partitions should be assigned")

		// Verify capacity not exceeded
		assert.True(t, len(buckets["worker-1"].Partitions) <= buckets["worker-1"].MaxPartitionsAllowed)
		assert.True(t, len(buckets["worker-2"].Partitions) <= buckets["worker-2"].MaxPartitionsAllowed)
		assert.True(t, len(buckets["worker-3"].Partitions) <= buckets["worker-3"].MaxPartitionsAllowed)
	})

	// Test 6: Partition count increased (scale-up)
	t.Run("partition count increased - scale up", func(t *testing.T) {
		buckets := map[string]*algorithmV1Bucket{
			"worker-1": {OwnerId: "worker-1", MaxPartitionsAllowed: 5, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-2": {OwnerId: "worker-2", MaxPartitionsAllowed: 5, Partitions: []algorithmV1OwnerPartitionMapping{}},
		}

		existingMapping := map[int]algorithmV1OwnerPartitionMapping{
			// Original 5 partitions (assigned)
			0: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 0},
			1: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 1},
			2: {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 2},
			3: {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 3},
			4: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 4},
			// New 5 partitions (unassigned)
			5: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 5},
			6: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 6},
			7: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 7},
			8: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 8},
			9: {OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: 9},
		}

		d.assignPartitions(buckets, existingMapping)

		// Verify all 10 partitions assigned
		total := len(buckets["worker-1"].Partitions) + len(buckets["worker-2"].Partitions)
		assert.Equal(t, 10, total, "all 10 partitions should be assigned")

		// Verify capacity limits
		assert.Equal(t, 5, len(buckets["worker-1"].Partitions))
		assert.Equal(t, 5, len(buckets["worker-2"].Partitions))

		// Verify sticky assignments for original partitions
		worker1HasOriginal := false
		for _, p := range buckets["worker-1"].Partitions {
			if p.Partition == 0 || p.Partition == 1 || p.Partition == 4 {
				worker1HasOriginal = true
			}
		}
		assert.True(t, worker1HasOriginal, "worker-1 should retain at least some original partitions")
	})

	// Test 7: Worker count decreased (scale-down)
	t.Run("worker count decreased - scale down", func(t *testing.T) {
		buckets := map[string]*algorithmV1Bucket{
			"worker-1": {OwnerId: "worker-1", MaxPartitionsAllowed: 5, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-2": {OwnerId: "worker-2", MaxPartitionsAllowed: 5, Partitions: []algorithmV1OwnerPartitionMapping{}},
		}

		existingMapping := map[int]algorithmV1OwnerPartitionMapping{
			0: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 0},
			1: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 1},
			2: {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 2},
			3: {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 3},
			4: {OwnerId: "worker-3", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 4}, // worker-3 removed
			5: {OwnerId: "worker-3", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 5}, // worker-3 removed
			6: {OwnerId: "worker-4", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 6}, // worker-4 removed
			7: {OwnerId: "worker-5", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 7}, // worker-5 removed
		}

		d.assignPartitions(buckets, existingMapping)

		// Verify all 8 partitions redistributed to remaining 2 workers
		total := len(buckets["worker-1"].Partitions) + len(buckets["worker-2"].Partitions)
		assert.Equal(t, 8, total, "all 8 partitions should be redistributed")

		// Verify capacity limits
		assert.True(t, len(buckets["worker-1"].Partitions) <= buckets["worker-1"].MaxPartitionsAllowed)
		assert.True(t, len(buckets["worker-2"].Partitions) <= buckets["worker-2"].MaxPartitionsAllowed)
	})

	// Test 8: Perfect equal distribution
	t.Run("perfect equal distribution", func(t *testing.T) {
		buckets := map[string]*algorithmV1Bucket{
			"worker-1": {OwnerId: "worker-1", MaxPartitionsAllowed: 6, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-2": {OwnerId: "worker-2", MaxPartitionsAllowed: 6, Partitions: []algorithmV1OwnerPartitionMapping{}},
		}

		existingMapping := map[int]algorithmV1OwnerPartitionMapping{
			0:  {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 0},
			1:  {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 1},
			2:  {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 2},
			3:  {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 3},
			4:  {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 4},
			5:  {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 5},
			6:  {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 6},
			7:  {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 7},
			8:  {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 8},
			9:  {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 9},
			10: {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 10},
			11: {OwnerId: "worker-2", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 11},
		}

		d.assignPartitions(buckets, existingMapping)

		// Verify perfect distribution
		assert.Equal(t, 6, len(buckets["worker-1"].Partitions))
		assert.Equal(t, 6, len(buckets["worker-2"].Partitions))

		// Verify both at capacity
		assert.Equal(t, buckets["worker-1"].MaxPartitionsAllowed, len(buckets["worker-1"].Partitions))
		assert.Equal(t, buckets["worker-2"].MaxPartitionsAllowed, len(buckets["worker-2"].Partitions))
	})

	// Test 9: All partitions unassigned (fresh start)
	t.Run("all partitions unassigned - fresh start", func(t *testing.T) {
		buckets := map[string]*algorithmV1Bucket{
			"worker-1": {OwnerId: "worker-1", MaxPartitionsAllowed: 4, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-2": {OwnerId: "worker-2", MaxPartitionsAllowed: 3, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-3": {OwnerId: "worker-3", MaxPartitionsAllowed: 3, Partitions: []algorithmV1OwnerPartitionMapping{}},
		}

		existingMapping := make(map[int]algorithmV1OwnerPartitionMapping)
		for i := 0; i < 10; i++ {
			existingMapping[i] = algorithmV1OwnerPartitionMapping{
				OwnerId:   "",
				Status:    databaseCommon.PartitionAssignmentStatusUnassigned,
				Partition: i,
			}
		}

		d.assignPartitions(buckets, existingMapping)

		// Verify all 10 partitions assigned
		total := len(buckets["worker-1"].Partitions) + len(buckets["worker-2"].Partitions) + len(buckets["worker-3"].Partitions)
		assert.Equal(t, 10, total, "all 10 partitions should be assigned")

		// Verify capacity limits
		assert.Equal(t, 4, len(buckets["worker-1"].Partitions))
		assert.Equal(t, 3, len(buckets["worker-2"].Partitions))
		assert.Equal(t, 3, len(buckets["worker-3"].Partitions))
	})

	// Test 10: Single bucket gets all
	t.Run("single bucket gets all", func(t *testing.T) {
		buckets := map[string]*algorithmV1Bucket{
			"worker-1": {OwnerId: "worker-1", MaxPartitionsAllowed: 15, Partitions: []algorithmV1OwnerPartitionMapping{}},
		}

		existingMapping := make(map[int]algorithmV1OwnerPartitionMapping)
		for i := 0; i < 15; i++ {
			status := databaseCommon.PartitionAssignmentStatusUnassigned
			owner := ""
			if i < 8 {
				status = databaseCommon.PartitionAssignmentStatusAssigned
				owner = "worker-1"
			}
			existingMapping[i] = algorithmV1OwnerPartitionMapping{
				OwnerId:   owner,
				Status:    status,
				Partition: i,
			}
		}

		d.assignPartitions(buckets, existingMapping)

		// Verify all 15 partitions go to single worker
		assert.Equal(t, 15, len(buckets["worker-1"].Partitions))
		assert.Equal(t, buckets["worker-1"].MaxPartitionsAllowed, len(buckets["worker-1"].Partitions))
	})

	// Test 11: Zero partitions edge case
	t.Run("zero partitions edge case", func(t *testing.T) {
		buckets := map[string]*algorithmV1Bucket{
			"worker-1": {OwnerId: "worker-1", MaxPartitionsAllowed: 10, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-2": {OwnerId: "worker-2", MaxPartitionsAllowed: 10, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-3": {OwnerId: "worker-3", MaxPartitionsAllowed: 10, Partitions: []algorithmV1OwnerPartitionMapping{}},
		}

		existingMapping := map[int]algorithmV1OwnerPartitionMapping{}

		d.assignPartitions(buckets, existingMapping)

		// Verify all buckets remain empty
		assert.Equal(t, 0, len(buckets["worker-1"].Partitions))
		assert.Equal(t, 0, len(buckets["worker-2"].Partitions))
		assert.Equal(t, 0, len(buckets["worker-3"].Partitions))
	})

	// Test 12: Sticky assignment exceeds new capacity
	t.Run("sticky assignment exceeds new capacity", func(t *testing.T) {
		buckets := map[string]*algorithmV1Bucket{
			"worker-1": {OwnerId: "worker-1", MaxPartitionsAllowed: 3, Partitions: []algorithmV1OwnerPartitionMapping{}}, // reduced from 10
			"worker-2": {OwnerId: "worker-2", MaxPartitionsAllowed: 4, Partitions: []algorithmV1OwnerPartitionMapping{}},
			"worker-3": {OwnerId: "worker-3", MaxPartitionsAllowed: 3, Partitions: []algorithmV1OwnerPartitionMapping{}},
		}

		existingMapping := map[int]algorithmV1OwnerPartitionMapping{
			0: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 0},
			1: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 1},
			2: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 2},
			3: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 3},
			4: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 4},
			5: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 5},
			6: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 6},
			7: {OwnerId: "worker-1", Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: 7},
		}

		d.assignPartitions(buckets, existingMapping)

		// Verify worker-1 only gets 3 (capacity limit), rest redistributed
		assert.Equal(t, 3, len(buckets["worker-1"].Partitions), "worker-1 capacity limited to 3")
		assert.True(t, len(buckets["worker-1"].Partitions) <= buckets["worker-1"].MaxPartitionsAllowed)

		// Verify total partitions assigned equals input
		total := len(buckets["worker-1"].Partitions) + len(buckets["worker-2"].Partitions) + len(buckets["worker-3"].Partitions)
		assert.Equal(t, 8, total, "all 8 partitions should be assigned")

		// Verify other workers picked up overflow
		assert.True(t, len(buckets["worker-2"].Partitions) > 0, "worker-2 should have partitions")
		assert.True(t, len(buckets["worker-3"].Partitions) > 0, "worker-3 should have partitions")
	})
}

func TestCapacityAllocation(t *testing.T) {
	d := distributorStrategyV1Impl{}

	// Test 1: Equal distribution - partitions match buckets (4 partitions, 4 buckets = 1 each)
	t.Run("equal distribution - partitions match buckets", func(t *testing.T) {
		bucket := d.buildBucket([]string{"1", "2", "3", "4"}, 4)
		assert.Equal(t, 4, len(bucket))
		assert.Equal(t, 1, bucket["1"].MaxPartitionsAllowed)
		assert.Equal(t, 1, bucket["2"].MaxPartitionsAllowed)
		assert.Equal(t, 1, bucket["3"].MaxPartitionsAllowed)
		assert.Equal(t, 1, bucket["4"].MaxPartitionsAllowed)
	})

	// Test 2: More partitions than buckets with overflow
	// 20 partitions, 6 buckets: base=3, remainder=2
	// Expected: first 2 buckets get 4, remaining 4 buckets get 3 each
	// 4+4+3+3+3+3 = 20
	t.Run("more partitions than buckets with overflow", func(t *testing.T) {
		bucket := d.buildBucket([]string{"1", "2", "3", "4", "5", "6"}, 20)
		assert.Equal(t, 6, len(bucket))

		// First 2 buckets should get 4 partitions (base + 1)
		assert.Equal(t, 4, bucket["1"].MaxPartitionsAllowed, "bucket 1 should have 4 partitions")
		assert.Equal(t, 4, bucket["2"].MaxPartitionsAllowed, "bucket 2 should have 4 partitions")

		// Remaining 4 buckets should get 3 partitions (base)
		assert.Equal(t, 3, bucket["3"].MaxPartitionsAllowed, "bucket 3 should have 3 partitions")
		assert.Equal(t, 3, bucket["4"].MaxPartitionsAllowed, "bucket 4 should have 3 partitions")
		assert.Equal(t, 3, bucket["5"].MaxPartitionsAllowed, "bucket 5 should have 3 partitions")
		assert.Equal(t, 3, bucket["6"].MaxPartitionsAllowed, "bucket 6 should have 3 partitions")

		// Verify total allocation
		total := 0
		for _, b := range bucket {
			total += b.MaxPartitionsAllowed
		}
		assert.Equal(t, 20, total, "total partitions should equal 20")
	})

	// Test 3: Fewer partitions than buckets
	// 3 partitions, 5 buckets: first 3 buckets get 1, last 2 get 0
	t.Run("fewer partitions than buckets", func(t *testing.T) {
		bucket := d.buildBucket([]string{"1", "2", "3", "4", "5"}, 3)
		assert.Equal(t, 5, len(bucket))

		// First 3 buckets get 1 partition each
		assert.Equal(t, 1, bucket["1"].MaxPartitionsAllowed, "bucket 1 should have 1 partition")
		assert.Equal(t, 1, bucket["2"].MaxPartitionsAllowed, "bucket 2 should have 1 partition")
		assert.Equal(t, 1, bucket["3"].MaxPartitionsAllowed, "bucket 3 should have 1 partition")

		// Remaining buckets get 0
		assert.Equal(t, 0, bucket["4"].MaxPartitionsAllowed, "bucket 4 should have 0 partitions")
		assert.Equal(t, 0, bucket["5"].MaxPartitionsAllowed, "bucket 5 should have 0 partitions")

		// Verify total allocation
		total := 0
		for _, b := range bucket {
			total += b.MaxPartitionsAllowed
		}
		assert.Equal(t, 3, total, "total partitions should equal 3")
	})

	// Test 4: Large partition count with small overflow
	// 10 partitions, 3 buckets: base=3, remainder=1
	// Expected: first bucket gets 4, remaining 2 get 3 each
	// 4+3+3 = 10
	t.Run("large partition count with small overflow", func(t *testing.T) {
		bucket := d.buildBucket([]string{"1", "2", "3"}, 10)
		assert.Equal(t, 3, len(bucket))

		assert.Equal(t, 4, bucket["1"].MaxPartitionsAllowed, "bucket 1 should have 4 partitions")
		assert.Equal(t, 3, bucket["2"].MaxPartitionsAllowed, "bucket 2 should have 3 partitions")
		assert.Equal(t, 3, bucket["3"].MaxPartitionsAllowed, "bucket 3 should have 3 partitions")

		// Verify total allocation
		total := 0
		for _, b := range bucket {
			total += b.MaxPartitionsAllowed
		}
		assert.Equal(t, 10, total, "total partitions should equal 10")
	})

	// Test 5: Single bucket gets all partitions
	t.Run("single bucket gets all partitions", func(t *testing.T) {
		bucket := d.buildBucket([]string{"1"}, 15)
		assert.Equal(t, 1, len(bucket))
		assert.Equal(t, 15, bucket["1"].MaxPartitionsAllowed, "single bucket should have all 15 partitions")
	})

	// Test 6: Zero partitions
	t.Run("zero partitions", func(t *testing.T) {
		bucket := d.buildBucket([]string{"1", "2", "3"}, 0)
		assert.Equal(t, 3, len(bucket))
		assert.Equal(t, 0, bucket["1"].MaxPartitionsAllowed)
		assert.Equal(t, 0, bucket["2"].MaxPartitionsAllowed)
		assert.Equal(t, 0, bucket["3"].MaxPartitionsAllowed)
	})

	// Test 7: Exact multiple (no overflow)
	// 12 partitions, 4 buckets: base=3, remainder=0
	// Expected: all buckets get 3 each
	t.Run("exact multiple - no overflow", func(t *testing.T) {
		bucket := d.buildBucket([]string{"1", "2", "3", "4"}, 12)
		assert.Equal(t, 4, len(bucket))

		assert.Equal(t, 3, bucket["1"].MaxPartitionsAllowed)
		assert.Equal(t, 3, bucket["2"].MaxPartitionsAllowed)
		assert.Equal(t, 3, bucket["3"].MaxPartitionsAllowed)
		assert.Equal(t, 3, bucket["4"].MaxPartitionsAllowed)

		// Verify total allocation
		total := 0
		for _, b := range bucket {
			total += b.MaxPartitionsAllowed
		}
		assert.Equal(t, 12, total, "total partitions should equal 12")
	})
}
