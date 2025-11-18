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
