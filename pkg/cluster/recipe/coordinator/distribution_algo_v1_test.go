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
