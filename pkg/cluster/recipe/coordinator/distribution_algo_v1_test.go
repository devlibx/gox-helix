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
						0: {Status: databaseCommon.PartitionStatusActive},
						1: {Status: databaseCommon.PartitionStatusActive},
						2: {Status: databaseCommon.PartitionStatusActive},
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
