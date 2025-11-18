package coordinator

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/stretchr/testify/assert"
	"go.uber.org/mock/gomock"
	"testing"
)

func TestDistributorStrategyV1Impl(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockWorkerService := NewMockWorkerService(ctrl)

	d := distributorStrategyV1Impl{
		CrossFunction: gox.NewCrossFunction(),
		WorkerService: mockWorkerService,
	}

	mockWorkerService.EXPECT().GetActiveWorkers(gomock.Any(), gomock.Any()).Return([]string{"node-1", "node-2"}, nil)

	distributionResponse, err := d.Distribute(context.Background(), DistributionRequest{DomainName: "test", TaskList: "test"})
	assert.NoError(t, err)
	// assert.Equal(t, []string{"node-1", "node-2"}, distributionResponse.TaskList)
	_ = distributionResponse
}
