package coordinator

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
)

type distributorStrategyV1Impl struct {
	gox.CrossFunction
	WorkerService WorkerService
}

func (d distributorStrategyV1Impl) Distribute(ctx context.Context, request DistributionRequest) (*DistributionResponse, error) {

	// Get all workers which can take partitions
	activeWorkersToAssignPartitions, err := d.WorkerService.GetActiveWorkers(ctx, request.DomainName)
	if err != nil {
		return nil, errors.Wrap(err, "get active workers failed for domain "+request.DomainName)
	}
	_ = activeWorkersToAssignPartitions

	return nil, nil
}

func NewDistributorStrategy(
	cf gox.CrossFunction,
	WorkerService WorkerService,
) (DistributorStrategy, error) {
	d := &distributorStrategyV1Impl{
		CrossFunction: cf,
		WorkerService: WorkerService,
	}
	return d, nil
}
