package coordinator

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
)

type distributorStrategyV1Impl struct {
	gox.CrossFunction
	ws WorkerService
	ps PartitionService
	ds DomainService
}

func (d distributorStrategyV1Impl) Distribute(ctx context.Context, request DistributionRequest) (*DistributionResponse, error) {

	// Get all workers which can take partitions
	activeWorkersToAssignPartitions, err := d.ws.GetActiveWorkers(ctx, request.DomainName)
	if err != nil {
		return nil, errors.Wrap(err, "get active workers failed for domain "+request.DomainName)
	}

	activePartitionMapping, err := d.ps.GetActivePartitionMappings(ctx, request.DomainName, request.TaskList)
	if err != nil {
		return nil, errors.Wrap(err, "get active partition mappings failed for domain "+request.DomainName)
	}

	taskListToHandle, err := d.ds.GetTaskListInfo(ctx, request.DomainName, request.TaskList)
	if err != nil {
		return nil, errors.Wrap(err, "get task list info failed for domain "+request.DomainName)
	}

	_, _, _ = activePartitionMapping, activeWorkersToAssignPartitions, taskListToHandle
	return nil, nil
}

func NewDistributorStrategy(
	cf gox.CrossFunction,
	ws WorkerService,
	ps PartitionService,
	ds DomainService,
) (DistributorStrategy, error) {
	d := &distributorStrategyV1Impl{
		CrossFunction: cf,
		ws:            ws,
		ps:            ps,
		ds:            ds,
	}
	return d, nil
}
