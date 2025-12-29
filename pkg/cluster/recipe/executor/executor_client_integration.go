package executor

import (
	"context"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator/processor"
	"github.com/devlibx/gox-helix/pkg/common/config"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	"time"
)

func (s *serviceImpl) startTasklistProcessorAndCallingClientWorkFunction(ctx context.Context, domain *config.Domain) error {
	for _, tl := range domain.TaskLists {
		go func(tasklist *config.TaskList) {
			s.runTaskProcessor(ctx, domain, tasklist)
		}(tl)
	}
	return nil
}

func (s *serviceImpl) runTaskProcessor(ctx context.Context, domain *config.Domain, tasklist *config.TaskList) {

	regularTickerToCheckPartitions := time.NewTicker(1 * time.Second)

	for {
		select {
		case <-ctx.Done():
			goto exit

		case <-regularTickerToCheckPartitions.C:
			// Step 1 - Find partitions to work on
			workerPartitionMapping, err := s.partitionService.GetValidPartitionByOwnerId(
				ctx,
				domain.Name,
				tasklist.Name,
				s.workerId,
			)
			if err != nil {
				s.logger.Info("failed to get valid partition mapping for to run task processor", "domain", domain.Name, "tasklist", tasklist.Name, "err", err.Error())
				continue
			}

			// We got the new partitions to process
			partitions := make([]int, 0)
			for p, v := range workerPartitionMapping.Mapping {
				if v.Status == databaseCommon.PartitionAssignmentStatusAssigned {
					partitions = append(partitions, p)
				}
			}
			s.logger.Debug("task processor has assigned partitions", "domain", domain.Name, "tasklist", tasklist.Name, "partitions", partitions)

			if domainTasklistProcessor, err := s.ProcessorFactory.GetOrCreateDomainTasklistProcessor(
				ctx,
				processor.CreateDomainTasklistProcessorRequest{
					Domain:                    domain.Name,
					TaskList:                  tasklist.Name,
					WorkerId:                  s.workerId,
					ClientFunctionProcessWork: s.ClientFunctionProcessWork,
				},
			); err != nil {
				s.logger.Info("failed to get or create task list processor to process partitions", "domain", domain.Name, "tasklist", tasklist.Name, "partitions", partitions, "err", err.Error())
			} else {
				if _, err := domainTasklistProcessor.Process(ctx, coordinator.DomainTasklistProcessRequest{Partitions: partitions}); err != nil {
					s.logger.Info("failed to run the processor to process partitions using takes processor", "domain", domain.Name, "tasklist", tasklist.Name, "partitions", partitions, "err", err.Error())
				}
			}
		}
	}
exit:
	s.logger.Info("[SHUTDOWN] stopped task processing for", "domain", domain.Name, "tasklist", tasklist.Name)
}
