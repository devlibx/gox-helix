package executor

import (
	"context"
	"github.com/devlibx/gox-base/v2/errors"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	helixWorkerMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
	"github.com/devlibx/gox-helix/pkg/common/config"
)

func (s *serviceImpl) Start(ctx context.Context) error {

	// Make sure domains are registered
	if err := s.domainService.Start(ctx); err != nil {
		return err
	}

	// Start the worker monitor
	if err := s.workerMonitor.Start(ctx); err != nil {
		return errors.Wrap(err, "failed to start worker monitor")
	}

	// Register worker and start partition distributors
	for _, domainCfg := range s.domainConfigs.Domains {
		if domainCfg.Disabled == true {
			continue
		}
		if err := s.registerDomainWorkerOnStart(ctx, domainCfg); err != nil {
			return err
		}
		if err := s.startPartitionDistributorOnStart(ctx, domainCfg); err != nil {
			return err
		}
		if err := s.startTasklistProcessorAndCallingClientWorkFunction(ctx, domainCfg); err != nil {
			return err
		}
	}
	return nil
}

func (s *serviceImpl) registerDomainWorkerOnStart(ctx context.Context, domain *config.Domain) error {
	if err := s.workerDataLayer.Querier.RegisterWorker(ctx, helixWorkerMysql.RegisterWorkerParams{
		WorkerID:        s.workerId,
		Domain:          domain.Name,
		CreatedAt:       s.Now(),
		LastHeartbeatAt: s.Now(),
	}); err != nil {
		return errors.Wrap(err, "failed to register worker: domain=%s", domain.Name)
	}
	return nil
}

func (s *serviceImpl) startPartitionDistributorOnStart(ctx context.Context, domain *config.Domain) error {
	for _, tl := range domain.TaskLists {
		if tl.Disabled {
			continue
		}
		go func(domain *config.Domain, tasklist *config.TaskList) {
			err := s.PartitionDistributionService.Process(ctx, coordinator.DistributionRequest{
				DomainName: domain.Name,
				TaskList:   tasklist.Name,
			})
			if err != nil {
				s.logger.Error("failed to start partition distributor: domain=%s, tasklist=%s", domain.Name, tasklist.Name)
			}
		}(domain, tl)
	}
	return nil
}
