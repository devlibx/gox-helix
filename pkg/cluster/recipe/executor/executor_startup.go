package executor

import (
	"context"
	"database/sql"
	"github.com/devlibx/gox-base/v2/errors"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	helixDomainMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/domain/database"
	helixWorkerMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
	"github.com/devlibx/gox-helix/pkg/common/config"
	"log/slog"
)

func (s *serviceImpl) Start(ctx context.Context) error {
	for _, domainCfg := range s.domainConfigs.Domains {
		if err := s.setupDomainOnStart(ctx, domainCfg); err != nil {
			return err
		}
	}
	return nil
}

func (s *serviceImpl) setupDomainOnStart(ctx context.Context, domain *config.Domain) error {
	if domain.Disabled == true {
		return nil
	}
	if err := s.setupDomainTasklistsOnStart(ctx, domain); err != nil {
		return err
	}
	if err := s.registerDomainWorkerOnStart(ctx, domain); err != nil {
		return err
	}
	if err := s.startPartitionDistributorOnStart(ctx, domain); err != nil {
		return err
	}
	return nil
}

func (s *serviceImpl) setupDomainTasklistsOnStart(ctx context.Context, domain *config.Domain) error {
	for tasklistName, taskList := range domain.TaskLists {
		if taskList.Disabled {
			continue
		}
		if err := s.domainDataLayer.UpsertTasklist(ctx, helixDomainMysql.UpsertTasklistParams{
			Domain:         domain.Name,
			Tasklist:       tasklistName,
			Metadata:       sql.NullString{Valid: true, String: `{}`},
			PartitionCount: uint32(taskList.PartitionCount),
		}); err != nil {
			return errors.Wrap(err, "failed to upsert tasklist: domain=%s, tasklist=%s", domain.Name, tasklistName)
		}
	}
	return nil
}

func (s *serviceImpl) registerDomainWorkerOnStart(ctx context.Context, domain *config.Domain) error {
	if err := s.workerDataLayer.RegisterWorker(ctx, helixWorkerMysql.RegisterWorkerParams{
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
		go func(domain *config.Domain, tasklist *config.TaskList) {
			err := s.PartitionDistributionService.Process(ctx, coordinator.DistributionRequest{
				DomainName: domain.Name,
				TaskList:   tasklist.Name,
			})
			if err != nil {
				slog.Error("failed to start partition distributor: domain=%s, tasklist=%s", domain.Name, tasklist.Name)
			}
		}(domain, tl)
	}
	return nil
}
