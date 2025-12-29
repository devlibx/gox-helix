package executor

import (
	"context"
	"fmt"
	"github.com/devlibx/gox-base/v2/errors"
	helixWorkerMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
	"github.com/devlibx/gox-helix/pkg/common/config"
)

func (s *serviceImpl) Stop(ctx context.Context) error {
	for _, domain := range s.domainConfigs.Domains {
		if err := s.stopDomainOnStop(ctx, domain); err != nil {
			return err
		}
	}
	return nil
}

func (s *serviceImpl) stopDomainOnStop(ctx context.Context, domain *config.Domain) error {
	if err := s.workerDataLayer.DeregisterWorker(
		ctx,
		helixWorkerMysql.DeregisterWorkerParams{
			Domain:   domain.Name,
			WorkerID: s.workerId,
		},
	); err != nil {
		return errors.Wrap(err, fmt.Sprintf("[SHUTDOWN] failed to deregister worker=%s for domain=%s", s.workerId, domain.Name))
	} else {
		s.logger.Info("[SHUTDOWN] worker deregistered successfully on shoutdown", "domain", domain.Name)
	}
	return nil
}
