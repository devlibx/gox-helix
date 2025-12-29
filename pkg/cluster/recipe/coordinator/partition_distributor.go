package coordinator

import (
	"context"
	"github.com/devlibx/gox-helix/pkg/common"
	"log/slog"
	"math/rand"
	"time"

	"github.com/devlibx/gox-base/v2/errors"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/google/uuid"
)

type PartitionDistributionService interface {
	Process(ctx context.Context, request DistributionRequest) error
}

type PartitionDistributionServiceImpl struct {
	lockService          locker.Locker
	distributor          DistributorStrategy
	partitionService     PartitionService
	applicationSingleton *common.ApplicationSingleton
	logger               *slog.Logger
}

func NewPartitionDistributionService(
	lockService locker.Locker,
	distributor DistributorStrategy,
	partitionService PartitionService,
	applicationSingleton *common.ApplicationSingleton,
) (PartitionDistributionService, error) {
	return &PartitionDistributionServiceImpl{
		lockService:          lockService,
		distributor:          distributor,
		partitionService:     partitionService,
		applicationSingleton: applicationSingleton,
		logger:               applicationSingleton.GetModuleLogger("partition_distributor"),
	}, nil
}

func (p *PartitionDistributionServiceImpl) Process(ctx context.Context, request DistributionRequest) error {
	ticker := time.NewTicker(time.Second * 1)
	defer ticker.Stop()

	p.logger.Info("partition distributor process started", "domain", request.DomainName, "tasklist", request.TaskList)
	for {
		select {
		case <-ctx.Done():
			goto exit
		case <-ticker.C:
			// Acquire a cluster-wide lock to ensure only one instance runs the distribution
			ownerId := uuid.NewString()
			lockKey := "partition-distributor-" + request.DomainName + "-" + request.TaskList
			if _, err := p.lockService.AcquireLock(ctx, locker.AcquireLockRequest{
				Domain:  request.DomainName,
				LockKey: lockKey,
				OwnerId: ownerId,
				TTL:     30 * time.Second,
			}); err == nil {
				// If lock is acquired, run the internal process
				if err = p.internalProcess(ctx, request); err != nil {
					p.logger.Error("failed to run partition distributor internal process", "err", err, "domain", request.DomainName, "tasklist", request.TaskList)
				}
			} else {
				p.logger.Debug("(expected - not all nodes will get lock) lock not acquired for partition distributor", "domain", request.DomainName, "tasklist", request.TaskList)
			}

			// Reset tick after 5-10 sec
			delay := randomDelay(5*time.Second, 10*time.Second)
			ticker.Reset(delay)
		}
	}

exit:
	p.logger.Info("[SHUTDOWN] partition distributor process stopped (context done)", "domain", request.DomainName, "tasklist", request.TaskList)
	return nil
}

func (p *PartitionDistributionServiceImpl) internalProcess(ctx context.Context, request DistributionRequest) error {
	// Step 1: Calculate the new distribution plan
	response, err := p.distributor.Distribute(ctx, request)
	if err != nil {
		return errors.Wrap(err, "failed to run distribution algorithm for domain=%s, tasklist=%s", request.DomainName, request.TaskList)
	}

	// Step 2: Atomically persist the new distribution plan to the database
	if err := p.partitionService.PersistDistribution(ctx, request.DomainName, request.TaskList, response); err != nil {
		return errors.Wrap(err, "failed to persist distribution for domain=%s, tasklist=%s", request.DomainName, request.TaskList)
	}

	p.logger.Debug("successfully completed partition distribution cycle", "domain", request.DomainName, "tasklist", request.TaskList)
	return nil
}

func randomDelay(min, max time.Duration) time.Duration {
	return min + time.Duration(rand.Int63n(int64(max-min)))
}
