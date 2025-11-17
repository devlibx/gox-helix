package coordinator

import (
	"context"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/google/uuid"
	"log/slog"
	"time"
)

type PartitionDistributionRequest struct {
	DomainName string
	TaskList   string
}

type PartitionDistributionService interface {
	Process(ctx context.Context, request PartitionDistributionRequest) error
}

type PartitionDistributionServiceImpl struct {
	lockService locker.Locker
}

func (p *PartitionDistributionServiceImpl) Process(ctx context.Context, request PartitionDistributionRequest) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			goto exit
		case <-ticker.C:
			if _, err := p.lockService.AcquireLock(ctx, locker.AcquireLockRequest{
				Domain:  request.DomainName,
				LockKey: request.TaskList + "--partition-distributor-owner",
				OwnerId: uuid.NewString(),
				TTL:     10 * time.Second,
			}); err == nil {
				_ = p.internalProcess(ctx, request)
			}
		}
	}

exit:
	slog.Info("partition distributor started")
	return nil
}

func (p *PartitionDistributionServiceImpl) internalProcess(ctx context.Context, request PartitionDistributionRequest) error {
	return nil
}
