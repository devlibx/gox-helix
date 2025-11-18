package coordinator

import (
	"context"
	helixDomainMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/domain/database"
)

//go:generate mockgen -source=objects.go -destination=mocks.go -package=coordinator

type WorkerService interface {
	GetActiveWorkers(ctx context.Context, domain string) ([]string, error)
}

type WorkerPartitionMapping struct {
	OwnerID string
	Mapping map[int]DistributionMapping
}

type PartitionService interface {
	GetActivePartitionMappings(ctx context.Context, domain string, tasklist string) ([]WorkerPartitionMapping, error)
}

type DomainService interface {
	GetTaskListInfo(ctx context.Context, domain string, tasklist string) (*helixDomainMysql.HelixDomain, error)
}
