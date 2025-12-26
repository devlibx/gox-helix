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
	OwnerID string                      `json:"owner_id"`
	Mapping map[int]DistributionMapping `db:"mapping"`
}

type PartitionService interface {
	GetActivePartitionMappings(ctx context.Context, domain string, tasklist string) ([]WorkerPartitionMapping, error)
	PersistDistribution(ctx context.Context, domain string, tasklist string, response *DistributionResponse) error
}

type DomainService interface {
	GetTaskListInfo(ctx context.Context, domain string, tasklist string) (*helixDomainMysql.HelixDomain, error)
}

type DistributorStrategy interface {
	Distribute(ctx context.Context, request DistributionRequest) (*DistributionResponse, error)
}
