package coordinator

import (
	"context"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
)

type DistributorStrategy interface {
	Distribute(ctx context.Context, request DistributionRequest) (*DistributionResponse, error)
}

type DistributionRequest struct {
	DomainName string
	TaskList   string
}

type DistributionResponse struct {
	DomainName string
	TaskList   string
	Mapping    map[string]DistributionMapping
}

type DistributionMapping struct {
	Partition int
	Status    databaseCommon.PartitionStatus
}

//go:generate mockgen -source=distribution_algo.go -destination=distribution_algo_mock.go -package=coordinator
type WorkerService interface {
	GetActiveWorkers(ctx context.Context, domain string) ([]string, error)
}
