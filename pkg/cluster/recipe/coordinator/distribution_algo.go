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
	Mapping    map[int]DistributionMapping
}

type DistributionMapping struct {
	Status databaseCommon.PartitionAssignmentStatus `yaml:"status"`
}
