package coordinator

import (
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
)

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
	OwnerId string
	Status  databaseCommon.PartitionAssignmentStatus `yaml:"status"`
}
