package coordinator

import (
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
)

type DistributionRequest struct {
	DomainName string `json:"domain"`
	TaskList   string `json:"task_list"`
}

type DistributionResponse struct {
	DomainName string                      `db:"domain"`
	TaskList   string                      `db:"task_list"`
	Mapping    map[int]DistributionMapping `db:"mapping"`
}

type DistributionMapping struct {
	OwnerId string                                   `json:"owner_id"`
	Status  databaseCommon.PartitionAssignmentStatus `yaml:"status"`
}
