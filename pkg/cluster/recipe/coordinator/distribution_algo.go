package coordinator

import (
	"encoding/json"
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

func (d *DistributionResponse) GetMappings() map[string][]int {
	mappings := make(map[string][]int)
	for partitionId, mapping := range d.Mapping {
		if mapping.OwnerId != "" {
			mappings[mapping.OwnerId] = append(mappings[mapping.OwnerId], partitionId)
		}
	}
	return mappings
}

func (d *DistributionResponse) GetMappingsAsString() map[string][]byte {
	mappingsToReturn := make(map[string][]byte)
	for k, partitionIds := range d.GetMappings() {
		if metadataJson, err := json.Marshal(partitionIds); err != nil {
			mappingsToReturn[k] = []byte("[]")
		} else {
			mappingsToReturn[k] = metadataJson
		}
	}
	return mappingsToReturn
}

type DistributionMapping struct {
	OwnerId string                                   `json:"owner_id"`
	Status  databaseCommon.PartitionAssignmentStatus `yaml:"status"`
}
