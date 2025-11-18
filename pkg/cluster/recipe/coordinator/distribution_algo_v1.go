package coordinator

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
)

type distributorStrategyV1Impl struct {
	gox.CrossFunction
	ws WorkerService
	ps PartitionService
	ds DomainService
}

func (d distributorStrategyV1Impl) Distribute(ctx context.Context, request DistributionRequest) (*DistributionResponse, error) {

	// Get all workers which can take partitions
	activeWorkersToAssignPartitions, err := d.ws.GetActiveWorkers(ctx, request.DomainName)
	if err != nil {
		return nil, errors.Wrap(err, "get active workers failed for domain "+request.DomainName)
	}

	activePartitionMapping, err := d.ps.GetActivePartitionMappings(ctx, request.DomainName, request.TaskList)
	if err != nil {
		return nil, errors.Wrap(err, "get active partition mappings failed for domain "+request.DomainName)
	}

	taskListToHandle, err := d.ds.GetTaskListInfo(ctx, request.DomainName, request.TaskList)
	if err != nil {
		return nil, errors.Wrap(err, "get task list info failed for domain "+request.DomainName)
	}
	partitionCount := int(taskListToHandle.PartitionCount)

	// Step 1 = Build what is the current mapping of existing partition
	existingPartitionDistribution := d.buildExisting(activePartitionMapping, partitionCount)

	resultMapping := map[string]*algorithmV1Bucket{}
	tempAlgorithmV1Bucket := make([]*algorithmV1Bucket, 0)
	for _, ownerId := range activeWorkersToAssignPartitions {
		resultMapping[ownerId] = &algorithmV1Bucket{MaxPartitionsAllowed: 0}
		tempAlgorithmV1Bucket = append(tempAlgorithmV1Bucket, resultMapping[ownerId])
	}

	for i := 0; i < partitionCount; i++ {
		idx := i % len(tempAlgorithmV1Bucket)
		tempAlgorithmV1Bucket[idx].MaxPartitionsAllowed = tempAlgorithmV1Bucket[idx].MaxPartitionsAllowed + 1
	}

	// Step 2 - assign partitions to existing owners (sticky)
	for partitionId, n := range existingPartitionDistribution {
		if _, ok := resultMapping[n.OwnerId]; !ok {
			resultMapping[n.OwnerId] = &algorithmV1Bucket{MaxPartitionsAllowed: 10}
		}
		assigned := resultMapping[n.OwnerId].TryAssign(n)
		if assigned {
			delete(existingPartitionDistribution, partitionId)
		}
	}

	_, _, _, _ = activePartitionMapping, activeWorkersToAssignPartitions, taskListToHandle, partitionCount
	return nil, nil
}

// buildExisting builds what is the current mapping of existing partitions
// Key=partition no, status=existing status, and owner=what is current owner
// If we have added new partitions or of we have missing partition, it will add unsigned records so
// that we can add them and use them in algo
func (d distributorStrategyV1Impl) buildExisting(activePartitionMapping []WorkerPartitionMapping, partitionCount int) map[int]algorithmV1OwnerPartitionMapping {

	existingPartitionDistribution := map[int]algorithmV1OwnerPartitionMapping{}
	for _, apm := range activePartitionMapping {
		for k, v := range apm.Mapping {
			existingPartitionDistribution[k] = algorithmV1OwnerPartitionMapping{OwnerId: apm.OwnerID, Status: v.Status, Partition: k}
		}
	}

	// Step 1.1 = If partition count is increased then make them unassigned
	for i := 0; i < partitionCount; i++ {
		if _, ok := existingPartitionDistribution[i]; !ok {
			existingPartitionDistribution[i] = algorithmV1OwnerPartitionMapping{OwnerId: "", Status: databaseCommon.PartitionAssignmentStatusUnassigned, Partition: i}
		}
	}

	return existingPartitionDistribution
}

func (d distributorStrategyV1Impl) buildBucket(workersOwnerIds []string, partitionCount int) map[string]*algorithmV1Bucket {
	resultMapping := map[string]*algorithmV1Bucket{}
	tempAlgorithmV1Bucket := make([]*algorithmV1Bucket, 0)

	// Make empty with MaxPartitionsAllowed = 0
	for _, ownerId := range workersOwnerIds {
		resultMapping[ownerId] = &algorithmV1Bucket{MaxPartitionsAllowed: 0}
		tempAlgorithmV1Bucket = append(tempAlgorithmV1Bucket, resultMapping[ownerId])
	}

	// Assign them equally
	for i := 0; i < partitionCount; i++ {
		idx := i % len(tempAlgorithmV1Bucket)
		tempAlgorithmV1Bucket[idx].MaxPartitionsAllowed = tempAlgorithmV1Bucket[idx].MaxPartitionsAllowed + 1
	}

	return resultMapping
}

type algorithmV1OwnerPartitionMapping struct {
	Status    databaseCommon.PartitionAssignmentStatus
	OwnerId   string
	Partition int
}

type algorithmV1Bucket struct {
	OwnerId              string
	MaxPartitionsAllowed int
	Partitions           []algorithmV1OwnerPartitionMapping
}

func (a *algorithmV1Bucket) TryAssign(m algorithmV1OwnerPartitionMapping) bool {

	// Ignore if it is not in assigned state
	if m.Status != databaseCommon.PartitionAssignmentStatusAssigned {
		return false
	}

	if a.Partitions == nil || len(a.Partitions) == 0 {
		a.Partitions = make([]algorithmV1OwnerPartitionMapping, 0)
	}

	currentPartitions := len(a.Partitions)
	if currentPartitions >= a.MaxPartitionsAllowed {
		return false
	}

	a.Partitions = append(a.Partitions, algorithmV1OwnerPartitionMapping{OwnerId: m.OwnerId, Status: databaseCommon.PartitionAssignmentStatusAssigned, Partition: m.Partition})
	return true
}

func (a *algorithmV1Bucket) getByPartitionId(i int) algorithmV1OwnerPartitionMapping {
	for _, p := range a.Partitions {
		if p.Partition == i {
			return p
		}
	}
	return algorithmV1OwnerPartitionMapping{}
}

func NewDistributorStrategy(
	cf gox.CrossFunction,
	ws WorkerService,
	ps PartitionService,
	ds DomainService,
) (DistributorStrategy, error) {
	d := &distributorStrategyV1Impl{
		CrossFunction: cf,
		ws:            ws,
		ps:            ps,
		ds:            ds,
	}
	return d, nil
}
