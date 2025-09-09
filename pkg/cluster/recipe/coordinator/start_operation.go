package coordinator

import (
	"context"
	"github.com/devlibx/gox-base/v2/errors"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	"time"
)

func (c *coordinatorImpl) Start(ctx context.Context) error {
	return nil
}

func (c *coordinatorImpl) performPartitionAllocation(ctx context.Context) error {

	// Make sure we are coordinator
	if isCoordinator := c.clusterManager.BecomeClusterCoordinator(ctx); !isCoordinator.Acquired {
		c.Sleep(5 * time.Second)
		return errors.New("not leader so avoid partition allocation")
	}

	domainsAndTasks, err := c.clusterDbInterface.GetAllDomainsAndTaskListsByClusterCname(ctx, c.clusterManager.GetClusterName())
	if err != nil {
		return errors.Wrap(err, "fail to get all domains and tasks for cluster %s", c.clusterManager.GetClusterName())
	}

	for _, domainAndTask := range domainsAndTasks {
		_, err := c.allocationManager.CalculateAllocation(ctx, managment.TaskListInfo{
			Cluster:        c.clusterManager.GetClusterName(),
			Domain:         domainAndTask.Domain,
			TaskList:       domainAndTask.Tasklist,
			PartitionCount: int(domainAndTask.PartitionCount),
		})
		if err != nil {
			return errors.Wrap(err, "fail to calculate allocation for domain %s", domainAndTask.Domain)
		}
	}

	return nil
}
