package coordinator

import (
	"context"
	"github.com/devlibx/gox-base/v2/errors"
	managment "github.com/devlibx/gox-helix/pkg/cluster/mgmt"
	"log/slog"
	"time"
)

func (c *coordinatorImpl) Start(ctx context.Context) error {
	clusterName := c.clusterManager.GetClusterName()
	slog.Info("Starting coordinator for cluster", slog.String("cluster", clusterName))

	// Start periodic partition allocation loop
	go func() {
		// Initial delay to allow cluster setup to complete
		c.Sleep(3 * time.Second)

		// How often we have to calculate allocation
		partitionAllocationInterval := 5 * time.Second
		if c.clusterManager.GetClusterManagerConfig().PartitionAllocationInterval > 0 {
			partitionAllocationInterval = c.NormalizeDuration(c.clusterManager.GetClusterManagerConfig().PartitionAllocationInterval)
		}

		ticker := time.NewTicker(partitionAllocationInterval) // Allocate every 5 seconds
		defer ticker.Stop()

		slog.Info("Coordinator allocation loop started", slog.String("cluster", clusterName))

		for {
			select {
			case <-ctx.Done():
				slog.Info("Coordinator stopping due to context cancellation", slog.String("cluster", clusterName))
				return
			case <-ticker.C:
				if err := c.performPartitionAllocation(ctx); err != nil {
					// Don't spam logs for leadership changes - these are expected in chaos testing
					if !isLeadershipError(err) {
						slog.Warn("Partition allocation failed",
							slog.String("cluster", clusterName),
							slog.String("error", err.Error()))
					}
				} else {
					slog.Debug("Partition allocation completed successfully",
						slog.String("cluster", clusterName))
				}
			}
		}
	}()

	return nil
}

// isLeadershipError checks if the error is related to not being coordinator
func isLeadershipError(err error) bool {
	if err == nil {
		return false
	}
	errorMsg := err.Error()
	return errorMsg == "not leader so avoid partition allocation"
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
