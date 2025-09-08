package managment

import (
	"context"
	"database/sql"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	"github.com/devlibx/gox-helix/pkg/common/lock"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"log/slog"
	"sync"
	"time"
)

type clusterManagerImpl struct {
	gox.CrossFunction

	clusterManagerConfig *ClusterManagerConfig

	// database access
	dbInterface *helixClusterMysql.Queries
	locker      lock.Locker

	Name string

	// Node information
	nodes     map[string]*Node
	nodeMutex *sync.RWMutex

	// internal
	clusterId string
	shutdown  bool
}

func NewClusterManager(
	cf gox.CrossFunction,
	config *ClusterManagerConfig,
	dbInterface *helixClusterMysql.Queries,
	locker lock.Locker,
) (ClusterManager, error) {

	cm := &clusterManagerImpl{
		CrossFunction:        cf,
		Name:                 config.Name,
		clusterManagerConfig: config,
		dbInterface:          dbInterface,
		nodeMutex:            &sync.RWMutex{},
		nodes:                map[string]*Node{},
		locker:               locker,
		clusterId:            uuid.New().String(),
	}

	// Start inactive node clean-up worker thread (mark nodes inactive if they have not give successful HB)
	go func() {
		cm.removeInactiveNodes(context.Background())
	}()

	return cm, nil
}

func (c *clusterManagerImpl) RegisterNode(ctx context.Context, request NodeRegisterRequest) (*NodeRegisterResponse, error) {
	now := c.Now()
	nodeId := uuid.NewString()

	err := c.dbInterface.UpsertNode(ctx, helixClusterMysql.UpsertNodeParams{
		ClusterName:  request.Cluster,
		NodeUuid:     nodeId,
		NodeMetadata: sql.NullString{Valid: true, String: "{}"},
		LastHbTime:   now,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to add node to cluster %s", request.Cluster)
	}

	// Take lock and add this to nodes map
	c.nodeMutex.Lock()
	defer c.nodeMutex.Unlock()
	c.nodes[nodeId] = &Node{
		CrossFunction: c.CrossFunction,
		Cluster:       request.Cluster,
		Id:            nodeId,
		Status:        helixClusterMysql.NodeStatusActive,
	}

	// Start
	go func() {
		c.startHeartbeatForNode(ctx, c.nodes[nodeId])
	}()

	return &NodeRegisterResponse{NodeId: nodeId}, nil
}

func (c *clusterManagerImpl) GetActiveNodes(ctx context.Context) ([]Node, error) {

	// Get all active nodes from DB
	nodes, err := c.dbInterface.GetActiveNodes(ctx, c.Name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get active nodes: cluster=%s", c.Name)
	}

	// Build result and return to caller
	toRet := make([]Node, 0, len(nodes))
	for _, node := range nodes {
		n := &Node{
			Cluster: node.ClusterName,
			Id:      node.NodeUuid,
			Status:  node.Status,
		}
		toRet = append(toRet, *n)
	}

	return toRet, nil
}

func (c *clusterManagerImpl) startHeartbeatForNode(ctx context.Context, node *Node) {
	node.StartHeartbeat(ctx, func(ctx context.Context, node *Node) {
		for {

			// Stop HB if we need to shut down
			if c.shutdown {
				slog.Warn("node healthcheck shutting down", slog.String("cluster", c.Name), slog.String("nodeId", node.Id))
				break
			}

			// Ensure we update the DB for each node
			currentTime := c.Now()
			if err := c.dbInterface.UpdateHeartbeat(ctx, helixClusterMysql.UpdateHeartbeatParams{
				LastHbTime:  currentTime,
				ClusterName: c.Name,
				NodeUuid:    node.Id,
			}); err != nil {
				slog.Warn("failed to update node heartbeat", slog.String("cluster", c.Name), slog.String("node", node.Id), slog.String("error", err.Error()))
			} else {
				slog.Debug("node heartbeat updated", slog.String("cluster", c.Name), slog.String("node", node.Id))
			}

			// Sleet between each HB update
			c.Sleep(3 * time.Second)
		}
	})
}

func (c *clusterManagerImpl) removeInactiveNodes(ctx context.Context) {
	for {

		// Stop if we need to shut down
		if c.shutdown {
			slog.Warn("cluster manager shutting down - stop node inactive marking job", slog.String("cluster", c.Name))
			break
		}

		// Acquire cluster controller lock to do this job
		if r, err := c.locker.Acquire(ctx, &lock.AcquireRequest{
			LockKey: "cluster-controller-" + c.Name,
			OwnerID: c.clusterId,
			TTL:     10 * time.Second,
		}); err != nil || !r.Acquired {
			slog.Debug("this node is not the cluster controller - (expected with multi node cluster) not allowed to mark nodes inactive", slog.String("cluster", c.Name))
			c.Sleep(time.Second)
			continue
		}

		// Remove inactive nodes from cluster (nodes not given HB for last 10 sec will be marked inactive)
		lastHbTime := c.Now().Add(-10 * time.Second)
		if err := c.dbInterface.MarkInactiveNodes(ctx, helixClusterMysql.MarkInactiveNodesParams{
			ClusterName: c.Name,
			LastHbTime:  lastHbTime,
		}); err != nil {
			slog.Warn("failed to mark nodes inactive (nodes where last HB was older than 10 sec)", slog.String("cluster", c.Name), slog.String("error", err.Error()))
		} else {
			slog.Debug("marked inactive nodes", slog.String("cluster", c.Name))
		}

		// Sleep for next clean-up to mark nodes inactive
		c.Sleep(1 * time.Second)
	}
}
