package managment

import (
	"context"
	"database/sql"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/errors"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	"github.com/google/uuid"
	"log/slog"
	"sync"
	"time"
)

type clusterManagerImpl struct {
	gox.CrossFunction
	clusterManagerConfig *ClusterManagerConfig
	dbInterface          helixClusterMysql.Queries

	Name string

	NodeMutex *sync.RWMutex
	Nodes     map[string]*Node

	shutdown bool
}

func NewClusterManager(
	cf gox.CrossFunction,
	config *ClusterManagerConfig,
	dbInterface helixClusterMysql.Queries,
) (ClusterManager, error) {

	cm := &clusterManagerImpl{
		CrossFunction:        cf,
		clusterManagerConfig: config,
		dbInterface:          dbInterface,
		NodeMutex:            &sync.RWMutex{},
		Nodes:                map[string]*Node{},
	}

	// Start inactive node clean-up work
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

	c.NodeMutex.Lock()
	defer c.NodeMutex.Unlock()
	c.Nodes[nodeId] = &Node{
		Cluster: request.Cluster,
		Id:      nodeId,
	}

	go func() {
		c.startHeartbeatForNode(ctx, c.Nodes[nodeId])
	}()

	return &NodeRegisterResponse{NodeId: nodeId}, nil
}

func (c *clusterManagerImpl) GetActiveNodes(ctx context.Context) ([]Node, error) {

	nodes, err := c.dbInterface.GetActiveNodes(ctx, c.Name)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get active nodes: cluster=%s", c.Name)
	}

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
				slog.Warn("failed to update heartbeat", slog.String("cluster", c.Name), slog.String("node", node.Id), slog.String("error", err.Error()))
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
		if c.shutdown {
			slog.Warn("cluster manager shutting down - stop node inactive marking job", slog.String("cluster", c.Name))
			break
		}

		// Remove inactive nodes from cluster
		lastHbTime := c.Now().Add(-10 * time.Second)
		if err := c.dbInterface.MarkInactiveNodes(ctx, helixClusterMysql.MarkInactiveNodesParams{
			ClusterName: c.Name,
			LastHbTime:  lastHbTime,
		}); err != nil {
			slog.Warn("failed to mark inactive nodes", slog.String("cluster", c.Name), slog.String("error", err.Error()))
		} else {
			slog.Debug("marked inactive nodes", slog.String("cluster", c.Name))
		}

		// Sleep for next clean-up
		c.Sleep(1 * time.Second)
	}
}
