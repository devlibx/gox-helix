package managment

import (
	"context"
	"github.com/devlibx/gox-base/v2"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	"log/slog"
	"time"
)

type Node struct {
	gox.CrossFunction
	Cluster string `json:"cluster" yaml:"cluster"`
	Id      string `json:"id" yaml:"id"`
	Status  int8
}

func (c *Node) StartHeartbeat(ctx context.Context, hbFunction func(ctx context.Context, node *Node)) {
	for {
		if c.Status != helixClusterMysql.NodeStatusActive {
			slog.Info("Node - stopping heartbeat", slog.String("clusterId", c.Id), slog.String("nodeId", c.Id))
			break
		}
		c.Sleep(10 * time.Second)
		hbFunction(context.Background(), c)
	}
}
