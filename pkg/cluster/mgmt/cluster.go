package managment

import (
	"context"
	"database/sql"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	"time"
)

type ClusterManagerConfig struct {
	NodeHeartbeatInterval time.Duration `json:"node_heartbeat_interval" yaml:"node_heartbeat_interval"`
	Name                  string        `json:"name" yaml:"name"`
}

type ClusterManager interface {
	RegisterNode(ctx context.Context, request NodeRegisterRequest) (*NodeRegisterResponse, error)

	GetActiveNodes(ctx context.Context) ([]Node, error)
}

type NodeRegisterRequest struct {
	Cluster string
}

type NodeRegisterResponse struct {
	NodeId string
}

func NewHelixDatasourceUsingSqlDb(db *sql.DB) (helixClusterMysql.Querier, *helixClusterMysql.Queries, error) {
	q, err := helixClusterMysql.Prepare(context.Background(), db)
	return q, q, err
}
