package managment

import (
	"context"
	"database/sql"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	"github.com/devlibx/gox-helix/pkg/common/lock"
	"time"
)

// ClusterManagerConfig holds configuration parameters for the ClusterManager.
type ClusterManagerConfig struct {
	// NodeHeartbeatInterval defines how often a node sends a heartbeat to the cluster.
	NodeHeartbeatInterval time.Duration `json:"node_heartbeat_interval" yaml:"node_heartbeat_interval"`
	// Name is the name of the cluster this manager belongs to.
	Name                  string        `json:"name" yaml:"name"`
	// ControllerTtl defines the time-to-live for the cluster controller lock.
	ControllerTtl         time.Duration `json:"controller_ttl" yaml:"controller_ttl"`
}

// ClusterManager defines the interface for managing cluster nodes and coordinating cluster-wide operations.
type ClusterManager interface {
	// GetClusterName returns the name of the cluster managed by this ClusterManager.
	GetClusterName() string

	// BecomeClusterCoordinator attempts to acquire the cluster coordinator lock.
	// It returns an AcquireResponse indicating if the lock was acquired and by whom.
	BecomeClusterCoordinator(ctx context.Context) *lock.AcquireResponse

	// RegisterNode registers a new node with the cluster.
	RegisterNode(ctx context.Context, request NodeRegisterRequest) (*NodeRegisterResponse, error)

	// GetActiveNodes retrieves a list of all currently active nodes in the cluster.
	GetActiveNodes(ctx context.Context) ([]Node, error)
}

// NodeRegisterRequest is the request payload for registering a new node.
type NodeRegisterRequest struct {
	Cluster string
}

// NodeRegisterResponse is the response payload after registering a new node.
type NodeRegisterResponse struct {
	NodeId string
}

// NewHelixDatasourceUsingSqlDb creates a new helixClusterMysql.Querier and helixClusterMysql.Queries
// from an existing *sql.DB connection. This is typically used for dependency injection.
func NewHelixDatasourceUsingSqlDb(db *sql.DB) (helixClusterMysql.Querier, *helixClusterMysql.Queries, error) {
	q, err := helixClusterMysql.Prepare(context.Background(), db)
	return q, q, err
}
