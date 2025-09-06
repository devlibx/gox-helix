package managment

import (
	"context"
	"time"
)

type ClusterManagerConfig struct {
	NodeHeartbeatInterval time.Duration `json:"node_heartbeat_interval" yaml:"node_heartbeat_interval"`
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
