package managment

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	helixClusterMysql "github.com/devlibx/gox-helix/pkg/cluster/mysql/database"
	"github.com/devlibx/gox-helix/pkg/common/lock"
	helixLock "github.com/devlibx/gox-helix/pkg/common/lock/mysql"
	"github.com/devlibx/gox-helix/pkg/util"
	"github.com/google/uuid"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"os"
	"testing"
	"time"
)

type ServiceTestSuite struct {
	suite.Suite
	helixLockMySqlConfig *helixLock.MySqlConfig
}

func (s *ServiceTestSuite) SetupSuite() {
	// Load environment variables from .env file
	err := util.LoadDevEnv()
	s.Require().NoError(err, "Failed to load dev environment")

	// Create MySQL configuration from environment variables
	db := os.Getenv("MYSQL_DB")
	dbHost := os.Getenv("MYSQL_HOST")
	dbUser := os.Getenv("MYSQL_USER")
	dbPassword := os.Getenv("MYSQL_PASSWORD")
	s.helixLockMySqlConfig = &helixLock.MySqlConfig{
		Database: db,
		Host:     dbHost,
		User:     dbUser,
		Password: dbPassword,
	}
}

type testSetup struct {
	app    *fx.App
	mockCf *util.MockCrossFunction

	clusterName         string
	locker              lock.Locker
	clusterManager      ClusterManager
	helixClusterMysqlDb *helixClusterMysql.Queries
	sqlDb               *sql.DB
}

func (s *ServiceTestSuite) makeApp() *testSetup {
	ts := testSetup{
		mockCf:      util.NewMockCrossFunction(time.Now()),
		clusterName: uuid.NewString(),
	}
	ts.app = fx.New(
		fx.Supply(s.helixLockMySqlConfig),
		fx.Provide(func() gox.CrossFunction {
			return ts.mockCf
		}),
		fx.Provide(func(config *helixLock.MySqlConfig) (*sql.DB, error) {
			config.SetupDefault()
			url := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", config.User, config.Password, config.Host, config.Port, config.Database)
			return sql.Open("mysql", url)
		}),
		fx.Provide(helixLock.NewHelixLockMySQLServiceWithSqlDb),

		fx.Supply(&ClusterManagerConfig{
			NodeHeartbeatInterval: time.Second,
			Name:                  ts.clusterName,
		}),
		fx.Provide(NewClusterManager, NewHelixDatasourceUsingSqlDb),

		fx.Populate(
			&ts.locker,
			&ts.clusterManager,
			&ts.helixClusterMysqlDb,
			&ts.sqlDb,
		),
	)
	err := ts.app.Start(context.Background())
	s.Require().NoError(err, "Failed to start service")
	return &ts
}

// We do not expect any node without register
func (s *ServiceTestSuite) TestNoNodesWithDefaultCluster() {
	ts := s.makeApp()
	cm := ts.clusterManager
	nodes, err := cm.GetActiveNodes(context.Background())
	s.Require().NoError(err, "Failed to get active nodes")
	s.Require().Len(nodes, 0)
}

// We do not expect any node without register
func (s *ServiceTestSuite) TestRegisterNode() {
	ts := s.makeApp()
	cm := ts.clusterManager
	db := ts.helixClusterMysqlDb

	nodes, err := cm.GetActiveNodes(context.Background())
	s.Require().NoError(err, "Failed to get active nodes")
	s.Require().Len(nodes, 0)

	nr, err := cm.RegisterNode(context.Background(), NodeRegisterRequest{Cluster: ts.clusterName})
	s.Require().NoError(err, "Failed to register node")
	s.Require().NotEmpty(nr.NodeId)

	nodes, err = cm.GetActiveNodes(context.Background())
	s.Require().NoError(err, "Failed to get active nodes")
	s.Require().Len(nodes, 1)

	var firstVersion int32 = -1
	for i := 0; i < 5; i++ {
		node, err := db.GetNodeById(context.Background(), helixClusterMysql.GetNodeByIdParams{
			ClusterName: ts.clusterName,
			NodeUuid:    nr.NodeId,
		})
		s.Require().NoError(err, "Failed to get node")
		fmt.Println("node version with HB", "id", node.NodeUuid, "version", node.Version)
		if firstVersion == -1 {
			firstVersion = node.Version
		} else if node.Version > firstVersion {
			firstVersion = node.Version
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.Require().True(firstVersion >= 1, "First version should be > 0")

}

// We do not expect any node without register
func (s *ServiceTestSuite) TestSimulateHbFailAndReregister() {
	ts := s.makeApp()
	cm := ts.clusterManager
	db := ts.helixClusterMysqlDb
	// sqlDb := ts.sqlDb

	nr, err := cm.RegisterNode(context.Background(), NodeRegisterRequest{Cluster: ts.clusterName})
	s.Require().NoError(err, "Failed to register node")
	s.Require().NotEmpty(nr.NodeId)
	nodes, err := cm.GetActiveNodes(context.Background())
	s.Require().NoError(err, "Failed to get active nodes")
	s.Require().Len(nodes, 1)

	err = db.DeregisterNode(context.Background(), helixClusterMysql.DeregisterNodeParams{
		ClusterName: ts.clusterName,
		NodeUuid:    nr.NodeId,
	})
	s.Require().NoError(err, "Failed to cleanup helix_locks table")
	nodes, err = cm.GetActiveNodes(context.Background())
	s.Require().NoError(err, "Failed to get active nodes")

	for _, node := range nodes {
		if node.Id == nr.NodeId {
			s.Require().Failf("we should never get this node as active as we marked it inactive", "nodeId=%s, nodeStatus=%d", node.Id, node.Status)
		}
	}

	gotNodeReregistered := false
	for i := 0; i < 5; i++ {
		nodes, err = cm.GetActiveNodes(context.Background())
		if len(nodes) == 1 {
			gotNodeReregistered = true
			break
		}
		time.Sleep(1 * time.Second)
	}
	s.Require().True(gotNodeReregistered, "We should never get this node reregistered")

	nodes, err = cm.GetActiveNodes(context.Background())
	s.Require().Len(nodes, 1)
	s.Require().False(nr.NodeId == nodes[0].Id, "old and new id must not be same")
}

func TestServiceTestSuite(t *testing.T) {
	suite.Run(t, new(ServiceTestSuite))
}
