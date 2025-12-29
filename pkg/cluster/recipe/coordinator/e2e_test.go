package coordinator

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/devlibx/gox-helix/pkg/common"
	"os"
	"testing"
	"time"

	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/domain"
	helixDomainMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/domain/database"
	locker "github.com/devlibx/gox-helix/pkg/cluster/recipe/lock"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/worker"
	helixWorkerMysql "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
)

type E2ETestSuite struct {
	suite.Suite
	db                   *sql.DB
	partitionService     PartitionService
	partitionDistributor *PartitionDistributionServiceImpl
	workerDataLayer      *worker.DataLayer
	domainDataLayer      *domain.DataLayer
}

func (s *E2ETestSuite) SetupSuite() {
	if os.Getenv("INTEGRATION_TESTS") != "true" {
		s.T().Skip("Skipping integration tests: Set INTEGRATION_TESTS=true to run")
	}

	// Setup test env and sql connection
	helix.SetupTestEnv()
	app := fx.New(
		fx.Provide(gox.NewCrossFunction),
		fx.Provide(func() (*sql.DB, error) {
			return sql.Open("mysql", helix.GetDefaultSqlUrl())
		}),
		fx.Provide(databaseCommon.NewConnectionHolder),

		fx.Provide(func() *common.ApplicationSingleton {
			return common.NewApplicationSingletonWithContext(context.Background())
		}),

		// Coordinator
		fx.Provide(NewCoordinatorDataLayer),
		fx.Provide(func(dataLayer *DataLayer) PartitionService { return dataLayer }),

		// Locker
		fx.Provide(locker.NewLockerDataLayer),
		fx.Provide(locker.NewLocker),

		// Worker
		fx.Provide(worker.NewWorkerDataLayer),
		fx.Provide(func(dataLayer *worker.DataLayer) WorkerService { return dataLayer }),

		// Domain
		fx.Provide(domain.NewDomainDataLayer),
		fx.Provide(func(dataLayer *domain.DataLayer) DomainService { return dataLayer }),

		// Distribution
		fx.Provide(NewDistributorStrategy),
		fx.Provide(func(lockService locker.Locker, distributor DistributorStrategy, partitionService PartitionService, as *common.ApplicationSingleton) (*PartitionDistributionServiceImpl, error) {
			p, err := NewPartitionDistributionService(lockService, distributor, partitionService, as)
			return p.(*PartitionDistributionServiceImpl), err
		}),

		fx.Populate(&s.db, &s.partitionService, &s.partitionDistributor, &s.workerDataLayer, &s.domainDataLayer),
	)

	err := app.Start(context.Background())
	s.Require().NoError(err, "fx app failed to start")
}

func (s *E2ETestSuite) SetupTest() {
	_, err := s.db.Exec("DELETE FROM helix_worker_partition_mapping WHERE domain like 'dev-automation-%'")
	s.Require().NoError(err)
	_, err = s.db.Exec("DELETE FROM helix_domain WHERE domain like 'dev-automation-%'")
	s.Require().NoError(err)
	_, err = s.db.Exec("DELETE FROM helix_workers WHERE domain like 'dev-automation-%'")
	s.Require().NoError(err)
}

func (s *E2ETestSuite) TestEndToEndDistribution() {
	domainName := "dev-automation-e2e-domain"
	tasklist := "dev-automation-e2e-tasklist"
	partitionCount := 20
	workerCount := 5
	ctx := context.Background()

	// 1. Register domain
	err := s.domainDataLayer.UpsertTasklist(ctx, helixDomainMysql.UpsertTasklistParams{
		Domain:         domainName,
		Tasklist:       tasklist,
		Metadata:       sql.NullString{Valid: true, String: `{}`},
		PartitionCount: uint32(partitionCount),
	})
	s.Require().NoError(err)

	// 2. Register workers
	for i := 0; i < workerCount; i++ {
		err := s.workerDataLayer.RegisterWorker(ctx, helixWorkerMysql.RegisterWorkerParams{
			WorkerID:        fmt.Sprintf("worker-%d", i),
			Domain:          domainName,
			CreatedAt:       s.workerDataLayer.Now(),
			LastHeartbeatAt: s.workerDataLayer.Now(),
		})
		s.Require().NoError(err)
	}

	// 3. Run distribution in a goroutine with cancellable context
	request := DistributionRequest{DomainName: domainName, TaskList: tasklist}
	processCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		_ = s.partitionDistributor.Process(processCtx, request)
	}()

	// Poll for results instead of fixed sleep - exits as soon as distribution completes
	maxAttempts := 30 // 30 * 500ms = 15s max
	for i := 0; i < maxAttempts; i++ {
		time.Sleep(500 * time.Millisecond)
		mappings, err := s.partitionService.GetActivePartitionMappings(ctx, domainName, tasklist)
		if err == nil && len(mappings) > 0 {
			// Distribution completed successfully, stop the process
			cancel()
			break
		}
	}
	cancel() // Ensure process stops even if we hit max attempts

	// 4. Verify the distribution
	mappings, err := s.partitionService.GetActivePartitionMappings(ctx, domainName, tasklist)
	s.Require().NoError(err)

	// a. Verify completeness: all partitions are assigned
	assignedPartitions := make(map[int]bool)
	for _, m := range mappings {
		for pId := range m.Mapping {
			assignedPartitions[pId] = true
		}
	}
	s.Len(assignedPartitions, partitionCount, "all partitions should be assigned")
	for i := 0; i < partitionCount; i++ {
		s.True(assignedPartitions[i], "partition %d should be assigned", i)
	}

	// b. Verify exclusivity: no partition is assigned to more than one worker
	partitionToWorkerMap := make(map[int]string)
	for _, m := range mappings {
		for pId := range m.Mapping {
			_, exists := partitionToWorkerMap[pId]
			s.False(exists, "partition %d is assigned to more than one worker", pId)
			partitionToWorkerMap[pId] = m.OwnerID
		}
	}
}

func TestE2E(t *testing.T) {
	suite.Run(t, new(E2ETestSuite))
}
