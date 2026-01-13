package code

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-base/v2/serialization"
	helix "github.com/devlibx/gox-helix"
	"github.com/devlibx/gox-helix/internal/common"
	goxHelixApi "github.com/devlibx/gox-helix/pkg/api"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/coordinator"
	"github.com/devlibx/gox-helix/pkg/cluster/recipe/executor"
	pkgCommon "github.com/devlibx/gox-helix/pkg/common"
	"github.com/devlibx/gox-helix/pkg/common/config"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	"go.uber.org/fx"
)

//go:embed config.yaml
var configStr string

var DeleteData = true
var mu sync.Mutex
var part = map[string]map[int]string{}

func FullMain() {
	helix.SetupTestEnv()

	appConfig := config.Config{}
	err := serialization.ReadParameterizedYaml(configStr, &appConfig, "env")
	if err != nil {
		panic(err)
	}
	appConfig.SetDefaults()
	fmt.Printf("%+v\n", appConfig)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	appSignal := pkgCommon.NewApplicationSingletonWithContext(ctx)

	var count int64
	appCtx := &common.ApplicationCtx{}

	app := fx.New(

		fx.Supply(&appConfig),
		fx.Supply(&appConfig.Domains),
		fx.Supply(appSignal),

		fx.Provide(gox.NewCrossFunction),
		fx.Provide(func() (*sql.DB, error) {
			return sql.Open("mysql", helix.GetDefaultSqlUrl())
		}),
		fx.Provide(databaseCommon.NewConnectionHolder),

		goxHelixApi.Provider,
		// processor.Provider,
		// locker.Provider,
		// coordinator.Provider,
		// domain.Provider,
		// worker.Provider,

		// fx.Provide(executor.NewExecutor),
		fx.Invoke(NewCleanupOnBootupProvider),
		fx.Invoke(executor.NewExecutorLifecycle),

		fx.Provide(func(config *config.Config) coordinator.ClientFunctionProvider {
			return &testClientFunctionProviderImpl{
				config: config,
				count:  count,
			}
		}),

		fx.Populate(
			&appCtx.DomainDataLayer,
			&appCtx.WorkerDataLayer,
			&appCtx.ConnectionHolder,
			&appCtx.PartitionDistributionService,
			&appCtx.ProcessorFactory,
			&appCtx.ExecutorService,
			&appCtx.HealthCheck,
		),
	)
	err = app.Start(ctx)
	if err != nil {
		panic(err)
	}

	go func() {
		for {
			time.Sleep(1 * time.Second)
			if err := appCtx.HealthCheck.Check(); err != nil {
				fmt.Println("health check", err)
			}
		}
	}()

	time.Sleep(30 * time.Minute)
	appSignal.Stop()
	time.Sleep(30 * time.Minute)
}

func NewCleanupOnBootupProvider(lifecycle fx.Lifecycle, connectionHolder databaseCommon.ConnectionHolder) {
	lifecycle.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			if !DeleteData {
				return nil
			}
			db := connectionHolder.GetHelixMasterDbConnection()
			_, _ = db.Exec("TRUNCATE TABLE helix_workers")
			_, _ = db.Exec("TRUNCATE TABLE helix_domain")
			_, _ = db.Exec("TRUNCATE TABLE helix_worker_partition_mapping")
			_, _ = db.Exec("TRUNCATE TABLE helix_locks")
			_, _ = db.Exec("DELETE FROM helix_workers WHERE domain='food'")
			_, _ = db.Exec("DELETE FROM helix_workers WHERE domain='mobility'")
			_, _ = db.Exec("DELETE FROM helix_domain WHERE domain='food'")
			_, _ = db.Exec("DELETE FROM helix_domain WHERE domain='mobility'")
			return nil
		},
		OnStop: func(ctx context.Context) error {
			_ = connectionHolder.GetHelixMasterDbConnection().Close()
			return nil
		},
	})

}

type testClientFunctionProviderImpl struct {
	config *config.Config
	count  int64
}

func (t *testClientFunctionProviderImpl) Process(ctx context.Context, work coordinator.Work) {
	mu.Lock()
	key := work.Domain + "-" + work.Tasklist
	if _, ok := part[key]; !ok {
		part[key] = make(map[int]string)
	}
	part[key][work.Partition] = ""

	if atomic.AddInt64(&t.count, 1)%20 == 0 {
		slog.Info("Got work to do", "work", work)
		for k, v := range part {
			fmt.Println(k, v)
		}

		for domainName, domainObj := range t.config.Domains {
			for tasklistName, tasklistObj := range domainObj.TaskLists {
				if p, ok := part[domainName+"-"+tasklistName]; ok {
					if len(p) == tasklistObj.PartitionCount {
						slog.Info("Full allocation found", "domain", domainName, "tasklist", tasklistName)
					}
				}
			}
		}
	}
	defer mu.Unlock()

	time.Sleep(100 * time.Millisecond)
	work.CompletedChannel <- coordinator.WorkResponse{}
	close(work.CompletedChannel)
}

func (t *testClientFunctionProviderImpl) Shutdown(ctx context.Context) {
}

func (t *testClientFunctionProviderImpl) CreateWorkProcessFunction(ctx context.Context, info coordinator.CreateWorkProcessFunctionInfo) coordinator.ClientFunctionProcessor {
	return t
}
