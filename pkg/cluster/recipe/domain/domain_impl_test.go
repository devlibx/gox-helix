package domain

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix"
	"github.com/devlibx/gox-helix/pkg/common/config"
	database "github.com/devlibx/gox-helix/pkg/common/database"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"log/slog"
	"os"
	"testing"
	"time"
)

type testDomainInfo struct {
	db         *sql.DB
	domain     Service
	ctx        context.Context
	cfg        *config.Config
	domainName string
}

// Helper to setup domain test with a specific config
func setupDomainTestWithConfig(t *testing.T, db *sql.DB, cfg *config.Config) (Service, context.Context) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	var domain Service
	app := fx.New(
		fx.Provide(func() gox.CrossFunction {
			return gox.NewCrossFunction()
		}),
		fx.Provide(func() database.ConnectionHolder {
			return database.NewConnectionHolder(db)
		}),
		fx.Provide(func() *config.Config {
			return cfg
		}),
		fx.Provide(NewDomainDataLayer),
		fx.Provide(NewService),
		fx.Populate(&domain),
	)
	err := app.Start(ctx)
	assert.NoError(t, err)

	t.Cleanup(func() {
		app.Stop(ctx)
	})

	return domain, ctx
}

// Main setup function for domain tests
func setupDomainTest(t *testing.T) *testDomainInfo {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	helix.SetupTestEnv()

	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	dbName := os.Getenv("DB_NAME")
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, password, host, port, dbName)
	db, err := sql.Open("mysql", url)
	assert.NoError(t, err)

	t.Cleanup(func() {
		_ = db.Close()
	})

	domainName := "test_domain_" + uuid.NewString()
	testConfig := &config.Config{
		Domains: map[string]*config.Domain{
			domainName: {
				TaskLists: map[string]*config.TaskList{
					"task_list_1": {
						PartitionCount: 10,
					},
					"task_list_2": {
						PartitionCount: 20,
					},
				},
			},
		},
	}
	testConfig.SetDefaults()

	domain, ctx := setupDomainTestWithConfig(t, db, testConfig)

	return &testDomainInfo{
		db:         db,
		domain:     domain,
		ctx:        ctx,
		cfg:        testConfig,
		domainName: domainName,
	}
}

func TestDomain_Start(t *testing.T) {
	td := setupDomainTest(t)

	err := td.domain.Start(td.ctx)
	assert.NoError(t, err)

	for domainName, domain := range td.cfg.Domains {
		for tasklistName, taskList := range domain.TaskLists {
			var fetchedDomain, fetchedTasklist string
			var fetchedPartitionCount int
			query := "SELECT domain, tasklist, partition_count FROM helix_domain WHERE domain = ? AND tasklist = ?"
			err := td.db.QueryRowContext(td.ctx, query, domainName, tasklistName).Scan(&fetchedDomain, &fetchedTasklist, &fetchedPartitionCount)
			assert.NoError(t, err)
			assert.Equal(t, domainName, fetchedDomain)
			assert.Equal(t, tasklistName, fetchedTasklist)
			assert.Equal(t, taskList.PartitionCount, fetchedPartitionCount)
		}
	}
}

func TestDomain_Start_Idempotency(t *testing.T) {
	td := setupDomainTest(t)

	// First Start
	err := td.domain.Start(td.ctx)
	assert.NoError(t, err)

	// Verify first start
	for domainName, domain := range td.cfg.Domains {
		for tasklistName, taskList := range domain.TaskLists {
			var fetchedPartitionCount int
			query := "SELECT partition_count FROM helix_domain WHERE domain = ? AND tasklist = ?"
			err := td.db.QueryRowContext(td.ctx, query, domainName, tasklistName).Scan(&fetchedPartitionCount)
			assert.NoError(t, err)
			assert.Equal(t, taskList.PartitionCount, fetchedPartitionCount)
		}
	}

	// Create a new domain with the same config and run Start again
	domain2, ctx2 := setupDomainTestWithConfig(t, td.db, td.cfg)
	err = domain2.Start(ctx2)
	assert.NoError(t, err)

	// Verify again
	for domainName, domain := range td.cfg.Domains {
		for tasklistName, taskList := range domain.TaskLists {
			var fetchedPartitionCount int
			query := "SELECT partition_count FROM helix_domain WHERE domain = ? AND tasklist = ?"
			err := td.db.QueryRowContext(ctx2, query, domainName, tasklistName).Scan(&fetchedPartitionCount)
			assert.NoError(t, err)
			assert.Equal(t, taskList.PartitionCount, fetchedPartitionCount)
		}
	}
}

func TestDomain_Start_AddNewTaskList(t *testing.T) {
	td := setupDomainTest(t)

	// First Start
	err := td.domain.Start(td.ctx)
	assert.NoError(t, err)

	// Verify first start
	for domainName, domain := range td.cfg.Domains {
		for tasklistName, taskList := range domain.TaskLists {
			var fetchedPartitionCount int
			query := "SELECT partition_count FROM helix_domain WHERE domain = ? AND tasklist = ?"
			err := td.db.QueryRowContext(td.ctx, query, domainName, tasklistName).Scan(&fetchedPartitionCount)
			assert.NoError(t, err)
			assert.Equal(t, taskList.PartitionCount, fetchedPartitionCount)
		}
	}

	// Create a new config with an additional task list
	newConfig := td.cfg
	newTaskList := &config.TaskList{PartitionCount: 30}
	newConfig.Domains[td.domainName].TaskLists["task_list_3"] = newTaskList
	newConfig.SetDefaults()

	// Create a new domain with the new config and run Start
	domain2, ctx2 := setupDomainTestWithConfig(t, td.db, newConfig)
	err = domain2.Start(ctx2)
	assert.NoError(t, err)

	// Verify all task lists are present
	for domainName, domain := range newConfig.Domains {
		for tasklistName, taskList := range domain.TaskLists {
			var fetchedPartitionCount int
			query := "SELECT partition_count FROM helix_domain WHERE domain = ? AND tasklist = ?"
			err := td.db.QueryRowContext(ctx2, query, domainName, tasklistName).Scan(&fetchedPartitionCount)
			assert.NoError(t, err)
			assert.Equal(t, taskList.PartitionCount, fetchedPartitionCount)
		}
	}
}
