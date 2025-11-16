package domain

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix"
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
	db     *sql.DB
	domain Domain
	ctx    context.Context
	config Config
}

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

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	testConfig := Config{
		Domain: "test_domain_" + uuid.NewString(),
		Domains: []TaskList{
			{Name: "task_list_1", PartitionCount: 10},
			{Name: "task_list_2", PartitionCount: 20},
		},
	}

	var domain Domain
	app := fx.New(
		fx.Provide(func() gox.CrossFunction {
			return gox.NewCrossFunction()
		}),
		fx.Provide(func() database.ConnectionHolder {
			return database.NewConnectionHolder(db)
		}),
		fx.Provide(func() Config {
			return testConfig
		}),
		fx.Provide(NewDomainDataLayer),
		fx.Provide(NewDomain),
		fx.Populate(&domain),
	)
	err = app.Start(ctx)
	assert.NoError(t, err)

	t.Cleanup(func() {
		app.Stop(ctx)
		db.Close()
	})

	return &testDomainInfo{
		db:     db,
		domain: domain,
		ctx:    ctx,
		config: testConfig,
	}
}

func TestDomain_Init(t *testing.T) {
	td := setupDomainTest(t)

	err := td.domain.Init(td.ctx)
	assert.NoError(t, err)

	for _, taskList := range td.config.Domains {
		var fetchedDomain, fetchedTasklist string
		var fetchedPartitionCount int
		query := "SELECT domain, tasklist, partition_count FROM helix_domain WHERE domain = ? AND tasklist = ?"
		err := td.db.QueryRowContext(td.ctx, query, td.config.Domain, taskList.Name).Scan(&fetchedDomain, &fetchedTasklist, &fetchedPartitionCount)
		assert.NoError(t, err)
		assert.Equal(t, td.config.Domain, fetchedDomain)
		assert.Equal(t, taskList.Name, fetchedTasklist)
		assert.Equal(t, taskList.PartitionCount, fetchedPartitionCount)
	}
}
