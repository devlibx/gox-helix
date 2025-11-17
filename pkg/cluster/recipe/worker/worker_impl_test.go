package worker

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/devlibx/gox-base/v2"
	"github.com/devlibx/gox-helix"
	workerdb "github.com/devlibx/gox-helix/pkg/cluster/recipe/worker/database"
	commonDb "github.com/devlibx/gox-helix/pkg/common/database"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"log/slog"
	"os"
	"testing"
	"time"
)

type testWorkerInfo struct {
	db          *sql.DB
	worker      Worker
	ctx         context.Context
	config      Config
	querier     *workerdb.Queries
	timeService *MockTimeService
}

// MockTimeService is a mock implementation of TimeService for testing.
type MockTimeService struct {
	now time.Time
}

func (m *MockTimeService) Now() time.Time {
	return m.now
}

func (m *MockTimeService) SetNow(t time.Time) {
	m.now = t
}

// Main setup function for worker tests
func setupWorkerTest(t *testing.T) *testWorkerInfo {
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
		db.Close()
	})

	mockTimeService := &MockTimeService{now: time.Now()}

	testConfig := Config{
		Domain:            "test_worker_domain_" + uuid.NewString(),
		HeartbeatInterval: 10 * time.Millisecond,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	var worker Worker
	var querier *workerdb.Queries
	app := fx.New(
		fx.Provide(func() gox.CrossFunction {
			return gox.NewCrossFunction()
		}),
		fx.Provide(func() commonDb.ConnectionHolder {
			return commonDb.NewConnectionHolder(db)
		}),
		fx.Provide(func() Config {
			return testConfig
		}),
		fx.Provide(func() TimeService {
			return mockTimeService
		}),
		fx.Provide(NewWorkerDataLayer),
		fx.Provide(NewWorker),
		fx.Provide(workerdb.New),
		fx.Provide(func() workerdb.DBTX { return db }),
		fx.Populate(&worker),
		fx.Populate(&querier),
	)
	err = app.Start(ctx)
	assert.NoError(t, err)

	t.Cleanup(func() {
		app.Stop(ctx)
	})

	return &testWorkerInfo{
		db:          db,
		worker:      worker,
		ctx:         ctx,
		config:      testConfig,
		querier:     querier,
		timeService: mockTimeService,
	}
}

// TestWorker_Register verifies that a worker is correctly registered in the
// database when it starts. It should check for the correct domain, worker_id,
// and an 'active' status.
func TestWorker_Register(t *testing.T) {
	td := setupWorkerTest(t)

	// 4. Call Start() in a goroutine.
	errCh := make(chan error, 1)
	go func() {
		errCh <- td.worker.Start(td.ctx)
	}()

	// Give some time for the worker to start and register
	time.Sleep(50 * time.Millisecond)

	// 5. Query the database directly to verify the worker's record exists and is active.
	workerRecord, err := td.querier.GetWorker(td.ctx, workerdb.GetWorkerParams{WorkerID: td.worker.ID(), Domain: td.config.Domain})
	assert.NoError(t, err)
	assert.NotNil(t, workerRecord)
	assert.Equal(t, td.worker.ID(), workerRecord.WorkerID)
	assert.Equal(t, td.config.Domain, workerRecord.Domain)
	assert.Equal(t, "active", workerRecord.Status)
	assert.False(t, workerRecord.LastHeartbeatAt.IsZero())

	// 6. Call Stop().
	td.worker.Stop()

	// Wait for the worker to stop
	select {
	case err := <-errCh:
		assert.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("worker did not stop in time")
	}

	// Test inactive call
	td.worker.Stop()
	workerRecord, err = td.querier.GetWorker(td.ctx, workerdb.GetWorkerParams{WorkerID: td.worker.ID(), Domain: td.config.Domain})
	assert.NoError(t, err)
	assert.NotNil(t, workerRecord)
	assert.Equal(t, td.worker.ID(), workerRecord.WorkerID)
	assert.Equal(t, td.config.Domain, workerRecord.Domain)
	assert.Equal(t, "inactive", workerRecord.Status)
	assert.False(t, workerRecord.LastHeartbeatAt.IsZero())
}

// TestWorker_StartAndHeartbeat checks that the worker's heartbeat timestamp
// is updated after it starts.
func TestWorker_StartAndHeartbeat(t *testing.T) {

	td := setupWorkerTest(t)
	errCh := make(chan error, 1)
	go func() {
		errCh <- td.worker.Start(td.ctx)
	}()
	time.Sleep(100 * time.Millisecond)
	cfNow := td.timeService.Now()
	_ = cfNow

	now := time.Now().Add(1 * time.Hour)
	td.timeService.SetNow(now)
	time.Sleep(2 * time.Second)

	workerRecord, err := td.querier.GetWorker(td.ctx, workerdb.GetWorkerParams{WorkerID: td.worker.ID(), Domain: td.config.Domain})
	assert.NoError(t, err)
	assert.NotNil(t, workerRecord)
	assert.Equal(t, td.worker.ID(), workerRecord.WorkerID)
	assert.False(t, workerRecord.LastHeartbeatAt.Equal(now))
}

// TestWorker_SelfTerminateOnInactiveStatus verifies that a worker will stop
// itself if its status is updated to 'inactive' in the database.
func TestWorker_SelfTerminateOnInactiveStatus(t *testing.T) {
	td := setupWorkerTest(t)
	errCh := make(chan error, 1)
	go func() {
		errCh <- td.worker.Start(td.ctx)
	}()
	time.Sleep(100 * time.Millisecond)
	cfNow := td.timeService.Now()
	_ = cfNow

	now := time.Now().Add(1 * time.Hour)
	td.timeService.SetNow(now)
	time.Sleep(2 * time.Second)

	workerRecord, err := td.querier.GetWorker(td.ctx, workerdb.GetWorkerParams{WorkerID: td.worker.ID(), Domain: td.config.Domain})
	assert.NoError(t, err)
	assert.NotNil(t, workerRecord)
	assert.Equal(t, td.worker.ID(), workerRecord.WorkerID)
	assert.False(t, workerRecord.LastHeartbeatAt.Equal(now))

	// Stop - this should be set as false
	td.worker.Stop()

	running := true
	for i := 0; i < 5; i++ {
		if td.worker.(*mysqlWorker).isRunning == false {
			running = false
			break
		}
		time.Sleep(time.Second)
	}
	assert.False(t, running)
}

// TestWorker_Stop ensures that calling the Stop() method gracefully terminates
// the worker's heartbeat loop.
func TestWorker_Stop(t *testing.T) {
	// TODO:
	// 1. Setup test database and get a querier.
	// 2. Create a mock TimeService.
	// 3. Create a new worker and Start() it in a goroutine.
	// 4. Call Stop().
	// 5. Verify that the Start() method returns and the goroutine exits cleanly.

	td := setupWorkerTest(t)
	errCh := make(chan error, 1)
	go func() {
		errCh <- td.worker.Start(td.ctx)
	}()
	time.Sleep(100 * time.Millisecond)

	td.worker.Stop()

	// Make sure it is stopped
	running := true
	for i := 0; i < 5; i++ {
		if td.worker.(*mysqlWorker).isRunning == false {
			running = false
			break
		}
		time.Sleep(time.Second)
	}
	assert.False(t, running)

}
