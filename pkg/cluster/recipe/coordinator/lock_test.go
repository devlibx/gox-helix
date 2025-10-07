package coordinator

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/devlibx/gox-base/v2"
	helix "github.com/devlibx/gox-helix"
	databaseCommon "github.com/devlibx/gox-helix/pkg/common/database"
	_ "github.com/go-sql-driver/mysql"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
)

type testIndo struct {
	db     *sql.DB
	locker Locker
	ctx    context.Context
	now    time.Time
}

func setupDb(t *testing.T) *testIndo {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	helix.SetupTestEnv()

	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	database := os.Getenv("DB_NAME")
	url := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true", user, password, host, port, database)
	db, err := sql.Open("mysql", url)
	assert.NoError(t, err)

	now := time.Now()
	ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)

	var locker Locker
	app := fx.New(
		fx.Provide(func() databaseCommon.ConnectionHolder {
			return databaseCommon.NewConnectionHolder(db)
		}),
		fx.Provide(NewCoordinatorDataLayer),
		fx.Provide(NewLock),
		fx.Populate(&locker),

		fx.Provide(func() gox.CrossFunction {
			return gox.NewCrossFunction(NewMockTimeService(now))
		}),
	)
	err = app.Start(ctx)
	assert.NoError(t, err)

	return &testIndo{
		db:     db,
		locker: locker,
		ctx:    ctx,
		now:    now,
	}
}

func TestAcquireLock_FirstAcquisition(t *testing.T) {
	tf := setupDb(t)
	domain := uuid.NewString()
	resp, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: "lk-" + domain,
		OwnerId: "owner-" + domain,
		TTL:     time.Hour,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.False(t, resp.Reacquired)
}

func TestAcquireLock_Reacquisition_SameOwner(t *testing.T) {
}

func TestAcquireLock_FailedAcquisition_LockHeldByAnother(t *testing.T) {
}

func TestAcquireLock_AcquisitionAfterExpiry(t *testing.T) {
}

func TestAcquireLock_ReacquisitionExtendsTTL(t *testing.T) {
}

func TestAcquireLock_EpochIncrementOnOwnerChange(t *testing.T) {
}

func TestAcquireLock_DifferentDomains(t *testing.T) {
}
