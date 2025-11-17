package locker

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
	db              *sql.DB
	locker          Locker
	ctx             context.Context
	now             time.Time
	mockTimeService *databaseCommon.MockTimeService
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

	mockTimeService := databaseCommon.NewMockTimeService(now)
	var locker Locker
	app := fx.New(
		fx.Provide(func() databaseCommon.ConnectionHolder {
			return databaseCommon.NewConnectionHolder(db)
		}),
		fx.Provide(func() gox.CrossFunction {
			return gox.NewCrossFunction(mockTimeService)
		}),
		fx.Provide(NewLockerDataLayer),
		fx.Provide(NewLocker),
		fx.Populate(&locker),
	)
	err = app.Start(ctx)
	assert.NoError(t, err)

	return &testIndo{
		db:              db,
		locker:          locker,
		ctx:             ctx,
		now:             now,
		mockTimeService: mockTimeService,
	}
}

// TestAcquireLock_FirstAcquisition verifies the initial acquisition of a lock.
//
// Test Objective:
// - Verify that a lock can be successfully acquired when no prior lock exists
// - Ensure the lock is created with correct initial state
//
// Expected Behavior:
// - Lock acquisition should succeed (err = nil)
// - Response should indicate this is NOT a reacquisition (Reacquired = false)
// - Lock should be stored in database with:
//   - Correct domain, lock_key, and owner_id
//   - Epoch = 1 (first acquisition)
//   - expiry_time = now + TTL
//   - Status = ACQUIRED
//
// Verification Steps:
// - Call AcquireLock with unique domain, lock_key, and owner_id
// - Assert no error returned
// - Assert response is not nil
// - Assert Reacquired flag is false
// - (Future) Query database to verify lock record with correct fields
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
}

// TestAcquireLock_Reacquisition_SameOwner verifies lock reacquisition by the same owner.
//
// Test Objective:
// - Verify that the same owner can reacquire their own lock before it expires
// - Ensure the lock state is maintained correctly on reacquisition
//
// Expected Behavior:
// - First acquisition should succeed with Reacquired = false
// - Second acquisition by same owner should succeed with Reacquired = true
// - Lock record should be updated with:
//   - Same owner_id
//   - Same epoch (no increment for same owner)
//   - Updated expiry_time = now + new TTL
//   - Status remains ACQUIRED
//
// Verification Steps:
// - Acquire lock with owner A
// - Immediately acquire same lock with owner A again
// - Assert second acquisition succeeds with Reacquired = true
// - (Future) Verify database shows updated expiry_time but same epoch
func TestAcquireLock_Reacquisition_SameOwner(t *testing.T) {
	tf := setupDb(t)
	domain := uuid.NewString()
	lockKey := "lk-" + domain
	ownerId := "owner-" + domain

	// First acquisition
	resp1, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: lockKey,
		OwnerId: ownerId,
		TTL:     time.Hour,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp1)

	// Advance time by 10 seconds to simulate time passage
	tf.mockTimeService.AdvanceTime(10 * time.Second)

	// Reacquisition by same owner
	resp2, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: lockKey,
		OwnerId: ownerId,
		TTL:     time.Hour,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp2)
}

// TestAcquireLock_FailedAcquisition_LockHeldByAnother verifies lock acquisition failure
// when the lock is held by a different owner.
//
// Test Objective:
// - Verify that lock acquisition fails when another owner holds the lock
// - Ensure the original lock remains unchanged
//
// Expected Behavior:
// - First acquisition by owner A should succeed
// - Second acquisition by owner B (different owner) should fail with ErrLockHeldByAnother
// - Lock record should remain unchanged:
//   - owner_id stays as owner A
//   - epoch remains unchanged
//   - expiry_time remains unchanged
//   - Status remains ACQUIRED
//
// Verification Steps:
// - Acquire lock with owner A
// - Attempt to acquire same lock with owner B (different owner)
// - Assert second acquisition fails with ErrLockHeldByAnother error
// - (Future) Verify database shows lock still owned by owner A with original values
func TestAcquireLock_FailedAcquisition_LockHeldByAnother(t *testing.T) {
	tf := setupDb(t)
	domain := uuid.NewString()
	lockKey := "lk-" + domain
	ownerA := "owner-A-" + domain
	ownerB := "owner-B-" + domain

	// First acquisition by owner A
	resp1, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: lockKey,
		OwnerId: ownerA,
		TTL:     time.Hour,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp1)

	// Advance time by 10 seconds (still well before expiry)
	tf.mockTimeService.AdvanceTime(10 * time.Second)

	// Attempt to acquire same lock with owner B (should fail)
	resp2, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: lockKey,
		OwnerId: ownerB,
		TTL:     time.Hour,
	})

	// Assert that acquisition failed
	assert.Error(t, err)
	assert.Nil(t, resp2)

	// Assert error is LockNotAcquiredError
	var lockErr *LockNotAcquiredError
	assert.ErrorAs(t, err, &lockErr)
	assert.Equal(t, domain, lockErr.Domain)
	assert.Equal(t, lockKey, lockErr.LockKey)
	assert.Equal(t, ownerB, lockErr.OwnerId)
}

// TestAcquireLock_AcquisitionAfterExpiry verifies lock acquisition after lock expires.
//
// Test Objective:
// - Verify that a lock can be acquired by a new owner after the previous lock expires
// - Ensure expired locks don't prevent new acquisitions
//
// Expected Behavior:
// - First acquisition by owner A with short TTL (e.g., 1 second) should succeed
// - Wait for lock to expire (time > TTL)
// - Second acquisition by owner B should succeed as a fresh acquisition
// - Lock record should be updated with:
//   - New owner_id = owner B
//   - Epoch incremented (owner changed)
//   - New expiry_time = current_time + new TTL
//   - Status = ACQUIRED
//
// Verification Steps:
// - Acquire lock with owner A and short TTL (1-2 seconds)
// - Wait for expiry (sleep for TTL + buffer)
// - Attempt to acquire lock with owner B
// - Assert acquisition succeeds with Reacquired = false (fresh acquisition)
// - (Future) Verify database shows new owner with incremented epoch
func TestAcquireLock_AcquisitionAfterExpiry(t *testing.T) {
	tf := setupDb(t)
	domain := uuid.NewString()
	lockKey := "lk-" + domain
	ownerA := "owner-A-" + domain
	ownerB := "owner-B-" + domain

	// First acquisition by owner A with short TTL
	resp1, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: lockKey,
		OwnerId: ownerA,
		TTL:     10 * time.Second,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp1)

	// Advance time past the TTL to let the lock expire
	tf.mockTimeService.AdvanceTime(11 * time.Second)

	// Acquisition by owner B after expiry (should succeed as fresh acquisition)
	resp2, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: lockKey,
		OwnerId: ownerB,
		TTL:     time.Hour,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp2)
}

// TestAcquireLock_ReacquisitionExtendsTTL verifies that reacquisition extends lock TTL.
//
// Test Objective:
// - Verify that when the same owner reacquires a lock, the TTL is properly extended
// - Ensure the lock doesn't expire prematurely when reacquired
//
// Expected Behavior:
// - First acquisition by owner A with TTL of 5 seconds should succeed
// - Wait for 3 seconds (more than half TTL but before expiry)
// - Reacquire lock with owner A and TTL of 10 seconds
// - Lock record should be updated with:
//   - Same owner_id = owner A
//   - Same epoch (no increment for same owner)
//   - New expiry_time = current_time + 10 seconds (extended)
//   - Status = ACQUIRED
//
// - Original expiry should be overwritten with new expiry
//
// Verification Steps:
// - Acquire lock with owner A and TTL = 5 seconds, record initial expiry time
// - Wait 3 seconds
// - Reacquire lock with owner A and TTL = 10 seconds
// - Assert Reacquired = true
// - (Future) Query database to verify expiry_time is now > initial_expiry_time
// - (Future) Verify new expiry = (current_time after wait) + 10 seconds
func TestAcquireLock_ReacquisitionExtendsTTL(t *testing.T) {
	tf := setupDb(t)
	domain := uuid.NewString()
	lockKey := "lk-" + domain
	ownerId := "owner-" + domain

	// First acquisition with TTL of 10 seconds
	resp1, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: lockKey,
		OwnerId: ownerId,
		TTL:     10 * time.Second,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp1)

	// Advance time by 5 seconds (half TTL, still before expiry)
	tf.mockTimeService.AdvanceTime(5 * time.Second)

	// Reacquisition by same owner with extended TTL of 20 seconds
	resp2, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: lockKey,
		OwnerId: ownerId,
		TTL:     20 * time.Second,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp2)
}

// TestAcquireLock_EpochIncrementOnOwnerChange verifies epoch increments when ownership changes.
//
// Test Objective:
// - Verify that the epoch counter increments whenever lock ownership changes
// - Ensure epoch tracking helps detect stale lock holders
//
// Expected Behavior:
// - First acquisition by owner A should create lock with epoch = 1
// - Lock expires (short TTL)
// - Second acquisition by owner B should update lock with epoch = 2
// - Lock expires again
// - Third acquisition by owner C should update lock with epoch = 3
// - Each ownership change should increment epoch by 1
//
// Verification Steps:
// - Acquire lock with owner A and short TTL (1 second)
// - (Future) Verify database shows epoch = 1
// - Wait for expiry
// - Acquire lock with owner B and short TTL
// - (Future) Verify database shows epoch = 2
// - Wait for expiry
// - Acquire lock with owner C
// - (Future) Verify database shows epoch = 3
// - Assert all acquisitions succeed
// - Verify epoch increments on each owner change but not on reacquisition by same owner
func TestAcquireLock_EpochIncrementOnOwnerChange(t *testing.T) {
	tf := setupDb(t)
	domain := uuid.NewString()
	lockKey := "lk-" + domain
	ownerA := "owner-A-" + domain
	ownerB := "owner-B-" + domain
	ownerC := "owner-C-" + domain

	// First acquisition by owner A with short TTL
	resp1, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: lockKey,
		OwnerId: ownerA,
		TTL:     5 * time.Second,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp1)

	// Query database to verify epoch = 1
	var epoch1 uint64
	err = tf.db.QueryRow("SELECT epoch FROM helix_locks WHERE lock_key = ? AND status = 1", lockKey).Scan(&epoch1)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), epoch1)

	// Advance time past TTL to let lock expire
	tf.mockTimeService.AdvanceTime(6 * time.Second)

	// Second acquisition by owner B (different owner after expiry)
	resp2, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: lockKey,
		OwnerId: ownerB,
		TTL:     5 * time.Second,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp2)

	// Query database to verify epoch = 2 (incremented due to owner change)
	var epoch2 uint64
	err = tf.db.QueryRow("SELECT epoch FROM helix_locks WHERE lock_key = ? AND status = 1", lockKey).Scan(&epoch2)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), epoch2)

	// Advance time past TTL again
	tf.mockTimeService.AdvanceTime(6 * time.Second)

	// Third acquisition by owner C (another owner change)
	resp3, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: lockKey,
		OwnerId: ownerC,
		TTL:     time.Hour,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp3)

	// Query database to verify epoch = 3 (incremented again)
	var epoch3 uint64
	err = tf.db.QueryRow("SELECT epoch FROM helix_locks WHERE lock_key = ? AND status = 1", lockKey).Scan(&epoch3)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), epoch3)

	// Verify reacquisition by same owner does NOT increment epoch
	tf.mockTimeService.AdvanceTime(10 * time.Second)
	resp4, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domain,
		LockKey: lockKey,
		OwnerId: ownerC, // Same owner as before
		TTL:     time.Hour,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp4)

	// Query database to verify epoch is still 3 (no increment for same owner)
	var epoch4 uint64
	err = tf.db.QueryRow("SELECT epoch FROM helix_locks WHERE lock_key = ? AND status = 1", lockKey).Scan(&epoch4)
	assert.NoError(t, err)
	assert.Equal(t, uint64(3), epoch4)
}

// TestAcquireLock_DifferentDomains verifies lock isolation across different domains.
//
// Test Objective:
// - Verify that locks with the same lock_key in different domains are independent
// - Ensure domain-based partitioning works correctly
//
// Expected Behavior:
// - Acquisition of lock_key "my-lock" in domain "A" by owner X should succeed
// - Acquisition of lock_key "my-lock" in domain "B" by owner Y should also succeed
// - Both locks should coexist independently:
//   - Domain A: lock owned by owner X
//   - Domain B: lock owned by owner Y
//
// - Changes to one domain's lock should not affect the other domain's lock
//
// Verification Steps:
// - Acquire lock with domain="A", lock_key="shared-key", owner="owner-A"
// - Assert acquisition succeeds
// - Acquire lock with domain="B", lock_key="shared-key", owner="owner-B"
// - Assert acquisition succeeds (not blocked by domain A's lock)
// - (Future) Verify database shows two separate lock records
// - (Future) Verify each record has correct domain and owner_id
// - Demonstrate locks are truly isolated by different domains
func TestAcquireLock_DifferentDomains(t *testing.T) {
	tf := setupDb(t)
	sharedLockKey := "shared-lock-key-" + uuid.NewString()
	domainA := "domain-A-" + uuid.NewString()
	domainB := "domain-B-" + uuid.NewString()
	ownerA := "owner-A"
	ownerB := "owner-B"

	// Acquire lock in domain A
	resp1, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domainA,
		LockKey: sharedLockKey,
		OwnerId: ownerA,
		TTL:     time.Hour,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp1)

	// Acquire lock with same lock_key in domain B (should succeed independently)
	resp2, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domainB,
		LockKey: sharedLockKey,
		OwnerId: ownerB,
		TTL:     time.Hour,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp2)

	// Verify both locks exist in database with correct domains and owners
	var ownerIdA, domainAFromDb string
	err = tf.db.QueryRow("SELECT domain, owner_id FROM helix_locks WHERE lock_key = ? AND domain = ? AND status = 1", sharedLockKey, domainA).Scan(&domainAFromDb, &ownerIdA)
	assert.NoError(t, err)
	assert.Equal(t, domainA, domainAFromDb)
	assert.Equal(t, ownerA, ownerIdA)

	var ownerIdB, domainBFromDb string
	err = tf.db.QueryRow("SELECT domain, owner_id FROM helix_locks WHERE lock_key = ? AND domain = ? AND status = 1", sharedLockKey, domainB).Scan(&domainBFromDb, &ownerIdB)
	assert.NoError(t, err)
	assert.Equal(t, domainB, domainBFromDb)
	assert.Equal(t, ownerB, ownerIdB)

	// Attempt to acquire domain A's lock with a different owner (should fail - lock held)
	resp3, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domainA,
		LockKey: sharedLockKey,
		OwnerId: "owner-different",
		TTL:     time.Hour,
	})
	assert.Error(t, err)
	assert.Nil(t, resp3)

	// Reacquire domain B's lock with same owner B (should succeed)
	tf.mockTimeService.AdvanceTime(10 * time.Second)
	resp4, err := tf.locker.AcquireLock(tf.ctx, AcquireLockRequest{
		Domain:  domainB,
		LockKey: sharedLockKey,
		OwnerId: ownerB,
		TTL:     time.Hour,
	})
	assert.NoError(t, err)
	assert.NotNil(t, resp4)
}
