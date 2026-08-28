//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

func TestCleanupMaintainerSingleConnectionIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test disabled in short mode")
	}

	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	})

	setupSchema(t, ctx, db)
	maintainer, err := mysql.NewCleanupMaintainer(db, mysql.CleanupMaintainerConfig{
		Table:     "outbox",
		Retention: time.Hour,
		Clock:     fixedClock{now: time.Now().UTC()},
		Logger:    outbox.NopLogger{},
	})
	require.NoError(t, err)

	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	ensureCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	result, err := maintainer.Ensure(ensureCtx)
	require.NoError(t, err)
	require.Equal(t, mysql.CleanupResult{}, result)
}

func TestMaintainersReleaseNamedLockAfterCancellationIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test disabled in short mode")
	}

	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	})

	t.Run("cleanup", func(t *testing.T) {
		const lockName = "outbox:test:cleanup-cancel"

		setupSchema(t, ctx, db)
		clock := newNamedLockBarrierClock(time.Now().UTC())
		maintainer, err := mysql.NewCleanupMaintainer(db, mysql.CleanupMaintainerConfig{
			Table:     "outbox",
			Retention: time.Hour,
			LockName:  lockName,
			Clock:     clock,
			Logger:    outbox.NopLogger{},
		})
		require.NoError(t, err)

		observer := prepareNamedLockObserver(t, ctx, db)
		defer observer.Close()
		ensureCtx, cancel := context.WithCancel(ctx)
		defer func() {
			cancel()
			clock.resume()
		}()
		ensureDone := make(chan error, 1)
		go func() {
			_, ensureErr := maintainer.Ensure(ensureCtx)
			ensureDone <- ensureErr
		}()

		clock.waitUntilReached(t)
		requireNamedLockOwnedByAnotherSession(t, ctx, observer, lockName)
		cancel()
		clock.resume()
		require.ErrorIs(t, waitForEnsure(t, ensureDone), context.Canceled)
		requireCanAcquireNamedLock(t, ctx, observer, lockName)
	})

	t.Run("partitions", func(t *testing.T) {
		const lockName = "outbox:test:partitions-cancel"

		base := time.Now().UTC().Truncate(24 * time.Hour)
		dropOutboxTable(t, ctx, db)
		setupPartitionedSchema(t, ctx, db, base)
		clock := newNamedLockBarrierClock(base.Add(12 * time.Hour))
		maintainer, err := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{
			Table:     "outbox",
			Period:    mysql.PartitionDay,
			Lookahead: time.Hour,
			LockName:  lockName,
			Clock:     clock,
			Logger:    outbox.NopLogger{},
		})
		require.NoError(t, err)

		observer := prepareNamedLockObserver(t, ctx, db)
		defer observer.Close()
		ensureCtx, cancel := context.WithCancel(ctx)
		defer func() {
			cancel()
			clock.resume()
		}()
		ensureDone := make(chan error, 1)
		go func() {
			ensureDone <- maintainer.Ensure(ensureCtx)
		}()

		clock.waitUntilReached(t)
		requireNamedLockOwnedByAnotherSession(t, ctx, observer, lockName)
		cancel()
		clock.resume()
		require.NoError(t, waitForEnsure(t, ensureDone))
		requireCanAcquireNamedLock(t, ctx, observer, lockName)
	})
}

type namedLockBarrierClock struct {
	now        time.Time
	reached    chan struct{}
	proceed    chan struct{}
	resumeOnce sync.Once
}

func newNamedLockBarrierClock(now time.Time) *namedLockBarrierClock {
	return &namedLockBarrierClock{
		now:     now,
		reached: make(chan struct{}),
		proceed: make(chan struct{}),
	}
}

func (c *namedLockBarrierClock) Now() time.Time {
	close(c.reached)
	<-c.proceed

	return c.now
}

func (c *namedLockBarrierClock) waitUntilReached(t *testing.T) {
	t.Helper()
	select {
	case <-c.reached:
	case <-time.After(10 * time.Second):
		t.Fatal("maintainer did not reach the clock after acquiring its named lock")
	}
}

func (c *namedLockBarrierClock) resume() {
	c.resumeOnce.Do(func() {
		close(c.proceed)
	})
}

func prepareNamedLockObserver(t *testing.T, ctx context.Context, db *sql.DB) *sql.Conn {
	t.Helper()
	db.SetMaxIdleConns(0)
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)

	observer, err := db.Conn(ctx)
	require.NoError(t, err)

	return observer
}

func requireNamedLockOwnedByAnotherSession(t *testing.T, ctx context.Context, observer *sql.Conn, name string) {
	t.Helper()
	var (
		observerID int64
		ownerID    sql.NullInt64
	)
	require.NoError(t, observer.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&observerID))
	require.NoError(t, observer.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", name).Scan(&ownerID))
	require.True(t, ownerID.Valid, "named lock was not acquired")
	require.NotEqual(t, observerID, ownerID.Int64, "observer must use a different MySQL session")
}

func requireCanAcquireNamedLock(t *testing.T, ctx context.Context, observer *sql.Conn, name string) {
	t.Helper()
	var acquired sql.NullInt64
	require.NoError(t, observer.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", name).Scan(&acquired))
	require.True(t, acquired.Valid)
	require.EqualValues(t, 1, acquired.Int64, "named lock remained held after Ensure returned")

	var released sql.NullInt64
	require.NoError(t, observer.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", name).Scan(&released))
	require.True(t, released.Valid)
	require.EqualValues(t, 1, released.Int64)
}

func waitForEnsure(t *testing.T, ensureDone <-chan error) error {
	t.Helper()
	select {
	case err := <-ensureDone:
		return err
	case <-time.After(10 * time.Second):
		t.Fatal("maintainer did not return after cancellation")

		return nil
	}
}
