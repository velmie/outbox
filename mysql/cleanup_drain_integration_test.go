//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

func TestCleanupMaintainerDrainIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test disabled in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(
		func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
			if err := container.Terminate(context.Background()); err != nil {
				t.Error(err)
			}
		},
	)
	setupSchema(t, ctx, db)
	store, err := mysql.NewStore(db)
	require.NoError(t, err)
	seed := func(t *testing.T, processed, dead int) []outbox.ID {
		t.Helper()
		_, err := db.ExecContext(ctx, "DELETE FROM outbox")
		require.NoError(t, err)
		entries := make([]outbox.Entry, processed+dead+1)
		for i := range entries {
			entries[i] = validIntegrationEntry("cleanup")
		}
		insertEntries(t, ctx, db, store, entries)
		ids, err := fetchAllRecords(ctx, db)
		require.NoError(t, err)
		old := time.Now().UTC().Add(-2 * time.Hour)
		for i := 0; i < processed; i++ {
			setStatus(t, ctx, db, ids[i], outbox.StatusProcessed, &old, &old)
		}
		for i := processed; i < processed+dead; i++ {
			setStatus(t, ctx, db, ids[i], outbox.StatusDead, nil, &old)
		}
		return ids
	}
	t.Run("default single batch and opt-in drain", func(t *testing.T) {
		seed(t, 5, 3)
		logger := &maintenanceLogger{}
		cfg := mysql.CleanupMaintainerConfig{
			Retention:   time.Hour,
			Limit:       2,
			IncludeDead: true,
			LockName:    "drain_cleanup",
			Logger:      logger,
		}
		single, err := mysql.NewCleanupMaintainer(db, cfg)
		require.NoError(t, err)
		result, err := single.Ensure(ctx)
		require.NoError(t, err)
		require.Equal(
			t,
			mysql.CleanupResult{
				Processed: 2,
			},
			result,
		)
		cfg.DrainTimeout = 10 * time.Second
		drain, err := mysql.NewCleanupMaintainer(db, cfg)
		require.NoError(t, err)
		result, err = drain.Ensure(ctx)
		require.NoError(t, err)
		require.Equal(
			t,
			mysql.CleanupResult{
				Processed: 3,
				Dead:      3,
			},
			result,
		)
		require.Equal(t, 1, countByStatus(t, ctx, db, outbox.StatusPending))
		require.Zero(t, countByStatus(t, ctx, db, outbox.StatusProcessed))
		require.Zero(t, countByStatus(t, ctx, db, outbox.StatusDead))
		require.Equal(t, []string{"cleanup.completed", "cleanup.completed"}, maintenanceEvents(logger))
	})
	t.Run("partial failing batch preserves pass totals", func(t *testing.T) {
		seed(t, 3, 1)
		_, err := db.ExecContext(
			ctx,
			`CREATE TRIGGER reject_drain_dead BEFORE DELETE ON outbox
FOR EACH ROW BEGIN
 IF OLD.status = -1 THEN
  SIGNAL SQLSTATE '45000' SET MYSQL_ERRNO = 1644, MESSAGE_TEXT = 'dead cleanup rejected';
 END IF;
END`,
		)
		require.NoError(t, err)
		t.Cleanup(
			func() {
				_, err := db.ExecContext(ctx, "DROP TRIGGER reject_drain_dead")
				require.NoError(t, err)
			},
		)
		logger := &maintenanceLogger{}
		drain, err := mysql.NewCleanupMaintainer(
			db,
			mysql.CleanupMaintainerConfig{
				Retention:    time.Hour,
				Limit:        2,
				IncludeDead:  true,
				DrainTimeout: 10 * time.Second,
				Logger:       logger,
			},
		)
		require.NoError(t, err)
		result, err := drain.Ensure(ctx)
		var cause *mysqldriver.MySQLError
		require.ErrorAs(t, err, &cause)
		require.EqualValues(t, 1644, cause.Number)
		require.Equal(
			t,
			mysql.CleanupResult{
				Processed: 3,
			},
			result,
		)
		require.Equal(t, 1, countByStatus(t, ctx, db, outbox.StatusDead))
		require.Equal(t, []string{"cleanup.failed"}, maintenanceEvents(logger))
		require.Same(t, err, logger.events[0]["err"])
	})
	t.Run("budget lets active batch finish while lock covers pass", func(t *testing.T) {
		ids := seed(t, 4, 0)
		blocker, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		defer blocker.Rollback()
		var status outbox.Status
		require.NoError(t, blocker.QueryRowContext(ctx, "SELECT status FROM outbox WHERE id = ? FOR UPDATE", ids[1]).Scan(&status))
		logger := &maintenanceLogger{}
		cfg := mysql.CleanupMaintainerConfig{
			Retention:    time.Hour,
			Limit:        1,
			DrainTimeout: 3 * time.Second,
			LockName:     "drain_budget_cleanup",
			Logger:       logger,
		}
		drain, err := mysql.NewCleanupMaintainer(db, cfg)
		require.NoError(t, err)
		var result mysql.CleanupResult
		var drainErr error
		done := make(chan struct{})
		go func() {
			result, drainErr = drain.Ensure(ctx)
			close(done)
		}()
		require.Eventually(
			t,
			func() bool {
				var count int
				queryErr := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox WHERE status = ?", outbox.StatusProcessed).Scan(&count)
				return queryErr == nil && count == 3
			},
			2*time.Second,
			10*time.Millisecond,
		)
		competingLogger := &maintenanceLogger{}
		cfg.Logger = competingLogger
		competing, err := mysql.NewCleanupMaintainer(db, cfg)
		require.NoError(t, err)
		skipped, err := competing.Ensure(ctx)
		require.NoError(t, err)
		require.Equal(t, mysql.CleanupResult{}, skipped)
		require.Equal(t, []string{"cleanup.skipped"}, maintenanceEvents(competingLogger))
		// Keep the second batch blocked beyond the budget. It must not be canceled.
		select {
		case <-done:
			t.Fatal("drain budget interrupted the active batch")
		case <-time.After(4 * time.Second):
		}
		require.NoError(t, blocker.Rollback())
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("cleanup did not finish after the active batch")
		}
		require.NoError(t, drainErr)
		require.Equal(
			t,
			mysql.CleanupResult{
				Processed: 2,
			},
			result,
		)
		require.Equal(t, []string{"cleanup.completed"}, maintenanceEvents(logger))
		require.Equal(t, "succeeded", logger.events[0]["outcome"])
		require.Equal(t, "budget_exhausted", logger.events[0]["reason"])
		require.NotContains(t, logger.events[0], "err")
		var owner sql.NullInt64
		require.NoError(t, db.QueryRowContext(ctx, "SELECT IS_USED_LOCK(?)", cfg.LockName).Scan(&owner))
		require.False(t, owner.Valid)
		remaining := countByStatus(t, ctx, db, outbox.StatusProcessed)
		require.Equal(t, 2, remaining)
		cfg.Logger = &maintenanceLogger{}
		resumed, err := mysql.NewCleanupMaintainer(db, cfg)
		require.NoError(t, err)
		result, err = resumed.Ensure(ctx)
		require.NoError(t, err)
		require.EqualValues(t, remaining, result.Processed)
		require.Equal(t, 1, countByStatus(t, ctx, db, outbox.StatusPending))
	})
}
