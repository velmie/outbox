//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

func TestPartitionMaintainerEnsureIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test disabled in short mode")
	}

	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	})

	now := time.Now().UTC().Truncate(24 * time.Hour)
	setupPartitionedSchema(t, ctx, db, now)

	maintainer, err := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{
		Table:     "outbox",
		Period:    mysql.PartitionDay,
		Lookahead: 24 * time.Hour,
		Retention: 24 * time.Hour,
		Clock:     fixedClock{now: now.Add(12 * time.Hour)},
		Logger:    outbox.NopLogger{},
	})
	require.NoError(t, err)

	require.NoError(t, maintainer.Ensure(ctx))

	names := listPartitionNames(t, ctx, db, "outbox")
	oldName := dayPartitionName(now.Add(-48 * time.Hour))
	prevName := dayPartitionName(now.Add(-24 * time.Hour))
	curName := dayPartitionName(now)
	nextName := dayPartitionName(now.Add(24 * time.Hour))

	require.NotContains(t, names, oldName)
	require.Contains(t, names, prevName)
	require.Contains(t, names, curName)
	require.Contains(t, names, nextName)
	require.Contains(t, names, "pmax")
}

func TestPartitionMaintainerRejectsUnsafeMetadataNamesIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test disabled in short mode")
	}

	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	})

	now := time.Date(2025, 3, 3, 0, 0, 0, 0, time.UTC)

	t.Run("reorganize", func(t *testing.T) {
		const table = "outbox_invalid_max"
		_, err := db.ExecContext(ctx, `
CREATE TABLE outbox_invalid_max (
    id BIGINT NOT NULL,
    created_ts BIGINT NOT NULL,
    PRIMARY KEY (id, created_ts)
) ENGINE=InnoDB
PARTITION BY RANGE (created_ts) (
    PARTITION `+"`p-max`"+` VALUES LESS THAN (MAXVALUE)
);`)
		require.NoError(t, err)
		require.Contains(t, listPartitionNames(t, ctx, db, table), "p-max")

		maintainer, err := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{
			Table:     table,
			Period:    mysql.PartitionDay,
			Lookahead: 24 * time.Hour,
			Clock:     fixedClock{now: now},
			Logger:    outbox.NopLogger{},
		})
		require.NoError(t, err)

		err = maintainer.Ensure(ctx)
		require.ErrorIs(t, err, mysql.ErrInvalidPartition)
		require.Contains(t, listPartitionNames(t, ctx, db, table), "p-max")
	})

	t.Run("drop", func(t *testing.T) {
		const table = "outbox_invalid_drop"
		stmt := fmt.Sprintf(`
CREATE TABLE outbox_invalid_drop (
    id BIGINT NOT NULL,
    created_ts BIGINT NOT NULL,
    PRIMARY KEY (id, created_ts)
) ENGINE=InnoDB
PARTITION BY RANGE (created_ts) (
    PARTITION `+"`p-old`"+` VALUES LESS THAN (%d),
    PARTITION pfuture VALUES LESS THAN (%d),
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
);`, now.Add(-48*time.Hour).Unix(), now.Add(48*time.Hour).Unix())
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)
		require.Contains(t, listPartitionNames(t, ctx, db, table), "p-old")

		maintainer, err := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{
			Table:     table,
			Period:    mysql.PartitionDay,
			Lookahead: 24 * time.Hour,
			Retention: 24 * time.Hour,
			Clock:     fixedClock{now: now},
			Logger:    outbox.NopLogger{},
		})
		require.NoError(t, err)

		err = maintainer.Ensure(ctx)
		require.ErrorIs(t, err, mysql.ErrInvalidPartition)
		require.Contains(t, listPartitionNames(t, ctx, db, table), "p-old")
	})
}

func TestPartitionMaintainerRetentionSafetyIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test disabled in short mode")
	}

	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	})

	base := time.Date(2025, 3, 3, 0, 0, 0, 0, time.UTC)
	pendingID := parseIntegrationID(t, "01955193-de00-7000-8000-000000000001")
	processedID := parseIntegrationID(t, "01955193-de00-7000-8000-000000000002")
	deadID := parseIntegrationID(t, "01955193-de00-7000-8000-000000000003")
	lateID := parseIntegrationID(t, "01955193-de00-7000-8000-000000000004")

	t.Run("incompatible layout fails before DDL", func(t *testing.T) {
		dropOutboxTable(t, ctx, db)
		stmt := fmt.Sprintf(`
CREATE TABLE outbox (
    id BINARY(16) NOT NULL,
    status BIGINT NOT NULL,
    created_ts BIGINT NOT NULL,
    PRIMARY KEY (id, status)
) ENGINE=InnoDB
PARTITION BY RANGE (status) (
    PARTITION p20250301 VALUES LESS THAN (%d),
    PARTITION p20250302 VALUES LESS THAN (%d),
    PARTITION p20250303 VALUES LESS THAN (%d),
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
);`, base.Add(-24*time.Hour).Unix(), base.Unix(), base.Add(24*time.Hour).Unix())
		_, err := db.ExecContext(ctx, stmt)
		require.NoError(t, err)

		maintainer := newRetentionMaintainer(t, db, base, outbox.NopLogger{})
		err = maintainer.Ensure(ctx)
		require.ErrorIs(t, err, mysql.ErrPartitionedTableRequired)
		require.Contains(t, listPartitionNames(t, ctx, db, "outbox"), "p20250301")
	})

	t.Run("pending row blocks drop", func(t *testing.T) {
		resetRetentionSchema(t, ctx, db, base)
		store, err := mysql.NewStore(db)
		require.NoError(t, err)
		insertEntries(t, ctx, db, store, []outbox.Entry{entryWithID(pendingID, "pending")})
		require.True(t, rowExistsInPartition(t, ctx, db, "p20250301", pendingID))

		maintainer := newRetentionMaintainer(t, db, base, outbox.NopLogger{})
		err = maintainer.Ensure(ctx)
		require.ErrorContains(t, err, "non-terminal records")
		require.Contains(t, listPartitionNames(t, ctx, db, "outbox"), "p20250301")
		require.True(t, rowExistsInPartition(t, ctx, db, "p20250301", pendingID))
	})

	t.Run("terminal only partition drops", func(t *testing.T) {
		resetRetentionSchema(t, ctx, db, base)
		store, err := mysql.NewStore(db)
		require.NoError(t, err)
		insertEntries(t, ctx, db, store, []outbox.Entry{
			entryWithID(processedID, "processed"),
			entryWithID(deadID, "dead"),
		})
		_, err = db.ExecContext(ctx, "UPDATE outbox SET status = ? WHERE id = ?", outbox.StatusProcessed, processedID)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "UPDATE outbox SET status = ? WHERE id = ?", outbox.StatusDead, deadID)
		require.NoError(t, err)

		maintainer := newRetentionMaintainer(t, db, base, outbox.NopLogger{})
		require.NoError(t, maintainer.Ensure(ctx))
		require.NotContains(t, listPartitionNames(t, ctx, db, "outbox"), "p20250301")
		require.False(t, rowExists(t, ctx, db, processedID))
		require.False(t, rowExists(t, ctx, db, deadID))
	})

	t.Run("late enqueue cannot be lost", func(t *testing.T) {
		resetRetentionSchema(t, ctx, db, base)
		enqueueConn, err := db.Conn(ctx)
		require.NoError(t, err)
		defer enqueueConn.Close()
		var connectionID int64
		require.NoError(t, enqueueConn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connectionID))
		store, err := mysql.NewStore(db)
		require.NoError(t, err)

		tx, err := enqueueConn.BeginTx(ctx, nil)
		require.NoError(t, err)
		_, err = store.Enqueue(ctx, tx, entryWithID(lateID, "late"))
		require.NoError(t, err)

		maintainer := newRetentionMaintainer(t, db, base, outbox.NopLogger{})
		ensureDone := make(chan error, 1)
		go func() {
			ensureDone <- maintainer.Ensure(ctx)
		}()

		lockCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		blocked := false
		for !blocked && lockCtx.Err() == nil {
			blocked = hasOtherPendingMetadataLock(t, lockCtx, db, connectionID, "outbox")
		}

		if blocked {
			require.NoError(t, tx.Commit())
		} else {
			require.NoError(t, tx.Rollback())
		}
		ensureErr := <-ensureDone
		require.True(t, blocked, "late enqueue was not fenced by a pending metadata lock")
		require.ErrorContains(t, ensureErr, "non-terminal records")
		require.Contains(t, listPartitionNames(t, ctx, db, "outbox"), "p20250301")
		require.True(t, rowExistsInPartition(t, ctx, db, "p20250301", lateID))
	})
}

func setupPartitionedSchema(t *testing.T, ctx context.Context, db *sql.DB, base time.Time) {
	t.Helper()
	parts := []mysql.Partition{
		{Name: dayPartitionName(base.Add(-48 * time.Hour)), LessThan: fmt.Sprintf("%d", base.Add(-24*time.Hour).Unix())},
		{Name: dayPartitionName(base.Add(-24 * time.Hour)), LessThan: fmt.Sprintf("%d", base.Unix())},
		{Name: dayPartitionName(base), LessThan: fmt.Sprintf("%d", base.Add(24*time.Hour).Unix())},
		{Name: "pmax", LessThan: "MAXVALUE"},
	}
	schema, err := mysql.PartitionedSchema("outbox", parts)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, schema)
	require.NoError(t, err)
}

func newRetentionMaintainer(
	t *testing.T,
	db *sql.DB,
	base time.Time,
	logger outbox.Logger,
) *mysql.PartitionMaintainer {
	t.Helper()
	maintainer, err := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{
		Table:     "outbox",
		Period:    mysql.PartitionDay,
		Lookahead: time.Hour,
		Retention: 24 * time.Hour,
		Clock:     fixedClock{now: base.Add(12 * time.Hour)},
		Logger:    logger,
	})
	require.NoError(t, err)

	return maintainer
}

func resetRetentionSchema(t *testing.T, ctx context.Context, db *sql.DB, base time.Time) {
	t.Helper()
	dropOutboxTable(t, ctx, db)
	setupPartitionedSchema(t, ctx, db, base)
}

func dropOutboxTable(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS outbox")
	require.NoError(t, err)
}

func parseIntegrationID(t *testing.T, value string) outbox.ID {
	t.Helper()
	id, err := outbox.ParseID(value)
	require.NoError(t, err)

	return id
}

func hasOtherPendingMetadataLock(t *testing.T, ctx context.Context, db *sql.DB, connectionID int64, table string) bool {
	t.Helper()
	var count int
	err := db.QueryRowContext(ctx, `
SELECT COUNT(*)
FROM performance_schema.metadata_locks AS ml
JOIN performance_schema.threads AS th ON th.THREAD_ID = ml.OWNER_THREAD_ID
WHERE th.PROCESSLIST_ID <> ?
  AND ml.OBJECT_TYPE = 'TABLE'
  AND ml.OBJECT_SCHEMA = DATABASE()
  AND ml.OBJECT_NAME = ?
  AND ml.LOCK_STATUS = 'PENDING'
`, connectionID, table).Scan(&count)
	if ctx.Err() != nil {
		return false
	}
	require.NoError(t, err)

	return count > 0
}

func rowExists(t *testing.T, ctx context.Context, db *sql.DB, id outbox.ID) bool {
	t.Helper()
	var exists bool
	require.NoError(t, db.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM outbox WHERE id = ?)", id).Scan(&exists))

	return exists
}

func rowExistsInPartition(
	t *testing.T,
	ctx context.Context,
	db *sql.DB,
	partition string,
	id outbox.ID,
) bool {
	t.Helper()
	quotedPartition := fmt.Sprintf("`%s`", partition)
	query := fmt.Sprintf("SELECT EXISTS(SELECT 1 FROM outbox PARTITION (%s) WHERE id = ?)", quotedPartition)
	var exists bool
	require.NoError(t, db.QueryRowContext(ctx, query, id).Scan(&exists))

	return exists
}

func listPartitionNames(t *testing.T, ctx context.Context, db *sql.DB, table string) []string {
	t.Helper()
	rows, err := db.QueryContext(ctx, `
SELECT PARTITION_NAME
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL
`, table)
	require.NoError(t, err)
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		names = append(names, name)
	}
	require.NoError(t, rows.Err())

	return names
}

func dayPartitionName(start time.Time) string {
	start = start.UTC()
	return fmt.Sprintf("p%04d%02d%02d", start.Year(), int(start.Month()), start.Day())
}
