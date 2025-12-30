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
