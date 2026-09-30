//go:build integration

package mysql_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/velmie/outbox/mysql"
)

func TestMaintenanceDiagnosticsIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test disabled in short mode")
	}
	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() { _ = db.Close(); _ = container.Terminate(ctx) })
	setupSchema(t, ctx, db)
	logger := &maintenanceLogger{}
	cleanup, err := mysql.NewCleanupMaintainer(db, mysql.CleanupMaintainerConfig{Retention: time.Hour, LockName: "diagnostics_cleanup", Logger: logger})
	require.NoError(t, err)
	holder, err := db.Conn(ctx)
	require.NoError(t, err)
	defer holder.Close()
	var locked int
	require.NoError(t, holder.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", "diagnostics_cleanup").Scan(&locked))
	require.Equal(t, 1, locked)
	_, err = cleanup.Ensure(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"cleanup.skipped"}, maintenanceEvents(logger))
	require.Equal(t, "lock_busy", logger.events[0]["reason"])
	require.NoError(t, holder.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", "diagnostics_cleanup").Scan(&locked))
	require.Equal(t, 1, locked)
	logger.events = nil
	_, err = cleanup.Ensure(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"cleanup.completed"}, maintenanceEvents(logger))
	dropOutboxTable(t, ctx, db)
	now := time.Now().UTC().Truncate(24 * time.Hour)
	setupPartitionedSchema(t, ctx, db, now)
	partitions, err := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{Table: "outbox", Period: mysql.PartitionDay, Lookahead: 24 * time.Hour, Retention: 24 * time.Hour, Clock: fixedClock{now: now.Add(12 * time.Hour)}, LockName: "diagnostics_partitions", Logger: logger})
	require.NoError(t, err)
	require.NoError(t, holder.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", "diagnostics_partitions").Scan(&locked))
	require.Equal(t, 1, locked)
	logger.events = nil
	require.NoError(t, partitions.Ensure(ctx))
	require.Equal(t, []string{"partitions.skipped"}, maintenanceEvents(logger))
	require.NoError(t, holder.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", "diagnostics_partitions").Scan(&locked))
	require.Equal(t, 1, locked)
	logger.events = nil
	require.NoError(t, partitions.Ensure(ctx))
	require.Equal(t, []string{"partitions.expansion_started", "partitions.expansion_completed", "partitions.dropped", "partitions.completed"}, maintenanceEvents(logger))
	require.Equal(t, "started", logger.events[0]["outcome"])
	for _, e := range logger.events[1:] {
		require.Equal(t, "succeeded", e["outcome"])
	}
	names := listPartitionNames(t, ctx, db, "outbox")
	require.NotContains(t, names, dayPartitionName(now.Add(-48*time.Hour)))
	require.Contains(t, names, dayPartitionName(now.Add(24*time.Hour)))
	logger.events = nil
	require.NoError(t, partitions.Ensure(ctx))
	require.Equal(t, []string{"partitions.completed"}, maintenanceEvents(logger))
}

func maintenanceEvents(logger *maintenanceLogger) []string {
	events := make([]string, 0, len(logger.events))
	for _, event := range logger.events {
		name, _ := event["event"].(string)
		events = append(events, name)
	}
	return events
}
