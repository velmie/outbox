//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

func TestStoreCleanupIntegration(t *testing.T) {
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

	store, err := mysql.NewStore(db)
	require.NoError(t, err)

	entries := []outbox.Entry{
		{AggregateType: "order", AggregateID: "1", EventType: "created", Payload: json.RawMessage(`{"id":1}`)},
		{AggregateType: "order", AggregateID: "2", EventType: "created", Payload: json.RawMessage(`{"id":2}`)},
		{AggregateType: "order", AggregateID: "3", EventType: "created", Payload: json.RawMessage(`{"id":3}`)},
	}
	insertEntries(t, ctx, db, store, entries)

	records, err := fetchAllRecords(ctx, db)
	require.NoError(t, err)
	require.Len(t, records, 3)

	now := time.Now().UTC()
	old := now.Add(-2 * time.Hour)
	recent := now.Add(-10 * time.Minute)

	setStatus(t, ctx, db, records[0], outbox.StatusProcessed, &old, &old)
	setStatus(t, ctx, db, records[1], outbox.StatusProcessed, &recent, &recent)
	setStatus(t, ctx, db, records[2], outbox.StatusDead, nil, &old)

	res, err := store.Cleanup(ctx, mysql.CleanupOptions{
		Before:      now.Add(-1 * time.Hour),
		Limit:       10,
		IncludeDead: true,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Processed)
	require.EqualValues(t, 1, res.Dead)

	require.Equal(t, 1, countByStatus(t, ctx, db, outbox.StatusProcessed))
	require.Equal(t, 0, countByStatus(t, ctx, db, outbox.StatusDead))
}

func TestStoreCleanupLimitIntegration(t *testing.T) {
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

	store, err := mysql.NewStore(db)
	require.NoError(t, err)

	entries := []outbox.Entry{
		{AggregateType: "order", AggregateID: "1", EventType: "created", Payload: json.RawMessage(`{"id":1}`)},
		{AggregateType: "order", AggregateID: "2", EventType: "created", Payload: json.RawMessage(`{"id":2}`)},
		{AggregateType: "order", AggregateID: "3", EventType: "created", Payload: json.RawMessage(`{"id":3}`)},
	}
	insertEntries(t, ctx, db, store, entries)

	records, err := fetchAllRecords(ctx, db)
	require.NoError(t, err)
	require.Len(t, records, 3)

	now := time.Now().UTC()
	old := now.Add(-2 * time.Hour)
	for _, id := range records {
		setStatus(t, ctx, db, id, outbox.StatusProcessed, &old, &old)
	}

	res, err := store.Cleanup(ctx, mysql.CleanupOptions{
		Before: now.Add(-1 * time.Hour),
		Limit:  1,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, res.Processed)
	require.Equal(t, 2, countByStatus(t, ctx, db, outbox.StatusProcessed))

	res, err = store.Cleanup(ctx, mysql.CleanupOptions{
		Before: now.Add(-1 * time.Hour),
		Limit:  5,
	})
	require.NoError(t, err)
	require.EqualValues(t, 2, res.Processed)
	require.Equal(t, 0, countByStatus(t, ctx, db, outbox.StatusProcessed))
}

func fetchAllRecords(ctx context.Context, db *sql.DB) ([]outbox.ID, error) {
	rows, err := db.QueryContext(ctx, "SELECT id FROM outbox ORDER BY id ASC")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []outbox.ID
	for rows.Next() {
		var id outbox.ID
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return ids, nil
}

func setStatus(t *testing.T, ctx context.Context, db *sql.DB, id outbox.ID, status outbox.Status, processedAt, updatedAt *time.Time) {
	t.Helper()
	var processed any
	if processedAt != nil {
		processed = *processedAt
	}
	var updated any
	if updatedAt != nil {
		updated = *updatedAt
	}
	_, err := db.ExecContext(
		ctx,
		"UPDATE outbox SET status = ?, processed_at = ?, updated_at = ? WHERE id = ?",
		status,
		processed,
		updated,
		id,
	)
	require.NoError(t, err)
}
