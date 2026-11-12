//go:build integration

package mysql_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

func TestRequeueDeadDelivery(t *testing.T) {
	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, container.Terminate(ctx))
	})
	ddl, err := mysql.SchemaWithOptions("outbox", mysql.SchemaOptions{
		Retry:         true,
		Deduplication: true,
	})
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, ddl)
	require.NoError(t, err)
	store, err := mysql.NewStore(db, mysql.WithRetryScheduling())
	require.NoError(t, err)
	entry := outbox.Entry{
		AggregateType: "endpoint",
		AggregateID:   "route",
		EventType:     "delivery",
		Payload:       []byte(`{"test":true}`),
		DedupKey:      "identity",
	}
	id, err := store.Enqueue(ctx, db, entry)
	require.NoError(t, err)
	var createdAt time.Time
	require.NoError(t, db.QueryRowContext(ctx, "SELECT created_at FROM outbox WHERE id = ?", id).Scan(&createdAt))
	_, err = db.ExecContext(
		ctx,
		`UPDATE outbox SET status = ?, attempt_count = 5, last_error = 'failure',
processed_at = UTC_TIMESTAMP(6), next_attempt_at = DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 1 DAY) WHERE id = ?`,
		outbox.StatusDead,
		id,
	)
	require.NoError(t, err)
	count, err := store.Requeue(ctx, mysql.RequeueOptions{
		IDs:   []outbox.ID{id},
		Limit: 1,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	state := readRetryState(t, ctx, db, id)
	require.Equal(t, outbox.StatusPending, state.status)
	require.Zero(t, state.attempts)
	require.False(t, state.lastError.Valid)
	require.False(t, state.deadline.Valid)
	var afterCreatedAt time.Time
	var payload string
	var dedup []byte
	var processedNull bool
	require.NoError(t, db.QueryRowContext(
		ctx,
		"SELECT created_at, payload, dedup_key, processed_at IS NULL FROM outbox WHERE id = ?",
		id,
	).Scan(&afterCreatedAt, &payload, &dedup, &processedNull))
	require.Equal(t, createdAt, afterCreatedAt)
	require.JSONEq(t, string(entry.Payload), payload)
	require.Equal(t, []byte(entry.DedupKey), dedup)
	require.True(t, processedNull)
	var got outbox.ID
	relay := outbox.NewRelay(store, outbox.HandlerFunc(func(_ context.Context, record outbox.Record) error {
		got = record.ID
		return nil
	}))
	processed, err := relay.ProcessOnce(ctx)
	require.NoError(t, err)
	require.True(t, processed)
	require.Equal(t, id, got)
	_, err = store.Enqueue(ctx, db, entry)
	require.ErrorIs(t, err, outbox.ErrDuplicate)

	legacyID, err := outbox.ParseID("550e8400-e29b-41d4-a716-446655440000")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO outbox
(id, aggregate_type, aggregate_id, event_type, payload, status, attempt_count, dedup_key)
VALUES (?, 'endpoint', 'legacy', 'delivery', '{}', ?, 3, 'legacy-key')`, legacyID, outbox.StatusDead)
	require.NoError(t, err)
	other := entry
	other.AggregateType = "other"
	other.DedupKey = "other"
	otherID, err := store.Enqueue(ctx, db, other)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "UPDATE outbox SET status = ? WHERE id = ?", outbox.StatusDead, otherID)
	require.NoError(t, err)
	scoped, err := mysql.NewStore(db, mysql.WithAggregateType("endpoint"))
	require.NoError(t, err)
	_, err = scoped.Requeue(ctx, mysql.RequeueOptions{
		AggregateType: "other",
		Limit:         1,
	})
	require.ErrorContains(t, err, "conflicts")
	_, err = store.Requeue(ctx, mysql.RequeueOptions{
		Limit: 1,
	})
	require.ErrorContains(t, err, "selector")
	_, err = store.Requeue(ctx, mysql.RequeueOptions{
		EventType: "delivery",
	})
	require.ErrorContains(t, err, "limit")
	count, err = scoped.Requeue(ctx, mysql.RequeueOptions{
		IDs:       []outbox.ID{legacyID, otherID},
		EventType: "different",
		Limit:     2,
	})
	require.NoError(t, err)
	require.Zero(t, count)
	count, err = scoped.Requeue(ctx, mysql.RequeueOptions{
		IDs:       []outbox.ID{legacyID, otherID},
		EventType: "delivery",
		Limit:     1,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	status, attempts := fetchStatus(t, ctx, db, legacyID)
	require.Equal(t, outbox.StatusPending, status)
	require.Zero(t, attempts)
	status, _ = fetchStatus(t, ctx, db, otherID)
	require.Equal(t, outbox.StatusDead, status)
	count, err = scoped.Requeue(ctx, mysql.RequeueOptions{
		Limit: 1,
	})
	require.NoError(t, err)
	require.Zero(t, count)

	for i := 0; i < 4; i++ {
		entry.DedupKey = ""
		newID, insertErr := store.Enqueue(ctx, db, entry)
		require.NoError(t, insertErr)
		_, updateErr := db.ExecContext(ctx, "UPDATE outbox SET status = ? WHERE id = ?", outbox.StatusDead, newID)
		require.NoError(t, updateErr)
	}
	type requeueResult struct {
		count int64
		err   error
	}
	results := make(chan requeueResult, 2)
	for i := 0; i < 2; i++ {
		go func() {
			updated, updateErr := scoped.Requeue(ctx, mysql.RequeueOptions{
				Limit: 2,
			})
			results <- requeueResult{
				count: updated,
				err:   updateErr,
			}
		}()
	}
	first, second := <-results, <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.EqualValues(t, 4, first.count+second.count)
	count, err = scoped.Requeue(ctx, mysql.RequeueOptions{
		Limit: 2,
	})
	require.NoError(t, err)
	require.Zero(t, count)

	legacyDDL, err := mysql.Schema("legacy_outbox")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, legacyDDL)
	require.NoError(t, err)
	legacyStore, err := mysql.NewStore(db, mysql.WithTable("legacy_outbox"))
	require.NoError(t, err)
	legacyEntryID, err := legacyStore.Enqueue(ctx, db, entry)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "UPDATE legacy_outbox SET status = ? WHERE id = ?", outbox.StatusDead, legacyEntryID)
	require.NoError(t, err)
	count, err = legacyStore.Requeue(ctx, mysql.RequeueOptions{
		IDs:   []outbox.ID{legacyEntryID},
		Limit: 1,
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}
