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

func TestScopedStoresIsolation(t *testing.T) {
	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, container.Terminate(ctx))
	})
	ddl, err := mysql.SchemaWithOptions("outbox", mysql.SchemaOptions{
		Retry:              true,
		AggregateTypeIndex: true,
	})
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, ddl)
	require.NoError(t, err)
	producer, err := mysql.NewStore(db)
	require.NoError(t, err)
	first, err := mysql.NewStore(db, mysql.WithAggregateType("first"), mysql.WithRetryDelay(time.Hour))
	require.NoError(t, err)
	second, err := mysql.NewStore(db, mysql.WithAggregateType("second"), mysql.WithRetryDelay(time.Hour))
	require.NoError(t, err)
	insertEntries(t, ctx, db, producer, []outbox.Entry{
		{
			AggregateType: "first",
			AggregateID:   "a",
			EventType:     "delivery",
			Payload:       []byte(`{}`),
		},
		{
			AggregateType: "second",
			AggregateID:   "b",
			EventType:     "delivery",
			Payload:       []byte(`{}`),
		},
	})
	batchA, err := first.Fetch(ctx, outbox.FetchOptions{
		BatchSize: 1,
	})
	require.NoError(t, err)
	require.Equal(t, "first", batchA.Records()[0].AggregateType)
	batchB, err := second.Fetch(ctx, outbox.FetchOptions{
		BatchSize: 1,
	})
	require.NoError(t, err)
	require.Equal(t, "second", batchB.Records()[0].AggregateType)
	idA := batchA.Records()[0].ID
	require.NoError(t, batchA.Rollback())
	require.NoError(t, batchB.Ack(ctx, []outbox.ID{idA}))
	require.NoError(t, batchB.Fail(ctx, []outbox.Failure{{
		ID: idA,
	}}))
	deadBatch, ok := batchB.(outbox.DeadBatch)
	require.True(t, ok)
	require.NoError(t, deadBatch.Dead(ctx, []outbox.Failure{{
		ID: idA,
	}}))
	require.NoError(t, batchB.Ack(ctx, []outbox.ID{batchB.Records()[0].ID}))
	require.NoError(t, batchB.Commit())
	status, attempts := fetchStatus(t, ctx, db, idA)
	require.Equal(t, outbox.StatusPending, status)
	require.Zero(t, attempts)
	batchA, err = first.Fetch(ctx, outbox.FetchOptions{
		BatchSize: 1,
	})
	require.NoError(t, err)
	require.NoError(t, batchA.Fail(ctx, []outbox.Failure{{
		ID: idA,
	}}))
	require.NoError(t, batchA.Commit())
	count, err := first.PendingCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)
	count, err = second.PendingCount(ctx)
	require.NoError(t, err)
	require.Zero(t, count)
	_, err = first.Fetch(ctx, outbox.FetchOptions{
		BatchSize: 1,
	})
	require.ErrorIs(t, err, outbox.ErrNoRecords)
	_, err = first.Enqueue(ctx, db, outbox.Entry{
		AggregateType: "second",
		AggregateID:   "c",
		EventType:     "delivery",
		Payload:       []byte(`{}`),
	})
	require.ErrorContains(t, err, "aggregate type")
	_, err = db.ExecContext(ctx, "UPDATE outbox SET status = ?, processed_at = UTC_TIMESTAMP(6) WHERE id = ?", outbox.StatusProcessed, idA)
	require.NoError(t, err)
	result, err := second.Cleanup(ctx, mysql.CleanupOptions{
		Before: time.Now().Add(time.Hour),
	})
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Processed)
	status, _ = fetchStatus(t, ctx, db, idA)
	require.Equal(t, outbox.StatusProcessed, status)
	_, err = db.ExecContext(
		ctx,
		"UPDATE outbox SET status = ?, updated_at = UTC_TIMESTAMP(6) WHERE id = ?",
		outbox.StatusDead,
		idA,
	)
	require.NoError(t, err)
	result, err = second.Cleanup(ctx, mysql.CleanupOptions{
		Before:      time.Now().Add(time.Hour),
		IncludeDead: true,
	})
	require.NoError(t, err)
	require.Zero(t, result.Dead)
	status, _ = fetchStatus(t, ctx, db, idA)
	require.Equal(t, outbox.StatusDead, status)
}
