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

func TestStoreExplicitRetrySchedulingIntegration(t *testing.T) {
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
	ddl, err := mysql.RetrySchema("outbox")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, ddl)
	require.NoError(t, err)
	store, err := mysql.NewStore(db, mysql.WithRetryScheduling())
	require.NoError(t, err)
	id, err := store.Enqueue(ctx, db, validIntegrationEntry("hints-only"))
	require.NoError(t, err)
	relay := outbox.NewRelay(
		store,
		outbox.HandlerFunc(
			func(context.Context, outbox.Record) error {
				return outbox.RetryAfter(context.DeadlineExceeded, time.Hour)
			},
		),
	)
	processed, err := relay.ProcessOnce(ctx)
	require.NoError(t, err)
	require.True(t, processed)
	state := readRetryState(t, ctx, db, id)
	require.Equal(t, 1, state.attempts)
	require.True(t, state.deadline.Valid)
	_, err = store.Fetch(
		ctx,
		outbox.FetchOptions{
			BatchSize: 1,
		},
	)
	require.ErrorIs(t, err, outbox.ErrNoRecords)
	_, err = db.ExecContext(ctx, "UPDATE outbox SET status = ? WHERE id = ?", outbox.StatusDead, id)
	require.NoError(t, err)
	count, err := store.Requeue(
		ctx,
		mysql.RequeueOptions{
			IDs:   []outbox.ID{id},
			Limit: 1,
		},
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
	require.False(t, readRetryState(t, ctx, db, id).deadline.Valid)
	batch, err := store.Fetch(
		ctx,
		outbox.FetchOptions{
			BatchSize: 1,
		},
	)
	require.NoError(t, err)
	require.Equal(t, id, batch.Records()[0].ID)
	require.NoError(
		t,
		batch.Fail(
			ctx,
			[]outbox.Failure{
				{
					ID: id,
				},
			},
		),
	)
	require.NoError(t, batch.Commit())
	batch, err = store.Fetch(
		ctx,
		outbox.FetchOptions{
			BatchSize: 1,
		},
	)
	require.NoError(t, err, "without a hint or base delay the row remains immediately eligible")
	require.Equal(t, id, batch.Records()[0].ID)
	require.NoError(t, batch.Rollback())
}
