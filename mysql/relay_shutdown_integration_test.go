//go:build integration

package mysql_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

func TestRelayShutdownTransactionsIntegration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
		if err := container.Terminate(context.Background()); err != nil {
			t.Error(err)
		}
	})
	setupSchema(t, ctx, db)
	store, err := mysql.NewStore(db)
	require.NoError(t, err)

	t.Run("workers drain admitted rows and leave new backlog", func(t *testing.T) {
		insertEntries(t, ctx, db, store, []outbox.Entry{
			validIntegrationEntry("first"),
			validIntegrationEntry("second"),
		})
		started := make(chan struct{}, 2)
		release := make(chan struct{})
		parent, stop := context.WithCancel(ctx)
		defer stop()
		done := make(chan error, 1)
		handler := outbox.HandlerFunc(func(context.Context, outbox.Record) error {
			started <- struct{}{}
			<-release
			return nil
		})
		relay := outbox.NewRelay(store, handler, outbox.WithWorkers(2), outbox.WithBatchSize(1))
		go func() { done <- relay.Run(parent) }()
		<-started
		<-started
		stop()
		insertEntries(t, ctx, db, store, []outbox.Entry{validIntegrationEntry("backlog")})
		close(release)
		require.NoError(t, <-done)
		require.Equal(t, 2, countByStatus(t, ctx, db, outbox.StatusProcessed))
		require.Equal(t, 1, countByStatus(t, ctx, db, outbox.StatusPending))
	})

	t.Run("timeout joins handler and releases its row lock", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "DELETE FROM outbox")
		require.NoError(t, err)
		insertEntries(t, ctx, db, store, []outbox.Entry{validIntegrationEntry("stalled")})
		failure := errors.New("publication interrupted")
		started := make(chan struct{})
		expired := make(chan struct{})
		release := make(chan struct{})
		parent, stop := context.WithCancel(ctx)
		defer stop()
		done := make(chan error, 1)
		handler := outbox.HandlerFunc(func(ctx context.Context, _ outbox.Record) error {
			close(started)
			<-ctx.Done()
			close(expired)
			<-release
			return failure
		})
		relay := outbox.NewRelay(store, handler, outbox.WithShutdownTimeout(50*time.Millisecond))
		go func() { done <- relay.Run(parent) }()
		<-started
		stop()
		<-expired
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		var status outbox.Status
		require.NoError(t, tx.QueryRowContext(ctx, "SELECT status FROM outbox FOR UPDATE").Scan(&status))
		require.Equal(t, outbox.StatusPending, status)
		require.NoError(t, tx.Rollback())
		select {
		case err := <-done:
			t.Fatalf("returned before handler finished: %v", err)
		default:
		}
		close(release)
		err = <-done
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.ErrorIs(t, err, failure)
		require.Equal(t, 1, countByStatus(t, ctx, db, outbox.StatusPending))
	})
}
