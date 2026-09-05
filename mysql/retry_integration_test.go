//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

func TestStoreRetrySchedulingIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test disabled in short mode")
	}
	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() { _ = db.Close(); _ = container.Terminate(ctx) })

	t.Run("committed delay survives consumers and permits unrelated work", func(t *testing.T) {
		resetRetrySchema(t, ctx, db)
		first := retryStore(t, db)
		second := retryStore(t, db)
		insertEntries(t, ctx, db, first, []outbox.Entry{validIntegrationEntry("delayed")})
		batch, err := first.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
		require.NoError(t, err)
		id := batch.Records()[0].ID
		var earliest time.Time
		require.NoError(t, db.QueryRowContext(ctx, "SELECT DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 1 HOUR)").Scan(&earliest))
		require.NoError(t, batch.Fail(ctx, []outbox.Failure{{ID: id, Err: errors.New("temporary")}}))
		require.NoError(t, batch.Commit())
		state := readRetryState(t, ctx, db, id)
		require.Equal(t, outbox.StatusPending, state.status)
		require.Equal(t, 1, state.attempts)
		require.Equal(t, sql.NullString{String: "temporary", Valid: true}, state.lastError)
		require.True(t, state.deadline.Valid)
		require.False(t, state.deadline.Time.Before(earliest))
		var latest time.Time
		require.NoError(t, db.QueryRowContext(ctx, "SELECT DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 1 HOUR)").Scan(&latest))
		require.False(t, state.deadline.Time.After(latest))
		requireNoRetryRecords(t, ctx, second)
		requireNoRetryRecords(t, ctx, retryStore(t, db))
		require.Equal(t, state, readRetryState(t, ctx, db, id), "polling must not consume attempts")

		insertEntries(t, ctx, db, first, []outbox.Entry{validIntegrationEntry("ready")})
		batch, err = second.Fetch(ctx, outbox.FetchOptions{BatchSize: 2})
		require.NoError(t, err)
		require.Len(t, batch.Records(), 1)
		require.NotEqual(t, id, batch.Records()[0].ID)
		require.NoError(t, batch.Ack(ctx, collectIDs(batch.Records())))
		require.NoError(t, batch.Commit())
		pending, err := first.PendingCount(ctx)
		require.NoError(t, err)
		require.Equal(t, 1, pending)
		_, err = first.Cleanup(ctx, mysql.CleanupOptions{Before: time.Now().UTC().Add(24 * time.Hour), Limit: 10, IncludeDead: true})
		require.NoError(t, err)
		require.Equal(t, state, readRetryState(t, ctx, db, id), "cleanup must preserve delayed pending records")

		makeRetryDue(t, ctx, db, id)
		batch, err = second.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
		require.NoError(t, err)
		require.Equal(t, id, batch.Records()[0].ID, "retry keeps the delivery ID")
		require.NoError(t, batch.Ack(ctx, []outbox.ID{id}))
		require.NoError(t, batch.Commit())
		state = readRetryState(t, ctx, db, id)
		require.Equal(t, outbox.StatusProcessed, state.status)
		require.Equal(t, 1, state.attempts)
		require.False(t, state.lastError.Valid)
		requireNoRetryRecords(t, ctx, first)
	})

	t.Run("uncommitted failure is locked and rollback restores state", func(t *testing.T) {
		resetRetrySchema(t, ctx, db)
		first, second := retryStore(t, db), retryStore(t, db)
		insertEntries(t, ctx, db, first, []outbox.Entry{validIntegrationEntry("rollback")})
		batch, err := first.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
		require.NoError(t, err)
		id := batch.Records()[0].ID
		before := readRetryState(t, ctx, db, id)
		require.False(t, before.deadline.Valid)
		require.NoError(t, batch.Fail(ctx, []outbox.Failure{{ID: id, Err: errors.New("temporary")}}))
		requireNoRetryRecords(t, ctx, second)
		require.Equal(t, before, readRetryState(t, ctx, db, id))
		require.NoError(t, batch.Rollback())
		require.Equal(t, before, readRetryState(t, ctx, db, id))
		batch, err = second.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
		require.NoError(t, err)
		require.Equal(t, id, batch.Records()[0].ID)
		require.NoError(t, batch.Fail(ctx, []outbox.Failure{{ID: id, Err: errors.New("committed")}}))
		require.NoError(t, batch.Commit())
		makeRetryDue(t, ctx, db, id)
		before = readRetryState(t, ctx, db, id)
		batch, err = first.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
		require.NoError(t, err)
		require.NoError(t, batch.Fail(ctx, []outbox.Failure{{ID: id, Err: errors.New("rolled back")}}))
		requireNoRetryRecords(t, ctx, second)
		require.NoError(t, batch.Rollback())
		require.Equal(t, before, readRetryState(t, ctx, db, id), "rollback preserves the previous deadline and failure")
	})

	t.Run("exhausted and permanent failures stop delivery", func(t *testing.T) {
		for _, permanent := range []bool{false, true} {
			resetRetrySchema(t, ctx, db)
			store := retryStore(t, db)
			insertEntries(t, ctx, db, store, []outbox.Entry{validIntegrationEntry("terminal")})
			batch, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
			require.NoError(t, err)
			id := batch.Records()[0].ID
			failure := []outbox.Failure{{ID: id, Err: errors.New("failure")}}
			require.NoError(t, batch.Fail(ctx, failure))
			require.NoError(t, batch.Commit())
			makeRetryDue(t, ctx, db, id)
			batch, err = store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
			require.NoError(t, err)
			if permanent {
				deadBatch, ok := batch.(outbox.DeadBatch)
				require.True(t, ok)
				err = deadBatch.Dead(ctx, failure)
			} else {
				err = batch.Fail(ctx, failure)
			}
			require.NoError(t, err)
			require.NoError(t, batch.Commit())
			state := readRetryState(t, ctx, db, id)
			require.Equal(t, outbox.StatusDead, state.status)
			require.Equal(t, 2, state.attempts)
			require.True(t, state.lastError.Valid)
			requireNoRetryRecords(t, ctx, retryStore(t, db))
		}
	})

	t.Run("due records retain ID order", func(t *testing.T) {
		resetRetrySchema(t, ctx, db)
		store := retryStore(t, db)
		older, err := outbox.ParseID("017f22e2-79b0-7cc3-98c4-dc0c0c07398f")
		require.NoError(t, err)
		newer, err := outbox.ParseID("017f22e2-79b1-7cc3-98c4-dc0c0c07398f")
		require.NoError(t, err)
		insertEntries(t, ctx, db, store, []outbox.Entry{entryWithID(newer, "newer"), entryWithID(older, "older")})
		makeRetryDue(t, ctx, db, newer)
		batch, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 2})
		require.NoError(t, err)
		require.Equal(t, []outbox.ID{older, newer}, collectIDs(batch.Records()))
		require.NoError(t, batch.Rollback())
	})

	t.Run("submicrosecond delay is due exactly at its UTC deadline", func(t *testing.T) {
		resetRetrySchema(t, ctx, db)
		db.SetMaxOpenConns(1)
		_, err := db.ExecContext(ctx, "SET timestamp = 2000000000")
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, "SET time_zone = '+05:30'")
		require.NoError(t, err)
		defer func() {
			_, resetErr := db.ExecContext(ctx, "SET timestamp = DEFAULT")
			require.NoError(t, resetErr)
			_, resetErr = db.ExecContext(ctx, "SET time_zone = DEFAULT")
			require.NoError(t, resetErr)
			db.SetMaxOpenConns(0)
		}()
		store, err := mysql.NewStore(db, mysql.WithRetryDelay(time.Nanosecond))
		require.NoError(t, err)
		insertEntries(t, ctx, db, store, []outbox.Entry{validIntegrationEntry("precision")})
		var now time.Time
		require.NoError(t, db.QueryRowContext(ctx, "SELECT UTC_TIMESTAMP(6)").Scan(&now))
		batch, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
		require.NoError(t, err)
		id := batch.Records()[0].ID
		require.NoError(t, batch.Fail(ctx, []outbox.Failure{{ID: id, Err: errors.New("temporary")}}))
		require.NoError(t, batch.Commit())
		require.Equal(t, now.Add(time.Microsecond), readRetryState(t, ctx, db, id).deadline.Time)
		requireNoRetryRecords(t, ctx, store)
		_, err = db.ExecContext(ctx, "SET timestamp = 2000000000.000001")
		require.NoError(t, err)
		batch, err = store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
		require.NoError(t, err)
		require.Equal(t, id, batch.Records()[0].ID)
		require.NoError(t, batch.Rollback())
	})

	t.Run("existing pending rows survive explicit migration", func(t *testing.T) {
		partitions := []mysql.Partition{{Name: "pmax", LessThan: "MAXVALUE"}}
		for _, tt := range []struct {
			name   string
			binary bool
			build  func() (string, error)
		}{
			{name: "json", build: func() (string, error) { return mysql.Schema("outbox") }},
			{name: "binary", binary: true, build: func() (string, error) { return mysql.SchemaBinary("outbox") }},
			{name: "partitioned json", build: func() (string, error) { return mysql.PartitionedSchema("outbox", partitions) }},
			{name: "partitioned binary", binary: true, build: func() (string, error) { return mysql.PartitionedSchemaBinary("outbox", partitions) }},
		} {
			t.Run(tt.name, func(t *testing.T) {
				_, err := db.ExecContext(ctx, "DROP TABLE outbox")
				require.NoError(t, err)
				schema, err := tt.build()
				require.NoError(t, err)
				_, err = db.ExecContext(ctx, schema)
				require.NoError(t, err)
				legacy, err := mysql.NewStore(db, mysql.WithValidatePayload(!tt.binary))
				require.NoError(t, err)
				entry := validIntegrationEntry("existing")
				if tt.binary {
					entry.Payload = []byte{0, 0xff}
				}
				insertEntries(t, ctx, db, legacy, []outbox.Entry{entry})
				var columns int
				require.NoError(t, db.QueryRowContext(ctx,
					"SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'outbox' AND COLUMN_NAME = 'next_attempt_at'",
				).Scan(&columns))
				require.Zero(t, columns)
				legacyBatch, err := legacy.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
				require.NoError(t, err)
				legacyID := legacyBatch.Records()[0].ID
				require.NoError(t, legacyBatch.Fail(ctx, []outbox.Failure{{ID: legacyID, Err: errors.New("legacy retry")}}))
				require.NoError(t, legacyBatch.Commit())
				legacyBatch, err = legacy.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
				require.NoError(t, err)
				require.Equal(t, legacyID, legacyBatch.Records()[0].ID)
				require.NoError(t, legacyBatch.Rollback())

				_, err = db.ExecContext(ctx, "ALTER TABLE outbox ADD COLUMN next_attempt_at DATETIME(6) NULL, ADD INDEX idx_status_next_attempt_id (status, next_attempt_at, id)")
				require.NoError(t, err)
				store, err := mysql.NewStore(db, mysql.WithRetryDelay(time.Hour))
				require.NoError(t, err)
				batch, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
				require.NoError(t, err)
				require.Len(t, batch.Records(), 1)
				if tt.binary {
					require.Equal(t, entry.Payload, batch.Records()[0].Payload)
				}
				id := batch.Records()[0].ID
				require.NoError(t, batch.Fail(ctx, []outbox.Failure{{ID: id, Err: errors.New("temporary")}}))
				require.NoError(t, batch.Commit())
				requireNoRetryRecords(t, ctx, retryStore(t, db))
				require.Equal(t, 2, readRetryState(t, ctx, db, id).attempts)
				legacyBatch, err = legacy.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
				require.NoError(t, err, "a scheduling-disabled consumer ignores a committed future deadline")
				require.Equal(t, id, legacyBatch.Records()[0].ID)
				require.NoError(t, legacyBatch.Rollback())
			})
		}
	})

	t.Run("relay retry keeps delivery ID and waits across relay instances", func(t *testing.T) {
		resetRetrySchema(t, ctx, db)
		first, second := retryStore(t, db), retryStore(t, db)
		insertEntries(t, ctx, db, first, []outbox.Entry{validIntegrationEntry("relay")})
		var ids []outbox.ID
		handler := outbox.HandlerFunc(func(_ context.Context, record outbox.Record) error {
			ids = append(ids, record.ID)
			if len(ids) == 1 {
				return errors.New("temporary delivery failure")
			}
			return nil
		})
		relay := outbox.NewRelay(first, handler)
		processed, err := relay.ProcessOnce(ctx)
		require.NoError(t, err)
		require.True(t, processed)
		require.Len(t, ids, 1)
		other := outbox.NewRelay(second, handler)
		processed, err = other.ProcessOnce(ctx)
		require.NoError(t, err)
		require.False(t, processed)
		require.Len(t, ids, 1)
		makeRetryDue(t, ctx, db, ids[0])
		processed, err = other.ProcessOnce(ctx)
		require.NoError(t, err)
		require.True(t, processed)
		require.Equal(t, []outbox.ID{ids[0], ids[0]}, ids)
		require.Equal(t, outbox.StatusProcessed, readRetryState(t, ctx, db, ids[0]).status)
	})

	t.Run("retry mode rejects missing retry column", func(t *testing.T) {
		_, err := db.ExecContext(ctx, "DROP TABLE outbox")
		require.NoError(t, err)
		setupSchema(t, ctx, db)
		store := retryStore(t, db)
		batch, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
		if batch != nil {
			require.NoError(t, batch.Rollback())
		}
		require.Error(t, err)
		require.NotErrorIs(t, err, outbox.ErrNoRecords)
	})
}

type retryState struct {
	status    outbox.Status
	attempts  int
	lastError sql.NullString
	deadline  sql.NullTime
}

func resetRetrySchema(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS outbox")
	require.NoError(t, err)
	schema, err := mysql.RetrySchema("outbox")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, schema)
	require.NoError(t, err)
}

func retryStore(t *testing.T, db *sql.DB) *mysql.Store {
	t.Helper()
	store, err := mysql.NewStore(db, mysql.WithRetryDelay(time.Hour), mysql.WithMaxAttempts(2),
		mysql.WithClock(fixedClock{now: time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)}))
	require.NoError(t, err)
	return store
}

func readRetryState(t *testing.T, ctx context.Context, db *sql.DB, id outbox.ID) retryState {
	t.Helper()
	var state retryState
	err := db.QueryRowContext(ctx, "SELECT status, attempt_count, last_error, next_attempt_at FROM outbox WHERE id = ?", id).
		Scan(&state.status, &state.attempts, &state.lastError, &state.deadline)
	require.NoError(t, err)
	return state
}

func requireNoRetryRecords(t *testing.T, ctx context.Context, store *mysql.Store) {
	t.Helper()
	batch, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 10})
	if batch != nil {
		require.NoError(t, batch.Rollback())
	}
	require.ErrorIs(t, err, outbox.ErrNoRecords)
}

func makeRetryDue(t *testing.T, ctx context.Context, db *sql.DB, id outbox.ID) {
	t.Helper()
	_, err := db.ExecContext(ctx, "UPDATE outbox SET next_attempt_at = DATE_SUB(UTC_TIMESTAMP(6), INTERVAL 1 SECOND) WHERE id = ?", id)
	require.NoError(t, err)
}
