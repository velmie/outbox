//go:build integration

package mysql_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

const mysqlImage = "mysql:8.4.11@sha256:b3b90af2a6552ae30c266fdb7d5dd55f3afb72404bb78d37fe8a23eb857fd3fb"

func TestStoreEnqueueFetchAckIntegration(t *testing.T) {
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

	records := []outbox.Entry{
		{AggregateType: "order", AggregateID: "1", EventType: "created", Payload: json.RawMessage(`{"id":1}`)},
		{AggregateType: "order", AggregateID: "2", EventType: "created", Payload: json.RawMessage(`{"id":2}`)},
		{AggregateType: "order", AggregateID: "3", EventType: "created", Payload: json.RawMessage(`{"id":3}`)},
	}
	insertEntries(t, ctx, db, store, records)

	batch1, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 2})
	require.NoError(t, err)
	ids1 := collectIDs(batch1.Records())
	require.Len(t, ids1, 2)
	require.NoError(t, batch1.Ack(ctx, ids1))
	require.NoError(t, batch1.Commit())

	batch2, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 10})
	require.NoError(t, err)
	ids2 := collectIDs(batch2.Records())
	require.Len(t, ids2, 1)
	require.NoError(t, batch2.Ack(ctx, ids2))
	require.NoError(t, batch2.Commit())

	_, err = store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.Error(t, err)
	require.ErrorIs(t, err, outbox.ErrNoRecords)

	require.Equal(t, 3, countByStatus(t, ctx, db, outbox.StatusProcessed))
}

func TestStorePendingEligibilityIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test disabled in short mode")
	}

	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	})

	now := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	oldID, err := outbox.ParseID("017f22e2-79b0-7cc3-98c4-dc0c0c07398f")
	require.NoError(t, err)

	t.Run("direct cutoff is ignored", func(t *testing.T) {
		resetPartitionedSchema(t, ctx, db, now)
		store := newFixedClockStore(t, db, now)
		insertEntries(t, ctx, db, store, []outbox.Entry{entryWithID(oldID, "old")})
		requireOldCreatedTS(t, ctx, db, oldID, now.Add(-time.Hour))

		batch, err := store.Fetch(ctx, outbox.FetchOptions{
			BatchSize:    1,
			MinCreatedAt: now,
		})
		require.NoError(t, err)
		require.Equal(t, []outbox.ID{oldID}, collectIDs(batch.Records()))
		require.NoError(t, batch.Rollback())
	})

	t.Run("old record remains eligible while new records arrive", func(t *testing.T) {
		resetPartitionedSchema(t, ctx, db, now)
		store := newFixedClockStore(t, db, now)
		insertEntries(t, ctx, db, store, []outbox.Entry{entryWithID(oldID, "old")})
		requireOldCreatedTS(t, ctx, db, oldID, now.Add(-time.Hour))

		seen := make(map[outbox.ID]bool)
		relay := outbox.NewRelay(
			store,
			outbox.HandlerFunc(func(_ context.Context, record outbox.Record) error {
				seen[record.ID] = true

				return nil
			}),
			outbox.WithBatchSize(1),
			outbox.WithClock(fixedClock{now: now}),
			outbox.WithPartitionWindow(time.Hour),
		)

		for i := 0; i < 3; i++ {
			insertEntries(t, ctx, db, store, []outbox.Entry{validIntegrationEntry(fmt.Sprintf("new-%d", i))})
			processed, err := relay.ProcessOnce(ctx)
			require.NoError(t, err)
			require.True(t, processed)
		}

		require.True(t, seen[oldID], "old pending record was not processed")
		status, _ := fetchStatus(t, ctx, db, oldID)
		require.Equal(t, outbox.StatusProcessed, status)
	})
}

func TestStoreSkipLockedIntegration(t *testing.T) {
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

	records := []outbox.Entry{
		{AggregateType: "order", AggregateID: "1", EventType: "created", Payload: json.RawMessage(`{"id":1}`)},
		{AggregateType: "order", AggregateID: "2", EventType: "created", Payload: json.RawMessage(`{"id":2}`)},
	}
	insertEntries(t, ctx, db, store, records)

	batch1, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.NoError(t, err)
	ids1 := collectIDs(batch1.Records())
	require.Len(t, ids1, 1)

	batch2, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.NoError(t, err)
	ids2 := collectIDs(batch2.Records())
	require.Len(t, ids2, 1)

	require.NotEqual(t, ids1[0], ids2[0])

	require.NoError(t, batch1.Rollback())
	require.NoError(t, batch2.Rollback())
}

func TestStoreFailureAttemptsIntegration(t *testing.T) {
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

	store, err := mysql.NewStore(db, mysql.WithMaxAttempts(2))
	require.NoError(t, err)

	entries := []outbox.Entry{{AggregateType: "order", AggregateID: "1", EventType: "created", Payload: json.RawMessage(`{"id":1}`)}}
	insertEntries(t, ctx, db, store, entries)

	batch1, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.NoError(t, err)
	ids1 := collectIDs(batch1.Records())
	require.NoError(t, batch1.Fail(ctx, []outbox.Failure{{ID: ids1[0], Err: errors.New("boom")}}))
	require.NoError(t, batch1.Commit())

	status, attempts := fetchStatus(t, ctx, db, ids1[0])
	require.Equal(t, outbox.StatusPending, status)
	require.Equal(t, 1, attempts)

	batch2, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.NoError(t, err)
	ids2 := collectIDs(batch2.Records())
	require.NoError(t, batch2.Fail(ctx, []outbox.Failure{{ID: ids2[0], Err: errors.New("boom")}}))
	require.NoError(t, batch2.Commit())

	status, attempts = fetchStatus(t, ctx, db, ids2[0])
	require.Equal(t, outbox.StatusDead, status)
	require.Equal(t, 2, attempts)
	require.Equal(t, 1, countByStatus(t, ctx, db, outbox.StatusDead))

	_, err = store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.Error(t, err)
	require.ErrorIs(t, err, outbox.ErrNoRecords)

	t.Run("relay failure persistence", func(t *testing.T) {
		panicSecret := "panic-secret-sentinel"
		errorSecret := "error-secret-sentinel"
		panicID := parseIntegrationID(t, "01955193-de00-7000-8000-000000000005")
		errorID := parseIntegrationID(t, "01955193-de00-7000-8000-000000000006")
		insertEntries(t, ctx, db, store, []outbox.Entry{
			entryWithID(panicID, "panic"),
			entryWithID(errorID, "error"),
		})

		relay := outbox.NewRelay(
			store,
			outbox.HandlerFunc(func(_ context.Context, record outbox.Record) error {
				switch record.AggregateID {
				case "panic":
					panic(panicSecret)
				case "error":
					if record.Attempts == 0 {
						return fmt.Errorf("publish failed with %s", errorSecret)
					}
				}

				return nil
			}),
			outbox.WithBatchSize(2),
		)

		var (
			processed  bool
			processErr error
		)
		require.NotPanics(t, func() {
			processed, processErr = relay.ProcessOnce(ctx)
		})
		require.NoError(t, processErr)
		require.True(t, processed)

		status, attempts, lastErr, processedAt := fetchDetails(t, ctx, db, panicID)
		require.Equal(t, outbox.StatusPending, status)
		require.Equal(t, 1, attempts)
		require.Equal(t, "outbox handler panicked", lastErr.String)
		require.NotContains(t, lastErr.String, panicSecret)
		require.False(t, processedAt.Valid)

		status, attempts, lastErr, processedAt = fetchDetails(t, ctx, db, errorID)
		require.Equal(t, outbox.StatusPending, status)
		require.Equal(t, 1, attempts)
		require.Equal(t, "outbox handler failed", lastErr.String)
		require.NotContains(t, lastErr.String, errorSecret)
		require.False(t, processedAt.Valid)

		processed, processErr = relay.ProcessOnce(ctx)
		require.NoError(t, processErr)
		require.True(t, processed)

		status, attempts, lastErr, processedAt = fetchDetails(t, ctx, db, panicID)
		require.Equal(t, outbox.StatusDead, status)
		require.Equal(t, 2, attempts)
		require.Equal(t, "outbox handler panicked", lastErr.String)
		require.NotContains(t, lastErr.String, panicSecret)
		require.False(t, processedAt.Valid)

		status, attempts, lastErr, processedAt = fetchDetails(t, ctx, db, errorID)
		require.Equal(t, outbox.StatusProcessed, status)
		require.Equal(t, 1, attempts)
		require.False(t, lastErr.Valid)
		require.True(t, processedAt.Valid)

		timeoutSecret := "timeout-secret-sentinel"
		timeoutID := parseIntegrationID(t, "01955193-de00-7000-8000-000000000007")
		insertEntries(t, ctx, db, store, []outbox.Entry{entryWithID(timeoutID, "timeout")})
		timeoutRelay := outbox.NewRelay(
			store,
			outbox.HandlerFunc(func(handlerCtx context.Context, _ outbox.Record) error {
				<-handlerCtx.Done()

				return fmt.Errorf("publish failed with %s: %w", timeoutSecret, handlerCtx.Err())
			}),
			outbox.WithBatchSize(1),
			outbox.WithHandlerTimeout(10*time.Millisecond),
		)
		processCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		processed, processErr = timeoutRelay.ProcessOnce(processCtx)
		require.NoError(t, processErr)
		require.True(t, processed)

		status, attempts, lastErr, processedAt = fetchDetails(t, ctx, db, timeoutID)
		require.Equal(t, outbox.StatusPending, status)
		require.Equal(t, 1, attempts)
		require.Equal(t, "outbox handler timed out", lastErr.String)
		require.NotContains(t, lastErr.String, timeoutSecret)
		require.False(t, processedAt.Valid)
	})
}

func TestStoreAckClearsLastErrorIntegration(t *testing.T) {
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

	store, err := mysql.NewStore(db, mysql.WithMaxAttempts(3))
	require.NoError(t, err)

	entries := []outbox.Entry{{AggregateType: "order", AggregateID: "1", EventType: "created", Payload: json.RawMessage(`{"id":1}`)}}
	insertEntries(t, ctx, db, store, entries)

	batch1, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.NoError(t, err)
	id := collectIDs(batch1.Records())[0]
	require.NoError(t, batch1.Fail(ctx, []outbox.Failure{{ID: id, Err: errors.New("boom")}}))
	require.NoError(t, batch1.Commit())

	batch2, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.NoError(t, err)
	require.NoError(t, batch2.Ack(ctx, []outbox.ID{id}))
	require.NoError(t, batch2.Commit())

	status, attempts, lastErr, processedAt := fetchDetails(t, ctx, db, id)
	require.Equal(t, outbox.StatusProcessed, status)
	require.Equal(t, 1, attempts)
	require.False(t, lastErr.Valid)
	require.True(t, processedAt.Valid)
}

func TestStoreFailSetsLastErrorIntegration(t *testing.T) {
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

	entries := []outbox.Entry{{AggregateType: "order", AggregateID: "1", EventType: "created", Payload: json.RawMessage(`{"id":1}`)}}
	insertEntries(t, ctx, db, store, entries)

	batch, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.NoError(t, err)
	id := collectIDs(batch.Records())[0]
	longErr := errors.New(strings.Repeat("a", 1100))
	require.NoError(t, batch.Fail(ctx, []outbox.Failure{{ID: id, Err: longErr}}))
	require.NoError(t, batch.Commit())

	status, attempts, lastErr, processedAt := fetchDetails(t, ctx, db, id)
	require.Equal(t, outbox.StatusPending, status)
	require.Equal(t, 1, attempts)
	require.True(t, lastErr.Valid)
	require.Len(t, lastErr.String, 1024)
	require.False(t, processedAt.Valid)
}

func TestStoreRollbackMakesVisibleIntegration(t *testing.T) {
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

	entries := []outbox.Entry{{AggregateType: "order", AggregateID: "1", EventType: "created", Payload: json.RawMessage(`{"id":1}`)}}
	insertEntries(t, ctx, db, store, entries)

	batch, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.NoError(t, err)
	id := collectIDs(batch.Records())[0]
	require.NoError(t, batch.Rollback())

	batch2, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.NoError(t, err)
	id2 := collectIDs(batch2.Records())[0]
	require.Equal(t, id, id2)
	require.NoError(t, batch2.Rollback())
}

func TestStoreDeadIntegration(t *testing.T) {
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

	entries := []outbox.Entry{{AggregateType: "order", AggregateID: "1", EventType: "created", Payload: json.RawMessage(`{"id":1}`)}}
	insertEntries(t, ctx, db, store, entries)

	batch, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.NoError(t, err)
	deadBatch, ok := batch.(outbox.DeadBatch)
	require.True(t, ok)

	ids := collectIDs(batch.Records())
	require.NoError(t, deadBatch.Dead(ctx, []outbox.Failure{{ID: ids[0], Err: errors.New("boom")}}))
	require.NoError(t, batch.Commit())

	status, attempts := fetchStatus(t, ctx, db, ids[0])
	require.Equal(t, outbox.StatusDead, status)
	require.Equal(t, 1, attempts)
}

func TestStorePendingCountIntegration(t *testing.T) {
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
	}
	insertEntries(t, ctx, db, store, entries)

	count, err := store.PendingCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, count)

	batch, err := store.Fetch(ctx, outbox.FetchOptions{BatchSize: 1})
	require.NoError(t, err)
	ids := collectIDs(batch.Records())
	require.NoError(t, batch.Ack(ctx, ids))
	require.NoError(t, batch.Commit())

	count, err = store.PendingCount(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func startMySQLContainer(t *testing.T, ctx context.Context) (testcontainers.Container, *sql.DB) {
	t.Helper()
	password := rand.Text()
	port := "3306/tcp"
	req := testcontainers.ContainerRequest{
		Image:        mysqlImage,
		ExposedPorts: []string{port},
		Env: map[string]string{
			"MYSQL_ROOT_PASSWORD": password,
			"MYSQL_DATABASE":      "outbox",
		},
		WaitingFor: wait.ForSQL(port, "mysql", func(host string, port network.Port) string {
			return fmt.Sprintf("root:%s@tcp(%s:%s)/outbox?parseTime=true&multiStatements=true", password, host, port.Port())
		}).WithStartupTimeout(2 * time.Minute),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start mysql container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("resolve host: %v", err)
	}
	mappedPort, err := container.MappedPort(ctx, port)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("resolve port: %v", err)
	}

	dsn := fmt.Sprintf("root:%s@tcp(%s:%s)/outbox?parseTime=true&multiStatements=true", password, host, mappedPort.Port())
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		_ = container.Terminate(ctx)
		t.Fatalf("open db: %v", err)
	}
	return container, db
}

func setupSchema(t *testing.T, ctx context.Context, db *sql.DB) {
	t.Helper()
	schema, err := mysql.Schema("outbox")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, schema)
	require.NoError(t, err)
}

func insertEntries(t *testing.T, ctx context.Context, db *sql.DB, store *mysql.Store, entries []outbox.Entry) {
	t.Helper()
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	for _, entry := range entries {
		_, err := store.Enqueue(ctx, tx, entry)
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}

func collectIDs(records []outbox.Record) []outbox.ID {
	ids := make([]outbox.ID, 0, len(records))
	for _, record := range records {
		ids = append(ids, record.ID)
	}
	return ids
}

func countByStatus(t *testing.T, ctx context.Context, db *sql.DB, status outbox.Status) int {
	t.Helper()
	var count int
	err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox WHERE status = ?", status).Scan(&count)
	require.NoError(t, err)
	return count
}

func fetchStatus(t *testing.T, ctx context.Context, db *sql.DB, id outbox.ID) (outbox.Status, int) {
	t.Helper()
	var status outbox.Status
	var attempts int
	err := db.QueryRowContext(ctx, "SELECT status, attempt_count FROM outbox WHERE id = ?", id).Scan(&status, &attempts)
	require.NoError(t, err)
	return status, attempts
}

func fetchDetails(t *testing.T, ctx context.Context, db *sql.DB, id outbox.ID) (outbox.Status, int, sql.NullString, sql.NullTime) {
	t.Helper()
	var (
		status      outbox.Status
		attempts    int
		lastError   sql.NullString
		processedAt sql.NullTime
	)
	err := db.QueryRowContext(ctx, "SELECT status, attempt_count, last_error, processed_at FROM outbox WHERE id = ?", id).
		Scan(&status, &attempts, &lastError, &processedAt)
	require.NoError(t, err)

	return status, attempts, lastError, processedAt
}

func resetPartitionedSchema(t *testing.T, ctx context.Context, db *sql.DB, now time.Time) {
	t.Helper()
	_, err := db.ExecContext(ctx, "DROP TABLE IF EXISTS outbox")
	require.NoError(t, err)
	setupPartitionedSchema(t, ctx, db, now)
}

func newFixedClockStore(t *testing.T, db *sql.DB, now time.Time) *mysql.Store {
	t.Helper()
	store, err := mysql.NewStore(db, mysql.WithClock(fixedClock{now: now}))
	require.NoError(t, err)

	return store
}

func entryWithID(id outbox.ID, aggregateID string) outbox.Entry {
	entry := validIntegrationEntry(aggregateID)
	entry.ID = id

	return entry
}

func validIntegrationEntry(aggregateID string) outbox.Entry {
	return outbox.Entry{
		AggregateType: "order",
		AggregateID:   aggregateID,
		EventType:     "created",
		Payload:       json.RawMessage(`{"id":1}`),
	}
}

func requireOldCreatedTS(t *testing.T, ctx context.Context, db *sql.DB, id outbox.ID, cutoff time.Time) {
	t.Helper()
	var createdTS int64
	err := db.QueryRowContext(ctx, "SELECT created_ts FROM outbox WHERE id = ?", id).Scan(&createdTS)
	require.NoError(t, err)
	require.Less(t, createdTS, cutoff.Unix())
}
