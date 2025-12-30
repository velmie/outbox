//go:build integration

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/cmd/internal/testutil"
	"github.com/velmie/outbox/mysql"
)

func TestCleanupCLIContainer(t *testing.T) {
	ctx := context.Background()
	env := testutil.StartMySQLContainer(t, ctx)

	schema, err := mysql.Schema("outbox")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := env.DB.ExecContext(ctx, schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	store, err := mysql.NewStore(env.DB)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}

	ids := insertEntries(t, ctx, env.DB, store, 3)
	oldTime := time.Now().Add(-48 * time.Hour).UTC()

	if err := markProcessed(ctx, env.DB, ids[0], oldTime); err != nil {
		t.Fatalf("mark processed: %v", err)
	}
	if err := markDead(ctx, env.DB, ids[1], oldTime); err != nil {
		t.Fatalf("mark dead: %v", err)
	}

	bin := testutil.BuildBinary(t, ".")
	args := []string{
		"-dsn", env.DSN,
		"-table", "outbox",
		"-retention", "24h",
		"-include-dead",
		"-once",
	}
	code, logs := testutil.RunCLIContainer(t, ctx, env.Network.Name, bin, args)
	if code != 0 {
		t.Fatalf("cleanup exit code %d logs: %s", code, logs)
	}

	pending := countByStatus(t, ctx, env.DB, outbox.StatusPending)
	processed := countByStatus(t, ctx, env.DB, outbox.StatusProcessed)
	dead := countByStatus(t, ctx, env.DB, outbox.StatusDead)

	if pending != 1 {
		t.Fatalf("pending count = %d, want 1", pending)
	}
	if processed != 0 {
		t.Fatalf("processed count = %d, want 0", processed)
	}
	if dead != 0 {
		t.Fatalf("dead count = %d, want 0", dead)
	}
}

func insertEntries(t *testing.T, ctx context.Context, db *sql.DB, store *mysql.Store, count int) []outbox.ID {
	t.Helper()

	ids := make([]outbox.ID, 0, count)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	for i := 0; i < count; i++ {
		id, err := store.Enqueue(ctx, tx, outbox.Entry{
			AggregateType: "order",
			AggregateID:   "1",
			EventType:     "created",
			Payload:       json.RawMessage(`{"id":1}`),
		})
		if err != nil {
			_ = tx.Rollback()
			t.Fatalf("enqueue: %v", err)
		}
		ids = append(ids, id)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	return ids
}

func markProcessed(ctx context.Context, db *sql.DB, id outbox.ID, ts time.Time) error {
	_, err := db.ExecContext(
		ctx,
		"UPDATE outbox SET status = ?, processed_at = ?, updated_at = ? WHERE id = ?",
		outbox.StatusProcessed,
		ts,
		ts,
		id,
	)
	return err
}

func markDead(ctx context.Context, db *sql.DB, id outbox.ID, ts time.Time) error {
	_, err := db.ExecContext(
		ctx,
		"UPDATE outbox SET status = ?, updated_at = ? WHERE id = ?",
		outbox.StatusDead,
		ts,
		id,
	)
	return err
}

func countByStatus(t *testing.T, ctx context.Context, db *sql.DB, status outbox.Status) int {
	t.Helper()

	var count int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox WHERE status = ?", status).Scan(&count); err != nil {
		t.Fatalf("count status %d: %v", status, err)
	}

	return count
}
