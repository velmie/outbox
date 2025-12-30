package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/velmie/outbox"
)

type fakeResult struct{}

func (fakeResult) LastInsertId() (int64, error) { return 0, nil }
func (fakeResult) RowsAffected() (int64, error) { return 1, nil }

type fakeExecutor struct {
	query string
	args  []any
}

func (f *fakeExecutor) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	f.query = query
	f.args = args
	return fakeResult{}, nil
}

type fixedGenerator struct {
	id    outbox.ID
	calls int
}

func (g *fixedGenerator) New() (outbox.ID, error) {
	g.calls++
	return g.id, nil
}

func TestStoreEnqueueGeneratesID(t *testing.T) {
	gen := &fixedGenerator{id: outbox.ID{0x01}}
	store := &Store{
		cfg:     Config{Generator: gen}.withDefaults(),
		queries: newQueries("outbox"),
		table:   "outbox",
	}
	entry := outbox.Entry{
		AggregateType: "order",
		AggregateID:   "1",
		EventType:     "created",
		Payload:       json.RawMessage(`{"id":1}`),
	}
	fakeExec := &fakeExecutor{}

	id, err := store.Enqueue(context.Background(), fakeExec, entry)
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if id != gen.id {
		t.Fatalf("expected generated id to be returned")
	}
	if gen.calls != 1 {
		t.Fatalf("expected generator to be called once")
	}
	if fakeExec.query == "" {
		t.Fatalf("expected query to be executed")
	}
	if len(fakeExec.args) != 6 {
		t.Fatalf("expected 6 args, got %d", len(fakeExec.args))
	}
}

func TestStoreEnqueueSkipsPayloadValidation(t *testing.T) {
	gen := &fixedGenerator{id: outbox.ID{0x02}}
	store := &Store{
		cfg: Config{
			Generator:          gen,
			ValidatePayload:    false,
			validatePayloadSet: true,
			ValidateHeaders:    true,
			validateHeadersSet: true,
		}.withDefaults(),
		queries: newQueries("outbox"),
		table:   "outbox",
	}
	entry := outbox.Entry{
		AggregateType: "order",
		EventType:     "created",
		Payload:       json.RawMessage(`{`),
	}
	fakeExec := &fakeExecutor{}

	if _, err := store.Enqueue(context.Background(), fakeExec, entry); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
}

func TestMakePlaceholders(t *testing.T) {
	if got := makePlaceholders(1); got != "?" {
		t.Fatalf("unexpected placeholders: %s", got)
	}
	if got := makePlaceholders(3); got != "?,?,?" {
		t.Fatalf("unexpected placeholders: %s", got)
	}
}

func TestTruncateError(t *testing.T) {
	long := strings.Repeat("a", maxErrorLen+10)
	msg := truncateError(errors.New(long))
	if len([]rune(msg)) != maxErrorLen {
		t.Fatalf("expected truncated length %d, got %d", maxErrorLen, len([]rune(msg)))
	}
}
