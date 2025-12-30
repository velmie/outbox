package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/velmie/outbox"
)

const (
	maxErrorLen       = 1024
	ackFixedArgs      = 2
	placeholderGrowth = 2
)

// Executor allows enqueuing within an existing transaction.
type Executor interface {
	// ExecContext executes a statement with the provided context.
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// Store implements a MySQL-backed outbox using polling + SKIP LOCKED.
type Store struct {
	db      *sql.DB
	cfg     Config
	queries queries
	table   string
}

var _ outbox.Consumer = (*Store)(nil)
var _ outbox.PendingCounter = (*Store)(nil)

// NewStore constructs a MySQL store with validated configuration.
func NewStore(db *sql.DB, opts ...Option) (*Store, error) {
	if db == nil {
		return nil, ErrDBRequired
	}

	var cfg Config
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg = cfg.withDefaults()

	table, err := sanitizeTableName(cfg.Table)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:      db,
		cfg:     cfg,
		queries: newQueries(table),
		table:   table,
	}, nil
}

// MustNewStore constructs a MySQL store or panics on error.
func MustNewStore(db *sql.DB, opts ...Option) *Store {
	store, err := NewStore(db, opts...)
	if err != nil {
		panic(err)
	}

	return store
}

// Enqueue inserts an outbox entry using the provided executor (transaction preferred).
func (s *Store) Enqueue(ctx context.Context, exec Executor, entry outbox.Entry) (outbox.ID, error) {
	if exec == nil {
		return outbox.ID{}, ErrExecutorRequired
	}
	if err := outbox.ValidateEntryWithOptions(entry, s.cfg.ValidatePayload, s.cfg.ValidateHeaders); err != nil {
		return outbox.ID{}, err
	}

	id := entry.ID
	if id.IsZero() {
		var err error
		id, err = s.cfg.Generator.New()
		if err != nil {
			return outbox.ID{}, fmt.Errorf("outbox mysql: generate id failed: %w", err)
		}
	}

	headers := any(nil)
	if len(entry.Headers) > 0 {
		headers = entry.Headers
	}

	_, err := exec.ExecContext(
		ctx,
		s.queries.insert,
		id,
		entry.AggregateType,
		entry.AggregateID,
		entry.EventType,
		entry.Payload,
		headers,
	)
	if err != nil {
		return outbox.ID{}, fmt.Errorf("outbox mysql: insert failed: %w", err)
	}

	return id, nil
}

// Fetch locks and returns a batch of pending records using READ COMMITTED + SKIP LOCKED.
func (s *Store) Fetch(ctx context.Context, opts outbox.FetchOptions) (outbox.Batch, error) {
	if opts.BatchSize <= 0 {
		return nil, outbox.ErrInvalidBatchSize
	}

	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("outbox mysql: begin tx failed: %w", err)
	}

	records, err := s.selectBatch(ctx, tx, opts)
	if err != nil {
		rollbackErr := tx.Rollback()

		return nil, errors.Join(err, rollbackErr)
	}
	if len(records) == 0 {
		_ = tx.Rollback()

		return nil, outbox.ErrNoRecords
	}

	return &batch{tx: tx, store: s, records: records}, nil
}

func (s *Store) selectBatch(ctx context.Context, tx *sql.Tx, opts outbox.FetchOptions) ([]outbox.Record, error) {
	var (
		rows *sql.Rows
		err  error
	)

	if opts.MinCreatedAt.IsZero() {
		rows, err = tx.QueryContext(ctx, s.queries.selectPending, outbox.StatusPending, opts.BatchSize)
	} else {
		rows, err = tx.QueryContext(ctx, s.queries.selectPendingTS, outbox.StatusPending, createdTS(opts.MinCreatedAt), opts.BatchSize)
	}
	if err != nil {
		return nil, fmt.Errorf("outbox mysql: select failed: %w", err)
	}
	defer rows.Close()

	records := make([]outbox.Record, 0, opts.BatchSize)
	for rows.Next() {
		var (
			id        outbox.ID
			aggType   string
			aggID     string
			eventType string
			payload   []byte
			headers   []byte
			createdAt time.Time
			attempts  int
		)

		if err := rows.Scan(&id, &aggType, &aggID, &eventType, &payload, &headers, &createdAt, &attempts); err != nil {
			return nil, fmt.Errorf("outbox mysql: scan failed: %w", err)
		}

		records = append(records, outbox.Record{
			ID:            id,
			AggregateType: aggType,
			AggregateID:   aggID,
			EventType:     eventType,
			Payload:       payload,
			Headers:       headers,
			CreatedAt:     createdAt,
			Attempts:      attempts,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox mysql: rows failed: %w", err)
	}

	return records, nil
}

func (s *Store) ack(ctx context.Context, tx *sql.Tx, ids []outbox.ID) error {
	if len(ids) == 0 {
		return nil
	}

	query := buildAckQuery(s.table, len(ids))
	args := make([]any, 0, len(ids)+ackFixedArgs)
	args = append(args, outbox.StatusProcessed, s.cfg.Clock.Now())
	for _, id := range ids {
		args = append(args, id)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("outbox mysql: ack update failed: %w", err)
	}

	return nil
}

func (s *Store) fail(ctx context.Context, tx *sql.Tx, failures []outbox.Failure) error {
	if len(failures) == 0 {
		return nil
	}

	for _, failure := range failures {
		errText := truncateError(failure.Err)
		if _, err := tx.ExecContext(
			ctx,
			s.queries.updateFailureOne,
			errText,
			s.cfg.MaxAttempts,
			outbox.StatusDead,
			outbox.StatusPending,
			failure.ID,
		); err != nil {
			return fmt.Errorf("outbox mysql: fail update failed: %w", err)
		}
	}

	return nil
}

func (s *Store) dead(ctx context.Context, tx *sql.Tx, failures []outbox.Failure) error {
	if len(failures) == 0 {
		return nil
	}

	for _, failure := range failures {
		errText := truncateError(failure.Err)
		if _, err := tx.ExecContext(
			ctx,
			s.queries.updateDeadOne,
			errText,
			outbox.StatusDead,
			failure.ID,
		); err != nil {
			return fmt.Errorf("outbox mysql: dead update failed: %w", err)
		}
	}

	return nil
}

// PendingCount returns the number of pending outbox rows.
func (s *Store) PendingCount(ctx context.Context) (int, error) {
	var count int
	if err := s.db.QueryRowContext(ctx, s.queries.countPending, outbox.StatusPending).Scan(&count); err != nil {
		return 0, fmt.Errorf("outbox mysql: pending count failed: %w", err)
	}

	return count, nil
}

func buildAckQuery(table string, count int) string {
	placeholders := makePlaceholders(count)

	return fmt.Sprintf("UPDATE %s SET status = ?, processed_at = ?, last_error = NULL WHERE id IN (%s)", table, placeholders)
}

func makePlaceholders(count int) string {
	if count <= 0 {
		return ""
	}

	buf := make([]byte, 0, count*placeholderGrowth)
	for i := 0; i < count; i++ {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = append(buf, '?')
	}

	return string(buf)
}

func createdTS(t time.Time) int64 {
	return t.UTC().Unix()
}

func truncateError(err error) string {
	if err == nil {
		return ""
	}

	msg := err.Error()
	if utf8.RuneCountInString(msg) <= maxErrorLen {
		return msg
	}

	runes := []rune(msg)
	if len(runes) <= maxErrorLen {
		return msg
	}

	return string(runes[:maxErrorLen])
}
