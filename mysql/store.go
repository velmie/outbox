package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
	"unicode/utf8"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/velmie/outbox"
)

const (
	maxErrorLen       = 1024
	ackFixedArgs      = 2
	placeholderGrowth = 2
)

// Store implements a MySQL-backed outbox using polling + SKIP LOCKED.
type Store struct {
	db      *sql.DB
	cfg     Config
	queries queries
	table   string
}

// Executor allows enqueuing within an existing transaction.
type Executor interface {
	// ExecContext executes a statement with the provided context.
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
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
	if cfg.RetryDelay < 0 {
		return nil, ErrRetryDelayInvalid
	}
	if cfg.RetryDelay > 0 && cfg.RetrySchedule != nil {
		return nil, errRetryPolicyConflict
	}

	table, err := sanitizeTableName(cfg.Table)
	if err != nil {
		return nil, err
	}

	return &Store{
		db:      db,
		cfg:     cfg,
		queries: newQueries(table, cfg.retryScheduling(), cfg.AggregateType != ""),
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
	if s.cfg.AggregateType != "" && entry.AggregateType != s.cfg.AggregateType {
		return outbox.ID{}, errAggregateTypeMismatch
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
		if id.IsZero() {
			return outbox.ID{}, fmt.Errorf("outbox mysql: generate id failed: %w", outbox.ErrInvalidID)
		}
		entry.ID = id
		if err := outbox.ValidateEntryWithOptions(entry, false, false); err != nil {
			return outbox.ID{}, fmt.Errorf("outbox mysql: generate id failed: %w", err)
		}
	}

	headers := any(nil)
	if len(entry.Headers) > 0 {
		headers = entry.Headers
	}

	query := s.queries.insert
	args := []any{
		id,
		entry.AggregateType,
		entry.AggregateID,
		entry.EventType,
		entry.Payload,
		headers,
	}
	if entry.DedupKey != "" {
		query = s.queries.insertDedup
		args = append(args, []byte(entry.DedupKey))
	}
	_, err := exec.ExecContext(ctx, query, args...)
	if err != nil {
		var mysqlErr *mysqldriver.MySQLError
		if errors.As(err, &mysqlErr) && mysqlErr.Number == 1062 {
			return outbox.ID{}, fmt.Errorf("outbox mysql: insert failed: %w: %w", outbox.ErrDuplicate, err)
		}

		return outbox.ID{}, fmt.Errorf("outbox mysql: insert failed: %w", err)
	}

	return id, nil
}

// Fetch locks eligible pending records using READ COMMITTED + SKIP LOCKED.
// When retry scheduling is enabled, future deadlines are excluded.
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

// PendingCount returns the number of pending outbox rows, including delayed retries.
func (s *Store) PendingCount(ctx context.Context) (int, error) {
	var count int
	args := s.scopeArgs([]any{outbox.StatusPending})
	if err := s.db.QueryRowContext(ctx, s.queries.countPending, args...).Scan(&count); err != nil {
		return 0, fmt.Errorf("outbox mysql: pending count failed: %w", err)
	}

	return count, nil
}

func (s *Store) selectBatch(ctx context.Context, tx *sql.Tx, opts outbox.FetchOptions) ([]outbox.Record, error) {
	args := s.scopeArgs([]any{outbox.StatusPending})
	args = append(args, opts.BatchSize)
	rows, err := tx.QueryContext(ctx, s.queries.selectPending, args...)
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
	if s.cfg.AggregateType != "" {
		query += aggregateScopePredicate
		args = append(args, s.cfg.AggregateType)
	}

	if _, err := tx.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("outbox mysql: ack update failed: %w", err)
	}

	return nil
}

func (s *Store) fail(ctx context.Context, tx *sql.Tx, failures []outbox.Failure, attempts map[outbox.ID]int) error {
	if len(failures) == 0 {
		return nil
	}

	for _, failure := range failures {
		attempt, found := attempts[failure.ID]
		if !found {
			continue
		}
		attempt++
		delay, err := s.retryDelay(failure, attempt)
		if err != nil {
			return fmt.Errorf("outbox mysql: retry record %s: %w", failure.ID, err)
		}
		errText := truncateError(failure.Err)
		args := []any{errText, s.cfg.MaxAttempts, outbox.StatusDead, outbox.StatusPending}
		if s.cfg.retryScheduling() {
			var micros int64
			if delay > 0 {
				micros = int64((delay-1)/time.Microsecond) + 1
			}
			args = append(args, micros)
		}
		args = append(args, failure.ID)
		args = s.scopeArgs(args)
		if _, err := tx.ExecContext(
			ctx,
			s.queries.updateFailureOne,
			args...,
		); err != nil {
			return fmt.Errorf("outbox mysql: fail update failed: %w", err)
		}
		attempts[failure.ID] = attempt
	}

	return nil
}

func (s *Store) retryDelay(failure outbox.Failure, attempt int) (time.Duration, error) {
	if attempt >= s.cfg.MaxAttempts {
		return 0, nil
	}
	if failure.RetryDelay < 0 {
		return 0, errNegativeRetryHint
	}
	if !s.cfg.retryScheduling() {
		if failure.RetryDelay > 0 {
			return 0, errRetrySchedulingRequired
		}

		return 0, nil
	}
	delay := s.cfg.RetryDelay
	if s.cfg.RetrySchedule != nil {
		delay = s.cfg.RetrySchedule(attempt)
		if delay < 0 {
			return 0, errNegativeSchedule
		}
	}
	if failure.RetryDelay > delay {
		delay = failure.RetryDelay
	}

	return delay, nil
}

func (s *Store) dead(ctx context.Context, tx *sql.Tx, failures []outbox.Failure) error {
	if len(failures) == 0 {
		return nil
	}

	for _, failure := range failures {
		errText := truncateError(failure.Err)
		args := s.scopeArgs([]any{errText, outbox.StatusDead, failure.ID})
		if _, err := tx.ExecContext(ctx, s.queries.updateDeadOne, args...); err != nil {
			return fmt.Errorf("outbox mysql: dead update failed: %w", err)
		}
	}

	return nil
}

func (s *Store) scopeArgs(args []any) []any {
	if s.cfg.AggregateType != "" {
		args = append(args, s.cfg.AggregateType)
	}

	return args
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

func truncateError(err error) string {
	if err == nil {
		return ""
	}

	msg := string([]rune(err.Error()))
	if utf8.RuneCountInString(msg) <= maxErrorLen {
		return msg
	}

	runes := []rune(msg)
	if len(runes) <= maxErrorLen {
		return msg
	}

	return string(runes[:maxErrorLen])
}
