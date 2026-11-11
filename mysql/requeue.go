package mysql

import (
	"context"
	"errors"
	"fmt"

	"github.com/velmie/outbox"
)

var (
	errRequeueLimit    = errors.New("outbox mysql: requeue limit must be positive")
	errRequeueSelector = errors.New("outbox mysql: requeue requires at least one selector")
	errRequeueScope    = errors.New("outbox mysql: requeue aggregate type conflicts with store scope")
)

// RequeueOptions selects a bounded set of dead records. Selectors intersect
// with each other and the store scope.
type RequeueOptions struct {
	IDs           []outbox.ID
	AggregateType string
	EventType     string
	Limit         int
}

// Requeue resets selected dead records and returns confirmed affected rows.
// An error does not prove that no rows changed.
func (s *Store) Requeue(ctx context.Context, opts RequeueOptions) (int64, error) {
	if opts.Limit <= 0 {
		return 0, errRequeueLimit
	}
	if s.cfg.AggregateType != "" && opts.AggregateType != "" && s.cfg.AggregateType != opts.AggregateType {
		return 0, errRequeueScope
	}
	if len(opts.IDs) == 0 && opts.AggregateType == "" && opts.EventType == "" && s.cfg.AggregateType == "" {
		return 0, errRequeueSelector
	}

	// #nosec G201 -- table is sanitized when constructing the store.
	query := fmt.Sprintf(
		"UPDATE %s SET status = ?, attempt_count = 0, last_error = NULL, "+
			"processed_at = NULL, updated_at = CURRENT_TIMESTAMP(6)",
		s.table,
	)
	if s.cfg.retryScheduling() {
		query += ", next_attempt_at = NULL"
	}
	query += " WHERE status = ?"
	args := []any{outbox.StatusPending, outbox.StatusDead}
	if s.cfg.AggregateType != "" {
		query += aggregateScopePredicate
		args = append(args, s.cfg.AggregateType)
	} else if opts.AggregateType != "" {
		query += aggregateScopePredicate
		args = append(args, opts.AggregateType)
	}
	if opts.EventType != "" {
		query += " AND event_type = ?"
		args = append(args, opts.EventType)
	}
	if len(opts.IDs) > 0 {
		query += " AND id IN (" + makePlaceholders(len(opts.IDs)) + ")"
		for _, id := range opts.IDs {
			args = append(args, id)
		}
	}
	query += " ORDER BY id LIMIT ?"
	args = append(args, opts.Limit)
	result, err := s.db.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, fmt.Errorf("outbox mysql: requeue update failed: %w", err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("outbox mysql: requeue rows affected failed: %w", err)
	}

	return count, nil
}
