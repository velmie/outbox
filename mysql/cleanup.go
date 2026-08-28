package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/velmie/outbox"
)

const (
	defaultCleanupLimit      = 10000
	defaultCleanupEvery      = time.Hour
	defaultCleanupLockPrefix = "outbox:cleanup:"
)

// CleanupOptions defines how to delete processed/dead records in non-partitioned tables.
type CleanupOptions struct {
	// Before removes rows older than this timestamp (required).
	Before time.Time
	// Limit caps the number of rows deleted per call (0 uses the default).
	Limit int
	// IncludeDead removes rows with status=dead using updated_at for cutoff.
	IncludeDead bool
}

// CleanupResult reports how many rows were removed.
type CleanupResult struct {
	Processed int64
	Dead      int64
}

// CleanupMaintainerConfig controls periodic cleanup of non-partitioned tables.
type CleanupMaintainerConfig struct {
	// Table is the outbox table name. Use schema.table for non-default schema.
	Table string
	// Retention removes rows older than now-retention (required).
	Retention time.Duration
	// CheckEvery is the interval between cleanup runs.
	CheckEvery time.Duration
	// Limit caps the number of rows deleted per run (0 uses the default).
	Limit int
	// IncludeDead removes dead rows in addition to processed rows.
	IncludeDead bool
	// LockName is the 1-to-64-character advisory lock name. Defaults to outbox:cleanup:<table>.
	LockName string
	// Clock overrides time source (useful for tests).
	Clock outbox.Clock
	// Logger receives warnings about cleanup failures.
	Logger outbox.Logger
}

// CleanupMaintainer runs periodic cleanup for non-partitioned tables.
type CleanupMaintainer struct {
	store *Store
	cfg   CleanupMaintainerConfig
}

// Cleanup removes processed rows (and optionally dead rows) older than opts.Before.
func (s *Store) Cleanup(ctx context.Context, opts CleanupOptions) (CleanupResult, error) {
	return s.cleanup(ctx, s.db, opts)
}

// NewCleanupMaintainer creates a new cleanup maintainer with defaults applied.
func NewCleanupMaintainer(db *sql.DB, cfg CleanupMaintainerConfig) (*CleanupMaintainer, error) {
	if db == nil {
		return nil, ErrDBRequired
	}
	if cfg.Retention <= 0 {
		return nil, ErrCleanupRetentionInvalid
	}
	if cfg.Clock == nil {
		cfg.Clock = outbox.SystemClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = outbox.NopLogger{}
	}
	if cfg.CheckEvery <= 0 {
		cfg.CheckEvery = defaultCleanupEvery
	}
	if cfg.Limit == 0 {
		cfg.Limit = defaultCleanupLimit
	}
	if cfg.Limit < 0 {
		return nil, ErrCleanupLimitInvalid
	}

	store, err := NewStore(db, WithTable(cfg.Table), WithValidateJSON(false))
	if err != nil {
		return nil, err
	}
	cfg.Table = store.table
	if cfg.LockName == "" {
		cfg.LockName = defaultCleanupLockPrefix + cfg.Table
	}
	if err := validateNamedLockName(cfg.LockName); err != nil {
		return nil, err
	}

	return &CleanupMaintainer{store: store, cfg: cfg}, nil
}

// Run periodically deletes old processed/dead rows until the context is canceled.
func (m *CleanupMaintainer) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.CheckEvery)
	defer ticker.Stop()

	if _, err := m.Ensure(ctx); err != nil {
		m.cfg.Logger.Warn("outbox cleanup failed", "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := m.Ensure(ctx); err != nil {
				m.cfg.Logger.Warn("outbox cleanup failed", "err", err)
			}
		}
	}
}

// Ensure executes a single cleanup pass.
func (m *CleanupMaintainer) Ensure(ctx context.Context) (result CleanupResult, err error) {
	conn, err := m.store.db.Conn(ctx)
	if err != nil {
		return CleanupResult{}, fmt.Errorf("outbox mysql: cleanup conn failed: %w", err)
	}
	defer conn.Close()

	locked, err := tryNamedLock(ctx, conn, m.cfg.LockName)
	if err != nil {
		return CleanupResult{}, err
	}
	if !locked {
		m.cfg.Logger.Debug("outbox cleanup lock held by another session")

		return CleanupResult{}, nil
	}
	defer func() {
		err = errors.Join(err, releaseNamedLock(ctx, conn, m.cfg.LockName))
	}()

	before := m.cfg.Clock.Now().Add(-m.cfg.Retention)

	return m.store.cleanup(ctx, conn, CleanupOptions{
		Before:      before,
		Limit:       m.cfg.Limit,
		IncludeDead: m.cfg.IncludeDead,
	})
}

func (s *Store) cleanup(ctx context.Context, exec Executor, opts CleanupOptions) (CleanupResult, error) {
	if opts.Before.IsZero() {
		return CleanupResult{}, ErrCleanupBeforeRequired
	}
	limit := opts.Limit
	if limit == 0 {
		limit = defaultCleanupLimit
	}
	if limit < 0 {
		return CleanupResult{}, ErrCleanupLimitInvalid
	}

	remaining := limit
	processed, err := s.cleanupByStatus(ctx, exec, outbox.StatusProcessed, "processed_at", opts.Before, remaining)
	if err != nil {
		return CleanupResult{}, err
	}
	remaining -= int(processed)

	var dead int64
	if opts.IncludeDead && remaining > 0 {
		dead, err = s.cleanupByStatus(ctx, exec, outbox.StatusDead, "updated_at", opts.Before, remaining)
		if err != nil {
			return CleanupResult{}, err
		}
	}

	return CleanupResult{Processed: processed, Dead: dead}, nil
}

func (s *Store) cleanupByStatus(
	ctx context.Context,
	exec Executor,
	status outbox.Status,
	tsColumn string,
	before time.Time,
	limit int,
) (int64, error) {
	if limit <= 0 {
		return 0, nil
	}

	// #nosec G201 -- table and column names are internal and sanitized.
	query := fmt.Sprintf(
		"DELETE FROM %s WHERE status = ? AND %s IS NOT NULL AND %s <= ? ORDER BY id LIMIT ?",
		s.table,
		tsColumn,
		tsColumn,
	)
	res, err := exec.ExecContext(ctx, query, status, before, limit)
	if err != nil {
		return 0, fmt.Errorf("outbox mysql: cleanup delete failed: %w", err)
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("outbox mysql: cleanup rows failed: %w", err)
	}

	return affected, nil
}
