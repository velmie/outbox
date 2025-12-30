package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/velmie/outbox"
)

const (
	defaultPartitionLookaheadDay   = 30 * 24 * time.Hour
	defaultPartitionLookaheadMonth = 90 * 24 * time.Hour
	defaultPartitionCheckEvery     = time.Hour
	defaultPartitionLockPrefix     = "outbox:partitions:"
	qualifiedTableParts            = 2
)

// PartitionPeriod defines the range partition granularity.
type PartitionPeriod int

const (
	// PartitionDay maintains daily partitions.
	PartitionDay PartitionPeriod = iota + 1
	// PartitionMonth maintains monthly partitions.
	PartitionMonth
)

// PartitionMaintainerConfig controls partition creation and cleanup.
type PartitionMaintainerConfig struct {
	// Table is the outbox table name. Use schema.table for non-default schema.
	Table string
	// Period controls partition granularity (day or month).
	Period PartitionPeriod
	// Lookahead defines how far ahead to create partitions.
	Lookahead time.Duration
	// CheckEvery is the interval between partition checks.
	CheckEvery time.Duration
	// LockName is the advisory lock name. Defaults to outbox:partitions:<table>.
	LockName string
	// Retention drops partitions older than now-retention (0 disables dropping).
	Retention time.Duration
	// Clock overrides time source (useful for tests).
	Clock outbox.Clock
	// Logger receives warnings about maintenance failures.
	Logger outbox.Logger
}

// PartitionMaintainer keeps range partitions ahead of time and trims old ones.
type PartitionMaintainer struct {
	db  *sql.DB
	cfg PartitionMaintainerConfig
}

// NewPartitionMaintainer creates a new maintainer with defaults applied.
//
// Example usage:
//
//	maintainer, err := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{
//		Table:      "outbox",
//		Period:     mysql.PartitionDay,
//		Lookahead:  30 * 24 * time.Hour,
//		CheckEvery: time.Hour,
//		Retention:  7 * 24 * time.Hour,
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	go func() {
//		_ = maintainer.Run(ctx)
//	}()
func NewPartitionMaintainer(db *sql.DB, cfg PartitionMaintainerConfig) (*PartitionMaintainer, error) {
	if db == nil {
		return nil, ErrDBRequired
	}
	table, err := sanitizeTableName(cfg.Table)
	if err != nil {
		return nil, err
	}
	cfg.Table = table
	if cfg.Period != PartitionDay && cfg.Period != PartitionMonth {
		return nil, ErrPartitionPeriodRequired
	}
	if cfg.Clock == nil {
		cfg.Clock = outbox.SystemClock{}
	}
	if cfg.Logger == nil {
		cfg.Logger = outbox.NopLogger{}
	}
	if cfg.CheckEvery <= 0 {
		cfg.CheckEvery = defaultPartitionCheckEvery
	}
	if cfg.Lookahead <= 0 {
		switch cfg.Period {
		case PartitionDay:
			cfg.Lookahead = defaultPartitionLookaheadDay
		case PartitionMonth:
			cfg.Lookahead = defaultPartitionLookaheadMonth
		}
	}
	if cfg.LockName == "" {
		cfg.LockName = defaultPartitionLockPrefix + cfg.Table
	}
	if cfg.Retention < 0 {
		return nil, ErrPartitionRetentionInvalid
	}

	return &PartitionMaintainer{db: db, cfg: cfg}, nil
}

// Run periodically ensures partitions until the context is canceled.
func (m *PartitionMaintainer) Run(ctx context.Context) error {
	ticker := time.NewTicker(m.cfg.CheckEvery)
	defer ticker.Stop()

	if err := m.Ensure(ctx); err != nil {
		m.cfg.Logger.Warn("outbox partitions ensure failed", "err", err)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := m.Ensure(ctx); err != nil {
				m.cfg.Logger.Warn("outbox partitions ensure failed", "err", err)
			}
		}
	}
}

// Ensure creates missing partitions ahead of time and optionally drops old ones.
func (m *PartitionMaintainer) Ensure(ctx context.Context) error {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("outbox mysql: partition conn failed: %w", err)
	}
	defer conn.Close()

	locked, err := m.tryLock(ctx, conn)
	if err != nil {
		return err
	}
	if !locked {
		m.cfg.Logger.Debug("outbox partitions lock held by another session")

		return nil
	}
	defer m.releaseLock(ctx, conn)

	schema, table, err := resolveSchemaTable(ctx, conn, m.cfg.Table)
	if err != nil {
		return err
	}

	info, err := loadPartitions(ctx, conn, schema, table)
	if err != nil {
		return err
	}

	plan, err := planPartitionChanges(m.cfg, info)
	if err != nil {
		return err
	}
	if len(plan.add) == 0 && len(plan.drop) == 0 {
		return nil
	}

	if len(plan.add) > 0 {
		if err := m.reorganizeMax(ctx, conn, info.maxName, plan.add); err != nil {
			return err
		}
	}
	if len(plan.drop) > 0 {
		if err := m.dropPartitions(ctx, conn, plan.drop); err != nil {
			return err
		}
	}

	return nil
}

type partitionInfo struct {
	maxName  string
	maxUpper int64
	bounds   map[int64]string
	names    map[string]int64
}

type partitionDef struct {
	name       string
	upperBound int64
}

type partitionPlan struct {
	add  []partitionDef
	drop []string
}

func (m *PartitionMaintainer) tryLock(ctx context.Context, conn *sql.Conn) (bool, error) {
	var got sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT GET_LOCK(?, 0)", m.cfg.LockName).Scan(&got); err != nil {
		return false, fmt.Errorf("outbox mysql: acquire lock failed: %w", err)
	}
	if !got.Valid || got.Int64 == 0 {
		return false, nil
	}

	return true, nil
}

func (m *PartitionMaintainer) releaseLock(ctx context.Context, conn *sql.Conn) {
	var released sql.NullInt64
	if err := conn.QueryRowContext(ctx, "SELECT RELEASE_LOCK(?)", m.cfg.LockName).Scan(&released); err != nil {
		m.cfg.Logger.Warn("outbox partitions release lock failed", "err", err)
	}
}

func (m *PartitionMaintainer) reorganizeMax(ctx context.Context, conn *sql.Conn, maxName string, add []partitionDef) error {
	m.cfg.Logger.Info(
		"outbox partitions reorganize",
		"table",
		m.cfg.Table,
		"pmax",
		maxName,
		"add",
		partitionDefNames(add),
	)
	parts := make([]string, 0, len(add)+1)
	for _, part := range add {
		parts = append(parts, fmt.Sprintf("PARTITION %s VALUES LESS THAN (%d)", part.name, part.upperBound))
	}
	parts = append(parts, fmt.Sprintf("PARTITION %s VALUES LESS THAN (MAXVALUE)", maxName))

	// #nosec G201 -- table and partition names are sanitized.
	stmt := fmt.Sprintf(
		"ALTER TABLE %s REORGANIZE PARTITION %s INTO (%s)",
		m.cfg.Table,
		maxName,
		strings.Join(parts, ", "),
	)
	if _, err := conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("outbox mysql: reorganize partition failed: %w", err)
	}

	return nil
}

func (m *PartitionMaintainer) dropPartitions(ctx context.Context, conn *sql.Conn, names []string) error {
	if len(names) == 0 {
		return nil
	}
	m.cfg.Logger.Info(
		"outbox partitions drop",
		"table",
		m.cfg.Table,
		"partitions",
		names,
	)
	stmt := fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s", m.cfg.Table, strings.Join(names, ", "))
	if _, err := conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("outbox mysql: drop partitions failed: %w", err)
	}

	return nil
}

func resolveSchemaTable(ctx context.Context, conn *sql.Conn, table string) (schema, tableName string, err error) {
	parts := strings.Split(table, ".")
	if len(parts) == qualifiedTableParts {
		return parts[0], parts[1], nil
	}
	if len(parts) > qualifiedTableParts {
		return "", "", ErrInvalidTableName
	}
	var dbName sql.NullString
	if err := conn.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName); err != nil {
		return "", "", fmt.Errorf("outbox mysql: resolve schema failed: %w", err)
	}
	if !dbName.Valid || dbName.String == "" {
		return "", "", ErrPartitionSchemaRequired
	}

	return dbName.String, table, nil
}

func loadPartitions(ctx context.Context, conn *sql.Conn, schema, table string) (partitionInfo, error) {
	rows, err := conn.QueryContext(ctx, `
SELECT PARTITION_NAME, PARTITION_DESCRIPTION
FROM information_schema.PARTITIONS
WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? AND PARTITION_NAME IS NOT NULL
ORDER BY PARTITION_ORDINAL_POSITION
`, schema, table)
	if err != nil {
		return partitionInfo{}, fmt.Errorf("outbox mysql: list partitions failed: %w", err)
	}
	defer rows.Close()

	info := partitionInfo{
		bounds: make(map[int64]string),
		names:  make(map[string]int64),
	}
	for rows.Next() {
		var (
			name sql.NullString
			desc sql.NullString
		)
		if err := rows.Scan(&name, &desc); err != nil {
			return partitionInfo{}, fmt.Errorf("outbox mysql: scan partitions failed: %w", err)
		}
		if !name.Valid || name.String == "" {
			continue
		}
		if !desc.Valid || desc.String == "" {
			return partitionInfo{}, ErrPartitionDescriptionInvalid
		}
		isMax, upper, err := parsePartitionDescription(desc.String)
		if err != nil {
			return partitionInfo{}, err
		}
		if isMax {
			if info.maxName != "" {
				return partitionInfo{}, ErrPartitionMaxRequired
			}
			info.maxName = name.String

			continue
		}

		info.bounds[upper] = name.String
		info.names[name.String] = upper
		if upper > info.maxUpper {
			info.maxUpper = upper
		}
	}
	if err := rows.Err(); err != nil {
		return partitionInfo{}, fmt.Errorf("outbox mysql: list partitions failed: %w", err)
	}
	if len(info.bounds) == 0 && info.maxName == "" {
		return partitionInfo{}, ErrPartitionedTableRequired
	}
	if info.maxName == "" {
		return partitionInfo{}, ErrPartitionMaxRequired
	}

	return info, nil
}

func parsePartitionDescription(desc string) (isMax bool, upper int64, err error) {
	if strings.EqualFold(desc, "MAXVALUE") {
		return true, 0, nil
	}
	upper, err = strconv.ParseInt(desc, 10, 64)
	if err != nil {
		return false, 0, fmt.Errorf("%w: %s", ErrPartitionDescriptionInvalid, desc)
	}

	return false, upper, nil
}

func planPartitionChanges(cfg PartitionMaintainerConfig, info partitionInfo) (partitionPlan, error) {
	now := cfg.Clock.Now().UTC()
	start := periodStart(now, cfg.Period)
	end := now.Add(cfg.Lookahead)

	add := make([]partitionDef, 0)
	names := make(map[string]struct{}, len(info.names))
	for name := range info.names {
		names[name] = struct{}{}
	}

	for {
		next := nextPeriod(start, cfg.Period)
		upper := next.Unix()
		if upper > info.maxUpper {
			if _, exists := info.bounds[upper]; !exists {
				name := partitionName(cfg.Period, start)
				if _, clash := names[name]; clash {
					return partitionPlan{}, fmt.Errorf("%w: %s", ErrPartitionNameConflict, name)
				}
				names[name] = struct{}{}
				add = append(add, partitionDef{name: name, upperBound: upper})
			}
		}
		if !next.Before(end) {
			break
		}
		start = next
	}

	drop := make([]string, 0)
	if cfg.Retention > 0 {
		cutoff := now.Add(-cfg.Retention).Unix()
		for upper, name := range info.bounds {
			if upper <= cutoff {
				drop = append(drop, name)
			}
		}
		sort.Strings(drop)
	}

	sort.Slice(add, func(i, j int) bool {
		return add[i].upperBound < add[j].upperBound
	})

	return partitionPlan{add: add, drop: drop}, nil
}

func periodStart(t time.Time, period PartitionPeriod) time.Time {
	t = t.UTC()
	switch period {
	case PartitionDay:
		return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
	case PartitionMonth:
		return time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
	default:
		return t
	}
}

func nextPeriod(t time.Time, period PartitionPeriod) time.Time {
	switch period {
	case PartitionDay:
		return t.AddDate(0, 0, 1)
	case PartitionMonth:
		return t.AddDate(0, 1, 0)
	default:
		return t
	}
}

func partitionName(period PartitionPeriod, start time.Time) string {
	switch period {
	case PartitionMonth:
		return fmt.Sprintf("p%04d%02d", start.Year(), int(start.Month()))
	default:
		return fmt.Sprintf("p%04d%02d%02d", start.Year(), int(start.Month()), start.Day())
	}
}

func partitionDefNames(defs []partitionDef) []string {
	names := make([]string, 0, len(defs))
	for _, def := range defs {
		names = append(names, def.name)
	}

	return names
}
