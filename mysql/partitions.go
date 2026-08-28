package mysql

import (
	"context"
	"database/sql"
	"errors"
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
	partitionLockCleanupTimeout    = 5 * time.Second
	partitionEngine                = "InnoDB"
	partitionMethod                = "RANGE"
	partitionExpression            = "created_ts"
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
	// LockName is the 1-to-64-character advisory lock name. Defaults to outbox:partitions:<table>.
	LockName string
	// Retention drops terminal-only partitions older than now-retention (0 disables dropping).
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
//	if err := maintainer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
//		log.Fatal(err)
//	}
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
	if err := validateNamedLockName(cfg.LockName); err != nil {
		return nil, err
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
func (m *PartitionMaintainer) Ensure(ctx context.Context) (err error) {
	conn, err := m.db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("outbox mysql: partition conn failed: %w", err)
	}
	defer conn.Close()

	locked, err := tryNamedLock(ctx, conn, m.cfg.LockName)
	if err != nil {
		return err
	}
	if !locked {
		m.cfg.Logger.Debug("outbox partitions lock held by another session")

		return nil
	}
	defer func() {
		err = errors.Join(err, releaseNamedLock(ctx, conn, m.cfg.LockName))
	}()

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
		if err := m.dropPartitions(ctx, conn, schema, table); err != nil {
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

var errPartitionRetentionBlocked = errors.New("outbox mysql: partition retention blocked by non-terminal records")

func (m *PartitionMaintainer) reorganizeMax(ctx context.Context, conn *sql.Conn, maxName string, add []partitionDef) error {
	quotedMaxName, err := quotePartitionName(maxName)
	if err != nil {
		return err
	}
	parts := make([]string, 0, len(add)+1)
	for _, part := range add {
		quotedName, err := quotePartitionName(part.name)
		if err != nil {
			return err
		}
		parts = append(parts, fmt.Sprintf("PARTITION %s VALUES LESS THAN (%d)", quotedName, part.upperBound))
	}
	parts = append(parts, fmt.Sprintf("PARTITION %s VALUES LESS THAN (MAXVALUE)", quotedMaxName))

	m.cfg.Logger.Info(
		"outbox partitions reorganize",
		"table",
		m.cfg.Table,
		"pmax",
		maxName,
		"add",
		partitionDefNames(add),
	)

	// #nosec G201 -- table and partition names are sanitized.
	stmt := fmt.Sprintf(
		"ALTER TABLE %s REORGANIZE PARTITION %s INTO (%s)",
		m.cfg.Table,
		quotedMaxName,
		strings.Join(parts, ", "),
	)
	if _, err := conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("outbox mysql: reorganize partition failed: %w", err)
	}

	return nil
}

func (m *PartitionMaintainer) dropPartitions(
	ctx context.Context,
	conn *sql.Conn,
	schema, table string,
) error {
	var dropped []string
	err := withPartitionWriteLock(ctx, conn, m.cfg.Table, func() error {
		info, err := loadPartitions(ctx, conn, schema, table)
		if err != nil {
			return err
		}
		plan, err := planPartitionChanges(m.cfg, info)
		if err != nil {
			return err
		}
		if len(plan.drop) == 0 {
			return nil
		}

		quotedNames, err := quotePartitionNames(plan.drop)
		if err != nil {
			return err
		}
		blocked, err := partitionsContainNonTerminal(ctx, conn, m.cfg.Table, quotedNames)
		if err != nil {
			return err
		}
		if blocked {
			return fmt.Errorf("%w in %s", errPartitionRetentionBlocked, strings.Join(plan.drop, ", "))
		}

		// #nosec G201 -- table and partition names are sanitized.
		stmt := fmt.Sprintf(
			"ALTER TABLE %s ALGORITHM=INPLACE, LOCK=EXCLUSIVE, DROP PARTITION %s",
			m.cfg.Table,
			strings.Join(quotedNames, ", "),
		)
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("outbox mysql: drop partitions failed: %w", err)
		}
		dropped = plan.drop

		return nil
	})
	if len(dropped) > 0 {
		m.cfg.Logger.Info(
			"outbox partitions drop",
			"table",
			m.cfg.Table,
			"partitions",
			dropped,
		)
	}

	return err
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
SELECT
    t.ENGINE,
    p.PARTITION_NAME,
    p.PARTITION_DESCRIPTION,
    p.PARTITION_METHOD,
    p.PARTITION_EXPRESSION,
    p.SUBPARTITION_METHOD
FROM information_schema.PARTITIONS AS p
JOIN information_schema.TABLES AS t
  ON t.TABLE_SCHEMA = p.TABLE_SCHEMA AND t.TABLE_NAME = p.TABLE_NAME
WHERE p.TABLE_SCHEMA = ? AND p.TABLE_NAME = ?
ORDER BY p.PARTITION_ORDINAL_POSITION
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
			engine             sql.NullString
			name               sql.NullString
			desc               sql.NullString
			method             sql.NullString
			expression         sql.NullString
			subpartitionMethod sql.NullString
		)
		if err := rows.Scan(&engine, &name, &desc, &method, &expression, &subpartitionMethod); err != nil {
			return partitionInfo{}, fmt.Errorf("outbox mysql: scan partitions failed: %w", err)
		}
		if !validPartitionMetadata(engine, name, method, expression, subpartitionMethod) {
			return partitionInfo{}, ErrPartitionedTableRequired
		}
		if _, err := quotePartitionName(name.String); err != nil {
			return partitionInfo{}, err
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

func withPartitionWriteLock(
	ctx context.Context,
	conn *sql.Conn,
	table string,
	action func() error,
) (err error) {
	var originalAutocommit int
	if err := conn.QueryRowContext(ctx, "SELECT @@SESSION.autocommit").Scan(&originalAutocommit); err != nil {
		return fmt.Errorf("outbox mysql: inspect partition autocommit failed: %w", err)
	}

	restoreAutocommit := originalAutocommit != 0
	if restoreAutocommit {
		if _, err := conn.ExecContext(ctx, "SET SESSION autocommit = 0"); err != nil {
			return fmt.Errorf("outbox mysql: disable partition autocommit failed: %w", err)
		}
	}

	locked := false
	defer func() {
		cleanupErr := finishPartitionWriteLock(ctx, conn, locked, restoreAutocommit)
		if cleanupErr != nil {
			discardConnection(conn)
			err = errors.Join(err, cleanupErr)
		}
	}()

	// #nosec G201 -- table is sanitized by NewPartitionMaintainer.
	stmt := fmt.Sprintf("LOCK TABLES %s WRITE", table)
	if _, err := conn.ExecContext(ctx, stmt); err != nil {
		return fmt.Errorf("outbox mysql: lock partition table failed: %w", err)
	}
	locked = true

	return action()
}

func finishPartitionWriteLock(
	ctx context.Context,
	conn *sql.Conn,
	locked, restoreAutocommit bool,
) error {
	cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), partitionLockCleanupTimeout)
	defer cancel()

	var cleanupErr error
	if locked {
		if _, err := conn.ExecContext(cleanupCtx, "COMMIT"); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("outbox mysql: commit partition lock failed: %w", err))
		}
		if _, err := conn.ExecContext(cleanupCtx, "UNLOCK TABLES"); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("outbox mysql: unlock partition table failed: %w", err))
		}
	}
	if restoreAutocommit {
		if _, err := conn.ExecContext(cleanupCtx, "SET SESSION autocommit = 1"); err != nil {
			cleanupErr = errors.Join(cleanupErr, fmt.Errorf("outbox mysql: restore partition autocommit failed: %w", err))
		}
	}

	return cleanupErr
}

func partitionsContainNonTerminal(
	ctx context.Context,
	conn *sql.Conn,
	table string,
	quotedNames []string,
) (bool, error) {
	// #nosec G201 -- table and partition names are sanitized.
	query := fmt.Sprintf(
		"SELECT EXISTS(SELECT 1 FROM %s PARTITION (%s) WHERE status NOT IN (?, ?) LIMIT 1)",
		table,
		strings.Join(quotedNames, ", "),
	)
	var blocked bool
	if err := conn.QueryRowContext(
		ctx,
		query,
		outbox.StatusProcessed,
		outbox.StatusDead,
	).Scan(&blocked); err != nil {
		return false, fmt.Errorf("outbox mysql: inspect partition records failed: %w", err)
	}

	return blocked, nil
}

func quotePartitionNames(names []string) ([]string, error) {
	quotedNames := make([]string, 0, len(names))
	for _, name := range names {
		quotedName, err := quotePartitionName(name)
		if err != nil {
			return nil, err
		}
		quotedNames = append(quotedNames, quotedName)
	}

	return quotedNames, nil
}

func validPartitionLayout(engine, method, expression string, subpartitioned bool) bool {
	expression = strings.TrimSpace(expression)
	if len(expression) >= 2 && expression[0] == '`' && expression[len(expression)-1] == '`' {
		expression = expression[1 : len(expression)-1]
	}

	return strings.EqualFold(engine, partitionEngine) &&
		strings.EqualFold(method, partitionMethod) &&
		strings.EqualFold(expression, partitionExpression) &&
		!subpartitioned
}

func validPartitionMetadata(
	engine, name, method, expression, subpartitionMethod sql.NullString,
) bool {
	return engine.Valid && name.Valid && name.String != "" && method.Valid && expression.Valid &&
		validPartitionLayout(engine.String, method.String, expression.String, subpartitionMethod.Valid)
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
