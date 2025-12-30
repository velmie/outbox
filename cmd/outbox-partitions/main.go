// Command outbox-partitions maintains MySQL outbox partitions.
//
// It wraps mysql.PartitionMaintainer for use in cron/CronJobs when the
// application itself should not run ALTER TABLE statements.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

const exitUsage = 2

var errInvalidPeriod = errors.New("outbox partitions: invalid period")

type stdLogger struct {
	logger  *log.Logger
	verbose bool
}

func (l stdLogger) Debug(msg string, args ...any) {
	if !l.verbose {
		return
	}
	l.logger.Printf("DEBUG %s %s", msg, formatArgs(args))
}

func (l stdLogger) Info(msg string, args ...any) {
	l.logger.Printf("INFO %s %s", msg, formatArgs(args))
}

func (l stdLogger) Warn(msg string, args ...any) {
	l.logger.Printf("WARN %s %s", msg, formatArgs(args))
}

func (l stdLogger) Error(msg string, args ...any) {
	l.logger.Printf("ERROR %s %s", msg, formatArgs(args))
}

func formatArgs(args []any) string {
	if len(args) == 0 {
		return ""
	}
	pairs := make([]string, 0, len(args))
	for i := 0; i < len(args); i += 2 {
		key := args[i]
		val := any("<missing>")
		if i+1 < len(args) {
			val = args[i+1]
		}
		pairs = append(pairs, fmt.Sprintf("%v=%v", key, val))
	}

	return strings.Join(pairs, " ")
}

func main() {
	var (
		dsn        string
		table      string
		period     string
		lookahead  time.Duration
		checkEvery time.Duration
		lockName   string
		retention  time.Duration
		once       bool
		verbose    bool
	)

	flag.StringVar(&dsn, "dsn", "", "MySQL DSN, e.g. user:pass@tcp(host:3306)/db?parseTime=true")
	flag.StringVar(&table, "table", "outbox", "Outbox table name")
	flag.StringVar(&period, "period", "day", "Partition period: day or month")
	flag.DurationVar(&lookahead, "lookahead", 0, "How far ahead to create partitions (e.g. 720h)")
	flag.DurationVar(&checkEvery, "check-every", time.Hour, "How often to check for missing partitions")
	flag.StringVar(&lockName, "lock-name", "", "Advisory lock name (optional)")
	flag.DurationVar(&retention, "retention", 0, "Drop partitions older than this duration (optional)")
	flag.BoolVar(&once, "once", false, "Run once and exit")
	flag.BoolVar(&verbose, "verbose", false, "Enable debug logging")
	flag.Parse()

	if dsn == "" {
		fmt.Fprintln(os.Stderr, "dsn is required")
		flag.Usage()
		os.Exit(exitUsage)
	}

	periodValue, err := parsePeriod(period)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		flag.Usage()
		os.Exit(exitUsage)
	}

	if err := run(dsn, table, periodValue, lookahead, checkEvery, lockName, retention, once, verbose); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(
	dsn, table string,
	period mysql.PartitionPeriod,
	lookahead, checkEvery time.Duration,
	lockName string,
	retention time.Duration,
	once, verbose bool,
) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	logger := stdLogger{logger: log.New(os.Stdout, "", log.LstdFlags), verbose: verbose}
	cfg := mysql.PartitionMaintainerConfig{
		Table:      table,
		Period:     period,
		Lookahead:  lookahead,
		CheckEvery: checkEvery,
		LockName:   lockName,
		Retention:  retention,
		Clock:      outbox.SystemClock{},
		Logger:     logger,
	}
	maintainer, err := mysql.NewPartitionMaintainer(db, cfg)
	if err != nil {
		return fmt.Errorf("init maintainer: %w", err)
	}

	ctx := context.Background()
	if once {
		if err := maintainer.Ensure(ctx); err != nil {
			return fmt.Errorf("ensure partitions: %w", err)
		}

		return nil
	}

	if err := maintainer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("run maintainer: %w", err)
	}

	return nil
}

func parsePeriod(value string) (mysql.PartitionPeriod, error) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "day", "daily":
		return mysql.PartitionDay, nil
	case "month", "monthly":
		return mysql.PartitionMonth, nil
	default:
		return 0, fmt.Errorf("%w: %s", errInvalidPeriod, value)
	}
}
