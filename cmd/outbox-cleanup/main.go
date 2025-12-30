// Command outbox-cleanup removes old rows from a non-partitioned MySQL outbox table.
//
// It wraps mysql.CleanupMaintainer for use in cron/CronJobs when the
// application itself should not run DELETE statements.
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
		dsn         string
		table       string
		retention   time.Duration
		checkEvery  time.Duration
		limit       int
		lockName    string
		includeDead bool
		once        bool
		verbose     bool
	)

	flag.StringVar(&dsn, "dsn", "", "MySQL DSN, e.g. user:pass@tcp(host:3306)/db?parseTime=true")
	flag.StringVar(&table, "table", "outbox", "Outbox table name")
	flag.DurationVar(&retention, "retention", 0, "Delete rows older than this duration")
	flag.DurationVar(&checkEvery, "check-every", time.Hour, "How often to run cleanup")
	flag.IntVar(&limit, "limit", 0, "Max rows deleted per run (0 uses default)")
	flag.StringVar(&lockName, "lock-name", "", "Advisory lock name (optional)")
	flag.BoolVar(&includeDead, "include-dead", false, "Delete dead rows as well")
	flag.BoolVar(&once, "once", false, "Run once and exit")
	flag.BoolVar(&verbose, "verbose", false, "Enable debug logging")
	flag.Parse()

	if dsn == "" {
		fmt.Fprintln(os.Stderr, "dsn is required")
		flag.Usage()
		os.Exit(exitUsage)
	}

	if err := run(dsn, table, retention, checkEvery, limit, lockName, includeDead, once, verbose); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run(
	dsn, table string,
	retention, checkEvery time.Duration,
	limit int,
	lockName string,
	includeDead, once, verbose bool,
) error {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("open db: %w", err)
	}
	defer db.Close()

	logger := stdLogger{logger: log.New(os.Stdout, "", log.LstdFlags), verbose: verbose}
	cfg := mysql.CleanupMaintainerConfig{
		Table:       table,
		Retention:   retention,
		CheckEvery:  checkEvery,
		Limit:       limit,
		IncludeDead: includeDead,
		LockName:    lockName,
		Clock:       outbox.SystemClock{},
		Logger:      logger,
	}
	maintainer, err := mysql.NewCleanupMaintainer(db, cfg)
	if err != nil {
		return fmt.Errorf("init maintainer: %w", err)
	}

	ctx := context.Background()
	if once {
		result, err := maintainer.Ensure(ctx)
		if err != nil {
			return fmt.Errorf("cleanup: %w", err)
		}
		if result.Processed > 0 || result.Dead > 0 {
			logger.Info("cleanup done", "processed", result.Processed, "dead", result.Dead)
		}

		return nil
	}

	if err := maintainer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("run maintainer: %w", err)
	}

	return nil
}
