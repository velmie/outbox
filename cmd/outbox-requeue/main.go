// Command outbox-requeue returns selected dead outbox records to pending.
package main

import (
	"context"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

const (
	dsnEnvName   = "OUTBOX_DSN"
	defaultLimit = 1000
	exitUsage    = 2
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := run(ctx, os.Args[1:], os.Getenv(dsnEnvName), os.Stdout, os.Stderr)
	stop()
	if code != 0 {
		os.Exit(code)
	}
}

func run(ctx context.Context, args []string, dsn string, out, diagnostics io.Writer) int {
	flags := flag.NewFlagSet("outbox-requeue", flag.ContinueOnError)
	flags.SetOutput(diagnostics)
	var ids idFlags
	flags.Var(&ids, "id", "Select a stored UUID; may be repeated")
	table := flags.String("table", "outbox", "Outbox table")
	aggregate := flags.String("aggregate-type", "", "Select an aggregate type")
	event := flags.String("event-type", "", "Select an event type")
	limit := flags.Int("limit", defaultLimit, "Maximum number of records")
	scheduled := flags.Bool("retry-scheduling", false, "Reset retry deadlines")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}

		return exitUsage
	}
	if len(flags.Args()) != 0 || *limit <= 0 || (len(ids) == 0 && *aggregate == "" && *event == "") || dsn == "" {
		fmt.Fprintln(diagnostics, "a positive limit, a selector, OUTBOX_DSN, and no positional arguments are required")

		return exitUsage
	}
	parsedIDs := make([]outbox.ID, 0, len(ids))
	for _, value := range ids {
		id, err := outbox.ParseID(value)
		if err != nil {
			fmt.Fprintln(diagnostics, "invalid -id: expected a UUID")

			return exitUsage
		}
		parsedIDs = append(parsedIDs, id)
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Fprintln(diagnostics, "invalid OUTBOX_DSN")

		return exitUsage
	}
	opts := []mysql.Option{mysql.WithTable(*table)}
	if *scheduled {
		opts = append(opts, mysql.WithRetrySchedule(func(int) time.Duration { return 0 }))
	}
	store, err := mysql.NewStore(db, opts...)
	if err != nil {
		if closeErr := db.Close(); closeErr != nil {
			fmt.Fprintf(diagnostics, "close database: %v\n", closeErr)
		}
		fmt.Fprintf(diagnostics, "invalid configuration: %v\n", err)

		return exitUsage
	}
	count, requeueErr := store.Requeue(ctx, mysql.RequeueOptions{
		IDs:           parsedIDs,
		AggregateType: *aggregate,
		EventType:     *event,
		Limit:         *limit,
	})
	closeErr := db.Close()
	if err = errors.Join(requeueErr, closeErr); err != nil {
		fmt.Fprintf(diagnostics, "requeue failed: %v\n", err)

		return 1
	}
	if _, err = fmt.Fprintln(out, count); err != nil {
		fmt.Fprintf(diagnostics, "write count: %v\n", err)

		return 1
	}

	return 0
}

type idFlags []string

func (ids *idFlags) String() string { return fmt.Sprint([]string(*ids)) }

func (ids *idFlags) Set(value string) error {
	*ids = append(*ids, value)

	return nil
}
