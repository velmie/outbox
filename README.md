# outbox

[![Go Reference](https://pkg.go.dev/badge/github.com/velmie/outbox.svg)](https://pkg.go.dev/github.com/velmie/outbox)
[![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)](mysql/go.mod)
[![License](https://img.shields.io/github/license/velmie/outbox)](LICENSE)

`outbox` is a high-performance transactional outbox library. It ships with a MySQL 8.0+ backend optimized for polling
with
`READ COMMITTED` + `SKIP LOCKED`, UUID v7 identifiers in `BINARY(16)`, batch processing, and partition-based retention.

## Installation

The MySQL adapter and command modules require Go 1.26.10 or later. The root library alone supports Go 1.25+.

```bash
go get github.com/velmie/outbox
go get github.com/velmie/outbox/mysql
```

Install `github.com/velmie/outbox/mysql` when you use the MySQL adapter.

## OpenTelemetry metrics

Use `github.com/velmie/outbox/oteloutbox` to record relay metrics with an
OpenTelemetry meter. Construct the adapter with `oteloutbox.NewMetrics` and pass
it to `outbox.WithMetrics`. It records processed, error, retry, dead, pending and
batch duration measurements with optional fixed attributes. Retry
exhaustion is counted as a retry and explicit permanent decisions as dead.

```bash
go get github.com/velmie/outbox/oteloutbox
```

Given an application-configured `metric.Meter`, an outbox consumer and a handler:

```go
func newRelay(meter metric.Meter, consumer outbox.Consumer, handler outbox.Handler) (*outbox.Relay, error) {
    metrics, err := oteloutbox.NewMetrics(meter, attribute.String("endpoint", "orders"))
    if err != nil {
        return nil, err
    }
    return outbox.NewRelay(
        consumer,
        handler,
        outbox.WithMetrics(metrics),
        outbox.WithPendingInterval(5*time.Second),
    ), nil
}
```

Import `time`, `go.opentelemetry.io/otel/attribute`, `go.opentelemetry.io/otel/metric`, `github.com/velmie/outbox` and
`github.com/velmie/outbox/oteloutbox`. Configure the meter provider with a reader
and exporter in the application. Pending sampling requires a consumer that
implements `outbox.PendingCounter` and a positive sampling interval.

Attributes apply to every measurement from the adapter. Use bounded identifiers
to limit metric cardinality. Adapters with identical attribute sets share a
metric series, including the pending gauge. Omit attributes with
`oteloutbox.NewMetrics(meter)` when no distinction is needed.

Counters and the pending gauge use the unit `{record}`. Batch duration uses
seconds and suggests explicit buckets from 5 ms through 10 s; the provider can
override the aggregation.

## Quick Start

Create the outbox table before starting the service. Generate its DDL with `mysql.Schema` or
`mysql.PartitionedSchema` and apply it through your normal migration or bootstrap identity; the runtime application
identity should not need schema-management privileges.

```go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

func main() {
	dsn := os.Getenv("OUTBOX_DSN")
	if dsn == "" {
		log.Fatal("OUTBOX_DSN is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	store, err := mysql.NewStore(db)
	if err != nil {
		log.Fatal(err)
	}

	// Producer: enqueue within the same business transaction.
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := store.Enqueue(ctx, tx, outbox.Entry{
		AggregateType: "order",
		AggregateID:   "123",
		EventType:     "order.created",
		Payload:       json.RawMessage(`{"id":"123"}`),
	}); err != nil {
		_ = tx.Rollback()
		log.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	// Consumer: poll and publish.
	handler := outbox.HandlerFunc(func(ctx context.Context, record outbox.Record) error {
		// Publish to Kafka/Rabbit/NATS/etc.
		return nil
	})

	relay := outbox.NewRelay(store, handler,
		outbox.WithBatchSize(50),
		outbox.WithWorkers(4),
		outbox.WithPollInterval(50*time.Millisecond),
	)

	if err := relay.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

## How it works

1. Your business transaction writes both domain data and an outbox entry.
2. A relay polls outbox rows with `SELECT ... FOR UPDATE SKIP LOCKED`.
3. Each record is handed to a handler and then marked `processed`, left `pending` for retry, or marked `dead`.

The polling predicate considers the entire pending backlog; pending records do not become ineligible as they age.
An opt-in durable retry delay temporarily excludes failed records until their stored deadline.
Delivery is at least once: publication may succeed before the relay can acknowledge and commit the row, so handlers
and downstream consumers must be idempotent. Publish `Record.ID` as the stable message identifier so downstream
consumers can deduplicate retries.

### Storage recovery and idle polling

Both behaviors are opt-in and apply only to `Run`:

```go
relay := outbox.NewRelay(store, handler,
    outbox.WithLogger(slog.Default()),
    outbox.WithStorageErrorBackoff(100*time.Millisecond, 5*time.Second),
    outbox.WithPollInterval(50*time.Millisecond),
    outbox.WithIdleBackoff(time.Second),
)
```

`WithStorageErrorBackoff` lets each worker report a storage error, wait, and fetch again without stopping
other workers. The wait doubles from the minimum to the maximum and resets only after an empty fetch or a
successfully processed and committed batch. Contract errors remain fatal. Without the option, storage errors
still stop `Run`.
An unsuccessful commit has an uncertain outcome. The relay attempts rollback and fetches a new batch;
records whose changes did not commit remain eligible and may be delivered again with the same IDs.

`WithIdleBackoff` doubles each worker's sleep after consecutive empty fetches, from `PollInterval` to the
configured maximum. Fetching records resets it. This reduces idle queries at the cost of up to the maximum
in added polling latency for the first record after an idle period. Storage recovery and handler time can
add further delay. Without the option, empty polls retain the fixed `PollInterval`.

See the [relay guide](docs/guide.md#storage-recovery-and-idle-backoff) for error classification, option bounds,
and shutdown behavior. Configure a logger to observe recovery; the default `NopLogger` discards diagnostics.

### Shutdown

`Run` stops admitting polling iterations when its parent is canceled or reaches its deadline, or when a worker
fails. An already admitted fetch and its batch may finish within one shared five-second budget. Configure a
different budget with `WithShutdownTimeout`; a non-positive duration cancels active work immediately. `Run`
waits for every worker before returning. A handler, callback, logger, or driver operation that does not respond
to cancellation can therefore extend shutdown beyond the budget. Keep the database, broker, and telemetry alive
until `Run` returns.

A successful parent-triggered stop returns `nil`, including on parent deadline expiry. Inspect the parent context
when the caller needs to distinguish its cancellation from its deadline. A drain timeout wraps
`context.DeadlineExceeded`; worker, batch, and cleanup failures are joined with their original causes.
`ProcessOnce` continues to use its caller's context directly. See the [relay guide](docs/guide.md#relay).

## Package layout

- `outbox` (root): core abstractions (`Relay`, `Handler`, `Consumer`, `Batch`, `IDGenerator`, `Clock`).
- `mysql/`: MySQL 8.0+ adapter (schema helpers + polling consumer + enqueue).

## Defaults

- `Relay`: batch size 50, poll interval 50ms, workers 1. Storage recovery and idle backoff are disabled.
- `MySQL`: table `outbox`, max attempts 5, UUID v7 generator, JSON validation enabled.

## Durable retry delay

To share retry timing between processes, create a JSON outbox table with `mysql.RetrySchema("outbox")`
and configure every consumer of that table with `mysql.WithRetryDelay(5*time.Second)`:

```go
store, err := mysql.NewStore(db,
    mysql.WithTable("outbox"),
    mysql.WithRetryDelay(5*time.Second),
)
```

Apply the schema through a controlled migration before starting consumers. The option does not execute DDL.
The existing schema helpers and stores without the option retain immediate retry eligibility and do not require
the additional column. A zero delay disables scheduling unless `WithRetryScheduling` or a schedule enables it.
Negative values are rejected. Positive durations round up to the next microsecond, matching MySQL's timestamp precision.

For attempt-dependent delays, use a concurrency-safe schedule. The argument is the just-failed
attempt, starting at one. A handler can add a minimum delay with `outbox.RetryAfter`:

```go
store, err := mysql.NewStore(db, mysql.WithRetrySchedule(func(attempt int) time.Duration {
	return time.Duration(attempt) * time.Second
}))
if err != nil {
	return err
}
relay := outbox.NewRelay(store, outbox.HandlerFunc(func(ctx context.Context, record outbox.Record) error {
	if err := deliver(ctx, record); err != nil {
		return outbox.RetryAfter(err, 30*time.Second)
	}
	return nil
}))
_ = relay
```

The longer of the configured delay and the hint applies. Use `mysql.WithRetryScheduling()` to enable
handler hints without a base delay or schedule function. A positive fixed delay and a schedule cannot be combined. A positive hint for a
retryable failure without scheduling rolls back the batch and returns an error. `Run` stops its workers;
`ProcessOnce` returns the error to its caller. A dead-letter classification or exhausted attempt limit
takes precedence over the hint.

By default, the relay persists generic error summaries. `outbox.WithErrorDetails()` opts in to persisting
the handler error text and recovered panic values. Enable it only when those values are safe to store.
MySQL stores at most 1024 Unicode code points and requires `utf8mb4` for full Unicode. Callbacks and
classifiers still receive the original error.

With scheduling enabled, `Batch.Fail` writes `next_attempt_at` together with the attempt count, error, and status
in the batch transaction. The deadline uses the database's UTC time when the failure update executes; time spent
before the transaction commits counts toward the delay. Each consumer fetches only pending rows with no deadline
or with a deadline at or before the database's current UTC time. Application clocks do not control eligibility.
Other eligible rows can proceed while a failed row waits. Fetches order eligible records by ID; concurrent workers
do not guarantee delivery completion order. Pending counts include delayed rows, and cleanup preserves them.
The deadline is ignored once the row is processed or dead.

The committed deadline survives process restarts. Rolling back the failure transaction rolls back both the attempt
and deadline. If a process crashes before committing the failure, another process may retry sooner. Successful
publication followed by a failed acknowledgement or commit can also repeat the same message; use its stable ID
for downstream deduplication.

For an existing table, apply the following migration before enabling scheduling (replace `outbox` with its
configured table name):

```sql
ALTER TABLE outbox
    ADD COLUMN next_attempt_at DATETIME(6) NULL,
    ADD INDEX idx_status_next_attempt_id (status, next_attempt_at, id);
```

Existing rows have no deadline and remain eligible. Stop consumers that ignore deadlines before any consumer starts
scheduling retries on that table, and configure every replacement consumer with scheduling enabled. A legacy
consumer, including a new store with scheduling disabled, would fetch delayed rows immediately. Producers can
continue using the existing enqueue contract. Missing scheduling schema causes a database error rather than a
fallback to immediate retries. `RetrySchema` creates a non-partitioned JSON table; existing binary or partitioned
tables can opt in through the explicit migration while retaining their payload and partition definitions.

## Requeue dead records

Requeue performs one bounded update of dead rows. Select by IDs, aggregate type, event type, or a
combination. Selectors intersect with the store scope. At least one selector and a positive limit are
required. The operation resets attempts, error and processed time, preserving the ID, payload, headers,
deduplication key and creation time. For a table with `next_attempt_at`, construct the store with
`mysql.WithRetryScheduling()` to clear retry deadlines too. Requeue-only callers need no delay or schedule.
The option also makes fetches honor deadlines and executes no schema changes. It preserves any configured
`WithRetryDelay` or `WithRetrySchedule`.

```go
count, err := endpoint.Requeue(ctx, mysql.RequeueOptions{
	EventType: "delivery",
	Limit:     100,
})
if err != nil {
	return err
}
_ = count
```

`Requeue` reports confirmed affected rows. An error does not prove that the database changed no rows.
Check the affected records before deciding whether to run the operation again. Requeued deliveries can
be delivered more than once, with the same ID.

The `outbox-requeue` command runs one batch and prints the affected count. Set `OUTBOX_DSN` through a
protected environment or secret manager. For example:

```sh
outbox-requeue -table outbox -aggregate-type endpoint-a -event-type delivery -limit 100 -retry-scheduling
```

Use repeated `-id` flags to select UUIDs, including previously stored UUID versions. `-retry-scheduling`
clears `next_attempt_at` and requires that column. The default limit is 1000. Invalid arguments exit with
status 2, execution failures with status 1, and success with status 0. The command does not retry mutations.

## MySQL schema

Generate with `mysql.Schema("outbox")` or use the template below. Apply generated DDL through a controlled migration;
`CREATE TABLE IF NOT EXISTS` does not convert an existing table to InnoDB or change its partition layout.
The schema is tuned for UUID v7 + polling, with `created_ts` used as the partition key for retention.

Use `SchemaWithOptions` to combine optional payload, retry, index, and partition capabilities:

```go
ddl, err := mysql.SchemaWithOptions("outbox", mysql.SchemaOptions{
	BinaryPayload:      true,
	Retry:              true,
	AggregateTypeIndex: true,
})
if err != nil {
	return err
}
// Apply ddl in a migration before starting consumers.
```

Binary payloads require `mysql.WithValidatePayload(false)` on the store. Headers remain JSON.
`Deduplication` adds a nullable unique key and cannot be combined with partitions, because the key must
remain unique across the whole table. The helper generates DDL only; it does not alter existing tables.

For independent delivery queues in one table, create an unscoped producer and a scoped store for each
consumer. A scoped store fetches, settles, counts, and cleans up only rows for its aggregate type. It rejects
entries with another aggregate type. Add `AggregateTypeIndex` to the schema for these queries and stop any
unscoped consumers before relying on isolation. Database collation determines query comparisons.

```go
producer, err := mysql.NewStore(db)
if err != nil {
	return err
}
endpoint, err := mysql.NewStore(db, mysql.WithAggregateType("endpoint-a"))
if err != nil {
	return err
}
// Use producer to enqueue and endpoint as the relay's consumer.
_, _ = producer, endpoint
```

For duplicate admission, enable `Deduplication` on an unpartitioned schema and supply an opaque
`DedupKey` of up to 512 bytes. A key is unique across the table while its row exists. An empty key
disables deduplication and works with existing schemas. On a conflict, `Enqueue` returns a zero ID
and `outbox.ErrDuplicate`; this does not prove that the existing payload is equal. The original MySQL
error is also available through `errors.As`. Commit the surrounding transaction before acknowledging
the input, including when the application treats a duplicate as successful admission. Use a non-secret
key: a MySQL duplicate-key error can contain the conflicting value.

```go
entry := outbox.Entry{
	AggregateType: "endpoint-a",
	EventType:     "delivery",
	Payload:       []byte(`{"message":"hello"}`),
	DedupKey:      "route-and-message-identity",
}
id, err := producer.Enqueue(ctx, tx, entry)
if err != nil && !errors.Is(err, outbox.ErrDuplicate) {
	return err
}
_ = id
```

```sql
CREATE TABLE IF NOT EXISTS outbox
(
    id             BINARY(16)    NOT NULL,
    aggregate_type VARCHAR(128)  NOT NULL,
    aggregate_id   VARCHAR(128)  NOT NULL,
    event_type     VARCHAR(128)  NOT NULL,
    payload        JSON          NOT NULL,
    headers        JSON          NULL,
    status         SMALLINT      NOT NULL DEFAULT 0,
    attempt_count  INT           NOT NULL DEFAULT 0,
    last_error     VARCHAR(1024) NULL,
    created_at     TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at     TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    processed_at   TIMESTAMP(6)  NULL,
    created_ts     BIGINT GENERATED ALWAYS AS (CONV(SUBSTR(HEX(id), 1, 12), 16, 10) DIV 1000) STORED,
    PRIMARY KEY (id, created_ts),
    INDEX idx_status_id (status, id)
) ENGINE=InnoDB;
```

### Partitioning (optional, recommended for fast cleanup)

```sql
CREATE TABLE IF NOT EXISTS outbox
(
    id             BINARY(16)    NOT NULL,
    aggregate_type VARCHAR(128)  NOT NULL,
    aggregate_id   VARCHAR(128)  NOT NULL,
    event_type     VARCHAR(128)  NOT NULL,
    payload        JSON          NOT NULL,
    headers        JSON          NULL,
    status         SMALLINT      NOT NULL DEFAULT 0,
    attempt_count  INT           NOT NULL DEFAULT 0,
    last_error     VARCHAR(1024) NULL,
    created_at     TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at     TIMESTAMP(6)  NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    processed_at   TIMESTAMP(6)  NULL,
    created_ts     BIGINT GENERATED ALWAYS AS (CONV(SUBSTR(HEX(id), 1, 12), 16, 10) DIV 1000) STORED,
    PRIMARY KEY (id, created_ts),
    INDEX idx_status_id (status, id)
) ENGINE=InnoDB
PARTITION BY RANGE (created_ts) (
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
);
```

Always keep a `MAXVALUE` tail partition (`pmax`) as a safety net. The maintainer will split it to add new ranges.

### Binary payloads (LONGBLOB)

If you do not want JSON payloads, use the binary schema helpers. This keeps headers in JSON
for traceability while storing payload as raw bytes:

```go
package main

import (
	"fmt"
	"log"

	"github.com/velmie/outbox/mysql"
)

func main() {
	schema, err := mysql.SchemaBinary("outbox")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(schema)
}
```

Partitioned variant:

```go
package main

import (
	"fmt"
	"log"

	"github.com/velmie/outbox/mysql"
)

func main() {
	partitions := []mysql.Partition{
		{Name: "pmax", LessThan: "MAXVALUE"},
	}

	schema, err := mysql.PartitionedSchemaBinary("outbox", partitions)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(schema)
}
```

When using binary payloads, disable payload validation:

```go
package main

import (
	"database/sql"
	"log"
	"os"

	_ "github.com/go-sql-driver/mysql"

	"github.com/velmie/outbox/mysql"
)

func main() {
	dsn := os.Getenv("OUTBOX_DSN")
	if dsn == "" {
		log.Fatal("OUTBOX_DSN is required")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if _, err := mysql.NewStore(db, mysql.WithValidatePayload(false)); err != nil {
		log.Fatal(err)
	}
}
```

### Partition maintenance

Two options are supported:

1. **Embedded maintainer** (inside your service) when the app can run `ALTER TABLE`.
2. **Standalone CLI** (cron / Kubernetes CronJob) when `ALTER` is not allowed in the app.

The maintainer uses `GET_LOCK` and reads `information_schema`. Partition creation needs `SELECT`, `ALTER`, `CREATE`,
and `INSERT` on the target table/schema. Retention additionally needs `DROP` and schema-level `LOCK TABLES`;
without `Retention`, those two privileges are not used.

Use one stable lock name for the same operation and table across all instances. `LockName` and `-lock-name` must contain
1-64 valid UTF-8 characters. The default is `outbox:partitions:<table>`; set a shorter explicit name when a qualified
table name would make the default too long.

Behavior overview:

- On startup it verifies the table is InnoDB and partitioned by `RANGE(created_ts)` without subpartitions.
- It creates missing partitions for the configured lookahead window using the chosen period.
- If `Retention` is set it drops eligible partitions only when all their rows are terminal
  (`processed` or `dead`). Any non-terminal row aborts that destructive pass before DDL.
- The terminal-state check and one exclusive, in-place `DROP PARTITION` run behind a short
  `LOCK TABLES ... WRITE` fence, so concurrent enqueue waits and cannot enter a checked partition.
- It repeats this work every `CheckEvery` interval and exits on context cancel.

Embedded usage:

```go
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/velmie/outbox/mysql"
)

func main() {
	dsn := os.Getenv("OUTBOX_DSN")
	if dsn == "" {
		log.Fatal("OUTBOX_DSN is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	maintainer, err := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{
		Table:      "outbox",
		Period:     mysql.PartitionDay,
		Lookahead:  30 * 24 * time.Hour,
		CheckEvery: time.Hour,
		Retention:  7 * 24 * time.Hour,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := maintainer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
```

CLI usage (run on schedule with a dedicated maintenance identity). Supply `OUTBOX_DSN`
through the process environment or a secret manager. The `-dsn` flag remains available
for compatibility in v0.2.0, but it is deprecated because command arguments can be inspected.

```bash
cd cmd/outbox-partitions
go run . \
  -table outbox \
  -period day \
  -lookahead 720h \
  -retention 168h \
  -once
```

CLI details:

- The CLI performs the same plan as the embedded maintainer.
- It creates missing partitions for the requested lookahead and period.
- With `-retention` it deletes terminal-only partitions older than the retention cutoff.
- `-lookahead=0` uses 30 days for daily partitions or 90 days for monthly partitions; `-retention=0` disables removal.
- With `-once` it runs a single maintenance cycle and exits.
- Without `-once` it stays running and repeats every `-check-every`.

### Non-partitioned cleanup (optional)

If you do not use partitioning, use batched deletes to keep the outbox table bounded:

```go
package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/velmie/outbox/mysql"
)

func main() {
	dsn := os.Getenv("OUTBOX_DSN")
	if dsn == "" {
		log.Fatal("OUTBOX_DSN is required")
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	store, err := mysql.NewStore(db)
	if err != nil {
		log.Fatal(err)
	}

	result, err := store.Cleanup(context.Background(), mysql.CleanupOptions{
		Before:      time.Now().Add(-7 * 24 * time.Hour),
		Limit:       10000,
		IncludeDead: true,
	})
	log.Printf("confirmed processed=%d dead=%d", result.Processed, result.Dead)
	if err != nil {
		log.Fatal(err)
	}
}
```

`Cleanup` returns confirmed deletion counts even when a later step fails. Processed rows are deleted first,
then optional dead rows use the remaining shared limit. The two DELETEs are separate autocommitted operations,
so a dead-row failure does not roll back processed deletions. A failed DELETE or `RowsAffected` call contributes
no count for that statement. With an error, a zero count does not prove that no rows were deleted.

For automation, run the embedded maintainer or the CLI:

```go
package main

import (
	"context"
	"database/sql"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/velmie/outbox/mysql"
)

func main() {
	dsn := os.Getenv("OUTBOX_DSN")
	if dsn == "" {
		log.Fatal("OUTBOX_DSN is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	maintainer, err := mysql.NewCleanupMaintainer(db, mysql.CleanupMaintainerConfig{
		Table:        "outbox",
		Retention:    7 * 24 * time.Hour,
		CheckEvery:   time.Hour,
		Limit:        10000,
		DrainTimeout: 30 * time.Second,
		IncludeDead:  true,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := maintainer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
```

A positive `DrainTimeout` enables repeated batches, each bounded by `Limit`. After taking the lock, the
first batch always runs. After each full batch, the pass continues only while the budget has not elapsed.
The budget starts at entry to `Ensure` and never cancels a running batch. Exhaustion returns totals with a
nil error. The default zero runs one batch per pass. `CleanupResult` totals confirmed deletions, including
partial results on error. The same connection holds the advisory lock across all batches with one retention
cutoff. Cleanup work can exceed the budget by one batch. Caller cancellation still interrupts the pass,
and lock release has a separate cleanup allowance.
See the [cleanup guide](docs/guide.md#non-partitioned-cleanup-batch-delete) for timing and failure semantics.

Supply `OUTBOX_DSN` through the process environment or a secret manager. The
deprecated `-dsn` flag is retained for compatibility in v0.2.0.

```bash
cd cmd/outbox-cleanup
go run . \
  -table outbox \
  -retention 168h \
  -limit 10000 \
  -include-dead \
  -once
```

Cleanup requires a positive `-retention`. A zero `-limit` uses 10000 rows per batch. The cleanup identity needs
`SELECT` and `DELETE`, but no DDL privileges. Its default lock name is `outbox:cleanup:<table>` and follows the same
1-64 character rule as partition maintenance.

## Polling query (MySQL 8.0+)

```sql
SELECT id, aggregate_type, aggregate_id, event_type, payload, headers, created_at, attempt_count
FROM outbox
WHERE status = 0
ORDER BY id ASC
LIMIT 50
FOR UPDATE SKIP LOCKED;
```

## Practical notes

- Use `READ COMMITTED` for polling sessions to avoid gap locks.
- Use UUID v7 in `BINARY(16)` to preserve time locality in the clustered index.
- Prefer batch sizes between 50-200; too small increases round trips, too large holds locks longer.
- JSON validation is enabled by default. Use `mysql.WithValidatePayload`/`mysql.WithValidateHeaders` or
  `mysql.WithValidateJSON(false)` for fine-grained control.
- Run multiple workers in parallel; `SKIP LOCKED` provides safe work stealing.
- Polling considers every pending row, including rows with old UUID timestamps. `WithPartitionWindow`,
  `RelayConfig.PartitionWindow`, and `FetchOptions.MinCreatedAt` are deprecated and ignored.
- A non-zero `Entry.ID` must be an RFC 9562 UUID v7. Its timestamp may be old; zero IDs use the configured generator.
- Keep handler processing short; avoid network retries inside the DB transaction.
- `WithHandlerTimeout` gives each call a cooperative context deadline. The relay calls `Handle` synchronously and
  waits for it to return, so the handler must observe `ctx` to finish near the deadline.
- By default, relay-created failure details for returned errors, recovered handler panics, and errors returned after an
  elapsed handler deadline use generic summaries. `WithErrorDetails` enables the original text and panic values.
  Callbacks and classifiers receive the original error or a recovered panic with its value and cause.
- `WithShutdownTimeout` controls the shared drain allowance. It defaults to five seconds. Zero or a negative
  duration cancels admitted work immediately.
- Use `WithFailureClassifier` to mark non-retryable failures as dead immediately.
- Raise `max_allowed_packet` if your JSON payloads can be large.

## Observability

`Relay` accepts optional logger and metrics interfaces. The `Metrics` interface can be wired to
Prometheus/StatsD/OpenTelemetry. Pending sampling is disabled by default. Enable it with
`WithPendingInterval`.

Classify diagnostics by their stable `event`, `operation`, and `outcome` fields. See the
[diagnostic contract](docs/diagnostics.md) for relay and maintenance events. `*slog.Logger` implements
`outbox.Logger` directly and records the original error text.

```go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

type relayMetrics struct{}

func (relayMetrics) ObserveBatchDuration(time.Duration) {}
func (relayMetrics) AddProcessed(int)                   {}
func (relayMetrics) AddErrors(int)                      {}
func (relayMetrics) AddRetries(int)                     {}
func (relayMetrics) AddDead(int)                        {}
func (relayMetrics) SetPending(int)                     {}

func main() {
	dsn := os.Getenv("OUTBOX_DSN")
	if dsn == "" {
		log.Fatal("OUTBOX_DSN is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	store, err := mysql.NewStore(db)
	if err != nil {
		log.Fatal(err)
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := store.Enqueue(ctx, tx, outbox.Entry{
		AggregateType: "order",
		AggregateID:   "123",
		EventType:     "order.created",
		Payload:       json.RawMessage(`{"id":"123"}`),
	}); err != nil {
		_ = tx.Rollback()
		log.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}

	handler := outbox.HandlerFunc(func(ctx context.Context, record outbox.Record) error {
		return nil
	})

	relay := outbox.NewRelay(store, handler,
		outbox.WithLogger(slog.Default()),
		outbox.WithMetrics(relayMetrics{}),
		outbox.WithPendingInterval(5*time.Second),
	)

	if err := relay.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
```

If the consumer implements `PendingCounter` (the MySQL store does), pending counts are sampled and reported via
`Metrics.SetPending`.

## Benchmarks (local, dev machine)

Single-run results on the developer laptop (not production hardware). Payload 512B JSON, 200k records/run.
MySQL 8.0.36 with a 1GB buffer pool. Performance depends heavily on MySQL and IO; the gap between profiles
below is mostly durability and storage cost.

| Profile   | Durability / storage      | Consume-only peak   | Mixed peak (p99)      | Enqueue (tx) |
|-----------|---------------------------|---------------------|-----------------------|--------------|
| Prod-like | fsync + binlog sync       | ~32k msg/s (16x200) | ~207 msg/s (~64 ms)   | ~203 msg/s   |
| Fast      | tmpfs, relaxed durability | ~51k msg/s (8x100)  | ~18.7k msg/s (~10 ms) | ~36k msg/s   |

Raw insert baseline (`use_tx=false`) is ~211 msg/s (prod-like) and ~53k msg/s (fast).
Historical reports: [docs/benchmarks/results/20251225T225119Z/report.md](docs/benchmarks/results/20251225T225119Z/report.md) and
[docs/benchmarks/results/20251228T165819Z/report.md](docs/benchmarks/results/20251228T165819Z/report.md).
They describe the repository revision that produced them; use the active benchmark documentation for current behavior.
Reproduce: [docs/benchmarks.md](docs/benchmarks.md).

## Docs

The [delivery queue example](examples/delivery/README.md) exercises JetStream admission, MySQL, and HTTP
delivery against test-owned dependencies.

See [docs/release-next.md](docs/release-next.md) for unreleased changes.
See [docs/guide.md](docs/guide.md) for architecture, tuning, failure handling, cleanup, and extension notes.
See [docs/benchmarks.md](docs/benchmarks.md) for the research harness and plotting workflow.
See [docs/migration-v0.6.0.md](docs/migration-v0.6.0.md) and [docs/release-v0.6.0.md](docs/release-v0.6.0.md)
for the delivery queue capabilities and upgrade procedure.
See [docs/migration-v0.3.0.md](docs/migration-v0.3.0.md) to upgrade from v0.2.0 and enable durable retry delay.
See [docs/release-v0.5.0.md](docs/release-v0.5.0.md) for the v0.5.0 release notes.
See [docs/release-v0.4.0.md](docs/release-v0.4.0.md) for the v0.4.0 release notes.
See [docs/release-v0.3.0.md](docs/release-v0.3.0.md) for the v0.3.0 release notes.
See [docs/migration-v0.2.0.md](docs/migration-v0.2.0.md) before upgrading from v0.1.1.
See [docs/release-v0.2.0.md](docs/release-v0.2.0.md) for the v0.2.0 release notes.

## Testing

```bash
for module in . mysql cmd; do
  (cd "$module" && go test -race ./... && go vet ./...)
done
(cd examples/delivery && GOWORK=off go test -race ./... && GOWORK=off go vet ./...)

(cd mysql && go test -count=1 -tags=integration -timeout 12m ./...)
(cd cmd && go test -count=1 -tags=integration -timeout 12m ./...)
(cd examples/delivery && GOWORK=off go test -count=1 -tags=integration -timeout 12m ./...)
```

The committed `go.work` connects the root, MySQL, and command modules. The separate example module resolves
released library versions. Integration tests use Testcontainers and require a working Docker daemon; container
startup failures fail the suite. CLI integration tests build the binaries and run them against real MySQL.

Run only the CLI integration tests:

```bash
cd cmd
go test -tags=integration -timeout 5m ./outbox-cleanup ./outbox-partitions ./outbox-requeue
```

## Lint

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
repo_root="$(pwd)"
for module in . mysql cmd; do
  (cd "$module" && golangci-lint run --config "$repo_root/.golangci.yml" ./...)
done
(cd examples/delivery && GOWORK=off golangci-lint run --config "$repo_root/.golangci.yml" ./...)
```

The complete gate requires Go 1.26.11, Git, tar, zip, Docker, golangci-lint 2.12.2, govulncheck 1.7.0, and Trivy 0.74.0.
Run `./scripts/verify.sh` for the library, commands, and delivery example. It verifies the v0.6.0 candidate module
graph without `go.work`, then runs integration tests, `govulncheck`, and Trivy.

## License

MIT - see [LICENSE](LICENSE).
