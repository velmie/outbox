# outbox

[![Go Reference](https://pkg.go.dev/badge/github.com/velmie/outbox.svg)](https://pkg.go.dev/github.com/velmie/outbox)
[![Go Version](https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go)](mysql/go.mod)
[![License](https://img.shields.io/github/license/velmie/outbox)](LICENSE)

`outbox` is a high-performance transactional outbox library. It ships with a MySQL 8.0+ backend optimized for polling
with
`READ COMMITTED` + `SKIP LOCKED`, UUID v7 identifiers in `BINARY(16)`, batch processing, and partition-based retention.

## Installation

The MySQL adapter and command modules require Go 1.26+. The root library alone supports Go 1.25+.

```bash
go get github.com/velmie/outbox
go get github.com/velmie/outbox/mysql
```

Install `github.com/velmie/outbox/mysql` when you use the MySQL adapter.

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
	"errors"
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

	if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
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

## Package layout

- `outbox` (root): core abstractions (`Relay`, `Handler`, `Consumer`, `Batch`, `IDGenerator`, `Clock`).
- `mysql/`: MySQL 8.0+ adapter (schema helpers + polling consumer + enqueue).

## Defaults

- `Relay`: batch size 50, poll interval 50ms, workers 1.
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
the additional column. A zero delay disables scheduling; negative values are rejected. Positive durations round
up to the next microsecond, matching MySQL's timestamp precision.

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
scheduling retries on that table, and configure every replacement consumer with a positive retry delay. A legacy
consumer, including a new store with scheduling disabled, would fetch delayed rows immediately. Producers can
continue using the existing enqueue contract. Missing scheduling schema causes a database error rather than a
fallback to immediate retries. `RetrySchema` creates a non-partitioned JSON table; existing binary or partitioned
tables can opt in through the explicit migration while retaining their payload and partition definitions.

## MySQL schema

Generate with `mysql.Schema("outbox")` or use the template below. Apply generated DDL through a controlled migration;
`CREATE TABLE IF NOT EXISTS` does not convert an existing table to InnoDB or change its partition layout.
The schema is tuned for UUID v7 + polling, with `created_ts` used as the partition key for retention.

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
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("processed=%d dead=%d", result.Processed, result.Dead)
}
```

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
		Table:       "outbox",
		Retention:   7 * 24 * time.Hour,
		CheckEvery:  time.Hour,
		Limit:       10000,
		IncludeDead: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := maintainer.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
```

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
- Relay-created failure details for returned errors, recovered handler panics, and errors returned after an elapsed
  handler deadline are limited to `outbox handler failed`, `outbox handler panicked`, and `outbox handler timed out`.
  Panic values are discarded.
- `WithErrorHandler` and `WithFailureClassifier` receive the original returned handler error and full record for
  diagnostics and policy. These explicitly configured callbacks must not log or persist secrets blindly.
- Use `WithFailureClassifier` to mark non-retryable failures as dead immediately.
- Raise `max_allowed_packet` if your JSON payloads can be large.

## Observability

`Relay` accepts optional logger and metrics interfaces. The `Metrics` interface can be wired to
Prometheus/StatsD/OpenTelemetry. Pending sampling is disabled by default. Enable it with
`WithPendingInterval`.

Classify diagnostics by their stable `event`, `operation`, and `outcome` fields. See the
[diagnostic contract](docs/diagnostics.md) for relay and maintenance events and a tested safe logger adapter.
Original errors remain available to adapters for inspection and require deliberate redaction before output.

```go
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log"
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

type stdLogger struct{}

func (stdLogger) Debug(msg string, args ...any) { log.Printf("DEBUG %s %v", msg, args) }
func (stdLogger) Info(msg string, args ...any)  { log.Printf("INFO %s %v", msg, args) }
func (stdLogger) Warn(msg string, args ...any)  { log.Printf("WARN %s %v", msg, args) }
func (stdLogger) Error(msg string, args ...any) { log.Printf("ERROR %s %v", msg, args) }

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
		outbox.WithLogger(stdLogger{}),
		outbox.WithMetrics(relayMetrics{}),
		outbox.WithPendingInterval(5*time.Second),
	)

	if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
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

See [docs/guide.md](docs/guide.md) for architecture, tuning, failure handling, cleanup, and extension notes.
See [docs/benchmarks.md](docs/benchmarks.md) for the research harness and plotting workflow.
See [docs/migration-v0.3.0.md](docs/migration-v0.3.0.md) to upgrade from v0.2.0 and enable durable retry delay.
See [docs/release-v0.3.0.md](docs/release-v0.3.0.md) for the v0.3.0 release notes.
See [docs/migration-v0.2.0.md](docs/migration-v0.2.0.md) before upgrading from v0.1.1.
See [docs/release-v0.2.0.md](docs/release-v0.2.0.md) for the v0.2.0 release notes.

## Testing

```bash
for module in . mysql cmd; do
  (cd "$module" && go test -race ./... && go vet ./...)
done

(cd mysql && go test -count=1 -tags=integration -timeout 12m ./...)
(cd cmd && go test -count=1 -tags=integration -timeout 12m ./...)
```

The committed `go.work` makes the nested modules use the current checkout. Integration tests use Testcontainers and
require a working Docker daemon; container startup failures fail the suite.
CLI integration tests in `cmd/outbox-cleanup` and `cmd/outbox-partitions` build the binaries and run them in a container against a real MySQL container.

Run only the CLI integration tests:

```bash
cd cmd
go test -tags=integration -timeout 5m ./outbox-cleanup ./outbox-partitions
```

## Lint

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
repo_root="$(pwd)"
for module in . mysql cmd; do
  (cd "$module" && golangci-lint run --config "$repo_root/.golangci.yml" ./...)
done
```

The complete gate requires Go 1.26.7, Git, tar, zip, Docker, golangci-lint 2.12.2, govulncheck 1.7.0, and Trivy 0.74.0.
Run `./scripts/verify.sh` for the root, `mysql`, and `cmd` release gate. It verifies the v0.3.0 candidate module graph
without `go.work`, then runs integration tests, `govulncheck`, and Trivy.

## License

MIT - see [LICENSE](LICENSE).
