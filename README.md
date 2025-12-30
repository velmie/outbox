# outbox

[![Go Reference](https://pkg.go.dev/badge/github.com/velmie/outbox.svg)](https://pkg.go.dev/github.com/velmie/outbox)
[![Go Report Card](https://goreportcard.com/badge/github.com/velmie/outbox)](https://goreportcard.com/report/github.com/velmie/outbox)
[![Go Version](https://img.shields.io/badge/go-1.24%2B-00ADD8?logo=go)](go.mod)
[![License](https://img.shields.io/github/license/velmie/outbox)](LICENSE)

`outbox` is a high-performance transactional outbox library. It ships with a MySQL 8.0+ backend optimized for polling
with
`READ COMMITTED` + `SKIP LOCKED`, UUID v7 identifiers in `BINARY(16)`, batch processing, and optional partition pruning.

## Installation

Requires Go 1.24+.

```bash
go get github.com/velmie/outbox
go get github.com/velmie/outbox/mysql
```

Install `github.com/velmie/outbox/mysql` when you use the MySQL adapter.

## Quick Start

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
		dsn = "root:secret@tcp(localhost:3306)/app?parseTime=true"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	schema, err := mysql.Schema("outbox")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		log.Fatal(err)
	}

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
		// outbox.WithPartitionWindow(1*time.Hour), // enable only with partitioned tables
	)

	if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatal(err)
	}
}
```

## How it works

1. Your business transaction writes both domain data and an outbox entry.
2. A relay polls outbox rows with `SELECT ... FOR UPDATE SKIP LOCKED`.
3. Each record is handed to a handler and then marked `processed` or `dead`.

At-least-once delivery means handlers must be idempotent.

## Package layout

- `outbox` (root): core abstractions (`Relay`, `Handler`, `Consumer`, `Batch`, `IDGenerator`, `Clock`).
- `mysql/`: MySQL 8.0+ adapter (schema helpers + polling consumer + enqueue).

## Defaults

- `Relay`: batch size 50, poll interval 50ms, workers 1.
- `MySQL`: table `outbox`, max attempts 5, UUID v7 generator, JSON validation enabled.

## MySQL schema

Generate with `mysql.Schema("outbox")` or use the template below.
The schema is tuned for UUID v7 + polling, with `created_ts` used for partitioning and pruning.

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
);
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
)
PARTITION BY RANGE (created_ts) (
    PARTITION p202512 VALUES LESS THAN (1767225600),
    PARTITION p202601 VALUES LESS THAN (1769904000),
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
		{Name: "p202512", LessThan: "1767225600"},
		{Name: "p202601", LessThan: "1769904000"},
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
		dsn = "root:secret@tcp(localhost:3306)/app?parseTime=true"
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

The maintainer uses `GET_LOCK` and `information_schema.PARTITIONS`, so ensure the DB user has `ALTER` and `SELECT` on
`information_schema`.

Behavior overview:

- On startup it verifies the table is range partitioned by `created_ts`.
- It creates missing partitions for the configured lookahead window using the chosen period.
- If `Retention` is set it drops partitions older than now minus retention.
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
		dsn = "root:secret@tcp(localhost:3306)/app?parseTime=true"
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

CLI usage (run on schedule with a DB user that can `ALTER`):

```bash
cd cmd/outbox-partitions
go run . \
  -dsn "user:pass@tcp(localhost:3306)/app?parseTime=true" \
  -table outbox \
  -period day \
  -lookahead 720h \
  -retention 168h \
  -once
```

CLI details:

- The CLI performs the same plan as the embedded maintainer.
- It creates missing partitions for the requested lookahead and period.
- With `-retention` it deletes partitions older than the retention cutoff.
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
		dsn = "root:secret@tcp(localhost:3306)/app?parseTime=true"
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
		dsn = "root:secret@tcp(localhost:3306)/app?parseTime=true"
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

```bash
cd cmd/outbox-cleanup
go run . \
  -dsn "user:pass@tcp(localhost:3306)/app?parseTime=true" \
  -table outbox \
  -retention 168h \
  -limit 10000 \
  -include-dead \
  -once
```

## Polling query (MySQL 8.0+)

```sql
SELECT id, aggregate_type, aggregate_id, event_type, payload, headers
FROM outbox
WHERE status = 0
  AND created_ts >= ? -- optional, for partition pruning
ORDER BY id ASC
LIMIT 50
FOR UPDATE SKIP LOCKED;
```

## Practical notes

- Use `READ COMMITTED` for polling sessions to avoid gap locks.
- Use UUID v7 in `BINARY(16)` to keep inserts append-only in the clustered index.
- Prefer batch sizes between 50-200; too small increases round trips, too large holds locks longer.
- JSON validation is enabled by default. Use `mysql.WithValidatePayload`/`mysql.WithValidateHeaders` or
  `mysql.WithValidateJSON(false)` for fine-grained control.
- Run multiple workers in parallel; `SKIP LOCKED` provides safe work stealing.
- Set `PartitionWindow` only with partitioned tables to keep scans within hot partitions.
- Keep handler processing short; avoid network retries inside the DB transaction.
- Use `WithHandlerTimeout` to cap per-record processing time and release locks faster.
- Use `WithFailureClassifier` to mark non-retryable failures as dead immediately.
- Raise `max_allowed_packet` if your JSON payloads can be large.

## Observability

`Relay` accepts optional logger and metrics interfaces. The `Metrics` interface can be wired to
Prometheus/StatsD/OpenTelemetry. Pending sampling is disabled by default. Enable it with
`WithPendingInterval`.

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
		dsn = "root:secret@tcp(localhost:3306)/app?parseTime=true"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	schema, err := mysql.Schema("outbox")
	if err != nil {
		log.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		log.Fatal(err)
	}

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
Full reports: [docs/benchmarks/results/20251225T225119Z/report.md](docs/benchmarks/results/20251225T225119Z/report.md) and
[docs/benchmarks/results/20251228T165819Z/report.md](docs/benchmarks/results/20251228T165819Z/report.md).
Reproduce: [docs/benchmarks.md](docs/benchmarks.md).

## Docs

See [docs/guide.md](docs/guide.md) for architecture, tuning, failure handling, cleanup, and extension notes.
See [docs/benchmarks.md](docs/benchmarks.md) for the research harness and plotting workflow.

## Testing

```bash
go test ./...

go test -tags=integration ./...
```

Integration tests use testcontainers and require Docker.
CLI integration tests in `cmd/outbox-cleanup` and `cmd/outbox-partitions` build the binaries and run them in a container against a real MySQL container.

Run only the CLI integration tests:

```bash
cd cmd
go test -tags=integration -timeout 5m ./outbox-cleanup ./outbox-partitions
```

## Lint

```bash
docker run --rm -v "$(pwd)":/app:ro -w /app golangci/golangci-lint:v2.5.0 golangci-lint run ./...
```

## License

MIT - see [LICENSE](LICENSE).
