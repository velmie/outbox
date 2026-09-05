# outbox guide

This guide covers the root library and MySQL adapter for production use. The MySQL adapter and commands require
Go 1.26+; the root library alone supports Go 1.25+. The MySQL adapter requires MySQL 8.0+.

For the v0.1.1 to v0.2.0 transition, follow [the migration guide](migration-v0.2.0.md).
For v0.3.0 and optional durable retry scheduling, follow [the v0.3.0 migration guide](migration-v0.3.0.md).

## Goals

- Solve the dual-write problem using a transactional outbox.
- Stay polling-based without sacrificing throughput on MySQL 8.0+.
- Keep adapter boundaries small enough for another database implementation when one has a current consumer.

## Core concepts

### Entry

`outbox.Entry` describes a new outbox row to be persisted inside your business transaction.
Required fields:
- `AggregateType` (logical stream name, e.g. `order`)
- `EventType` (event name, e.g. `order.created`)
- `Payload` (valid JSON, stored in MySQL JSON column)

Optional fields:
- `AggregateID` (stream instance id)
- `Headers` (JSON metadata)
- `ID` (if zero, UUID v7 is generated; a non-zero value must be an RFC 9562 UUID v7, but its timestamp may be old)

### Record

`outbox.Record` is the stored representation returned by polling. It includes:
- `ID`, `AggregateType`, `AggregateID`, `EventType`
- `Payload`, `Headers`
- `CreatedAt`, `Attempts`

### Status

Records move through these states:
- `StatusPending` (0): ready for processing
- `StatusProcessed` (1): processed successfully
- `StatusDead` (-1): terminal because retries were exhausted or a classifier marked the failure non-retryable

## Flow

1. Start a business transaction.
2. Write domain data and call `Store.Enqueue` in the same transaction.
3. Commit the transaction.
4. A `Relay` polls, locks, processes, and updates outbox rows.

Delivery is at least once: publication may succeed before the relay can acknowledge and commit the row, so handlers
and downstream consumers must be idempotent. Publish `Record.ID` as the stable message identifier so downstream
consumers can deduplicate retries.

## Core API

### Handler

`Handler` processes a single record:

```go
err := handler.Handle(ctx, record)
```

Errors and handler panics trigger retry/dead accounting. Relay-created `Failure.Err` values contain only stable,
secret-safe summaries. You can register an optional `FailureHandler` for logging or metrics; it receives the original
returned handler error and full record, so it owns safe handling of those details. Panic values are discarded.

### Consumer and Batch

`Consumer` fetches a locked batch and returns a `Batch` handle. The batch is responsible for:
- `Ack` (mark processed)
- `Fail` (increment attempts, possibly dead-letter)
- `Commit` / `Rollback`

`mysql.Store` implements `Consumer`.

Contract note: `Fetch` must return `ErrNoRecords` when there is no work and must not return an empty batch. An empty batch is treated as `ErrEmptyBatch`.

### Relay

`Relay` coordinates polling and processing:
- `Run` runs worker goroutines that fetch batches and dispatch to the handler.
- `ProcessOnce` processes a single batch.

Useful options:
- `WithHandlerTimeout` to give each handler call a cooperative context deadline. Relay invokes `Handle` synchronously
  and waits for it to return; a handler that ignores `ctx` is not forcibly interrupted.
- `WithFailureClassifier` to decide retry vs dead-letter on errors.
- `WithLogger` / `WithMetrics` to plug in observability.
- `WithPendingInterval` to enable pending sampling and set the minimum interval.

## MySQL adapter details

### Polling query

Without retry scheduling, the adapter uses this query:

```sql
SELECT id, aggregate_type, aggregate_id, event_type, payload, headers, created_at, attempt_count
FROM outbox
WHERE status = 0
ORDER BY id ASC
LIMIT ?
FOR UPDATE SKIP LOCKED;
```

Key properties:
- `READ COMMITTED` avoids gap locks and blocking inserts.
- `SKIP LOCKED` allows multiple workers to progress without waiting.
- `ORDER BY id` benefits from UUID v7 ordering.
- `LIMIT` keeps transactions short.
- The predicate considers the entire pending backlog; pending rows remain eligible regardless of UUID timestamp.

With `mysql.WithRetryDelay` set to a positive duration, the predicate also requires
`next_attempt_at IS NULL OR next_attempt_at <= UTC_TIMESTAMP(6)`. This temporarily excludes delayed retries while
keeping the same ID ordering and locking behavior. Pending counts include delayed rows.

### Enqueue semantics

`Store.Enqueue` uses the provided executor (typically `*sql.Tx`) so you can ensure atomicity with your domain write.
JSON validation is enabled by default; disable it for high-throughput scenarios with `mysql.WithValidateJSON(false)` or use `mysql.WithValidatePayload` / `mysql.WithValidateHeaders` for granular control.

### Binary payloads (optional)

If you do not want JSON payloads, use the binary schema helpers and store raw bytes:

```go
schema, err := mysql.SchemaBinary("outbox")
```

Partitioned variant:

```go
schema, err := mysql.PartitionedSchemaBinary("outbox", partitions)
```

When using binary payloads, disable payload validation:

```go
store, err := mysql.NewStore(db, mysql.WithValidatePayload(false))
```

### Failure semantics

Each `Fail` call:
- increments `attempt_count`
- sets `last_error` (truncated to 1024 chars). Relay-created failures use only `outbox handler failed`,
  `outbox handler panicked`, or `outbox handler timed out`; direct `Batch.Fail` callers own the safety of supplied errors.
- keeps `status = pending` until `attempt_count` reaches `MaxAttempts`, then sets `status = dead`

With `mysql.WithRetryDelay` enabled, `Fail` also writes a database-clock deadline to `next_attempt_at` in the same
transaction. A commit makes both the attempt and deadline durable; a rollback restores both. The delay starts
at the failure update, so a slow commit can consume part or all of it. New entries have a null deadline and are
immediately eligible. Processed and dead rows remain terminal regardless of any stored deadline.

The default delay is zero, which preserves immediate retry eligibility on the original schema. A positive delay
requires the scheduling schema and consumers that all honor deadlines. See the
[activation and rollback procedure](migration-v0.3.0.md) before enabling it on an existing table.

`Relay` can classify failures with `FailureClassifier`. When it returns:
- `FailureRetry`: `Fail` is called and attempts are incremented.
- `FailureDead`: `Dead` is called (if the batch supports `DeadBatch`), otherwise it falls back to `Fail`.

To inspect dead-lettered rows:

```sql
SELECT * FROM outbox WHERE status = -1 ORDER BY created_at DESC;
```

## Schema guidance

Generate schema DDL with the package helpers and apply it through a controlled migration or bootstrap identity. The
runtime application identity should not need schema-management privileges. `CREATE TABLE IF NOT EXISTS` does not
convert an existing table to InnoDB or change its partition layout.

### UUID v7 in BINARY(16)

- UUID v7 is time ordered (48-bit Unix milliseconds prefix).
- `BINARY(16)` halves index size vs `CHAR(36)` and improves cache locality.
- `ORDER BY id` benefits from UUIDv7 time locality.

### Generated timestamp for partitioning

The schema includes a `created_ts` column derived from UUID v7:

```sql
created_ts BIGINT GENERATED ALWAYS AS (CONV(SUBSTR(HEX(id), 1, 12), 16, 10) DIV 1000) STORED
```

- It stores Unix seconds and provides the range-partition key used for retention and rotation.
- MySQL requires `STORED` when the column participates in the primary key.
- The expression is evaluated on insert. For very high ingest rates, budget CPU accordingly.

### Indexing

`INDEX idx_status_id (status, id)` matches the polling query and avoids filesort.

### Partitioning + cleanup

Use InnoDB range partitions on `created_ts` and drop old, terminal-only partitions instead of deleting rows.
`DROP PARTITION` discards every row in the target partition, so use the maintainer below to fence concurrent
enqueue and verify terminal state before DDL:

```sql
ALTER TABLE outbox DROP PARTITION p202501;
```

For `RANGE` partitions MySQL can perform this in place without copying the remaining table, avoiding expensive
row-by-row delete/undo work.

#### Why `pmax` matters

Always keep a tail partition `pmax VALUES LESS THAN (MAXVALUE)`. It prevents `INSERT` failures if a new partition was not created in time and lets the maintainer split it safely.

#### Partition rotation (example)

Pre-create future partitions and drop expired ones on a schedule. When you keep `pmax`, you must split it with `REORGANIZE` to insert new ranges:

```sql
-- Add tomorrow's partition (UTC) by splitting pmax.
ALTER TABLE outbox REORGANIZE PARTITION pmax INTO (
    PARTITION p20250303 VALUES LESS THAN (UNIX_TIMESTAMP('2025-03-04')),
    PARTITION pmax VALUES LESS THAN (MAXVALUE)
);

-- Destructive: the maintainer runs this only after its locked terminal-state check.
ALTER TABLE outbox ALGORITHM=INPLACE, LOCK=EXCLUSIVE, DROP PARTITION p20250201;
```

Prefer the built-in maintainer or standalone CLI below instead of embedding database
credentials and DDL in a cron command.

#### Built-in partition maintainer

`mysql.PartitionMaintainer` keeps partitions ahead of time and optionally drops old, terminal-only ones. It verifies
an InnoDB `RANGE(created_ts)` layout, uses `GET_LOCK` on a single session, reads `information_schema`, and splits
`pmax` via `REORGANIZE PARTITION` so missing ranges can be inserted before `MAXVALUE`. Before destructive DDL it
takes a short `LOCK TABLES ... WRITE` fence, revalidates and replans, and aborts without dropping anything if any
candidate contains a non-terminal row. Concurrent enqueue waits for this fenced check/drop section and resumes
against the remaining partition map.

Partition names are generated as:
- daily: `pYYYYMMDD`
- monthly: `pYYYYMM`

Use the embedded maintainer only when granting DDL privileges to the service identity is an accepted deployment choice:

- your service runs 24/7,
- the DB user has the required maintenance grants,
- you want the simplest deployment with no extra infrastructure.

Permissions required:

- Partition creation needs `SELECT`, `ALTER`, `CREATE`, and `INSERT` on the target table/schema.
- Retention additionally needs `DROP` and schema-level `LOCK TABLES`; without `Retention`, those privileges are unused.
- `GET_LOCK` and `RELEASE_LOCK` are built-in MySQL functions and need no additional grant.

Use one stable `LockName` across instances for the same operation and table. It must contain 1-64 valid UTF-8
characters. The default is `outbox:partitions:<table>`; set a shorter explicit name when a qualified table name would
make the default too long. A pass is skipped when another session owns the same named lock.

Operational note: retention briefly blocks reads and writes while it verifies and drops eligible partitions, so keep
`CheckEvery` reasonably large (hourly/daily) and batch changes.
Operational note: DDL operations are logged at Info level via the maintainer logger.

Example:

```go
maintainer, err := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{
	Table:      "outbox",
	Period:     mysql.PartitionDay,
	Lookahead:  30 * 24 * time.Hour,
	CheckEvery: time.Hour,
	Retention:  7 * 24 * time.Hour, // optional
})
if err != nil {
	return err
}

if err := maintainer.Run(ctx); err != nil {
	return err // Treat context cancellation according to the service lifecycle.
}
```

#### Standalone CLI (cron / CronJob)

Use the CLI when `ALTER` privileges cannot be granted to the app or when you want a dedicated ops job.
Supply `OUTBOX_DSN` through the process environment or a secret manager. The `-dsn` flag remains
available for compatibility in v0.2.0, but it is deprecated because command arguments can be inspected.

```bash
cd cmd/outbox-partitions
go run . \
  -table outbox \
  -period day \
  -lookahead 720h \
  -retention 168h \
  -once
```

`-lookahead=0` uses 30 days for daily partitions or 90 days for monthly partitions. `-retention=0` disables removal.

### Non-partitioned cleanup (batch DELETE)

If you do **not** use partitioning, cleanup requires periodic batched deletes. This is less efficient than dropping partitions, but it keeps tables bounded.

Use `Store.Cleanup` when you want to trigger cleanup manually:

```go
result, err := store.Cleanup(ctx, mysql.CleanupOptions{
	Before:      time.Now().Add(-7 * 24 * time.Hour),
	Limit:       10000,
	IncludeDead: true,
})
if err != nil {
	return err
}
_ = result
```

For automated cleanup in the application, use `mysql.CleanupMaintainer`:

```go
maintainer, err := mysql.NewCleanupMaintainer(db, mysql.CleanupMaintainerConfig{
	Table:       "outbox",
	Retention:   7 * 24 * time.Hour,
	CheckEvery:  time.Hour,
	Limit:       10000,
	IncludeDead: true,
})
if err != nil {
	return err
}

if err := maintainer.Run(ctx); err != nil {
	return err // Treat context cancellation according to the service lifecycle.
}
```

Standalone CLI (cron / CronJob). Supply `OUTBOX_DSN` through the process environment
or a secret manager; the deprecated `-dsn` flag is retained for compatibility in v0.2.0:

```bash
cd cmd/outbox-cleanup
go run . \
  -table outbox \
  -retention 168h \
  -limit 10000 \
  -include-dead \
  -once
```

Cleanup requires a positive `Retention` or `-retention`. A zero `Limit` or `-limit` uses 10000 rows per batch. The
cleanup identity needs `SELECT` and `DELETE`, but no DDL privileges. Use one stable 1-64 character lock name across
instances; the default is `outbox:cleanup:<table>`. A pass is skipped while another session owns that name.

Operational note: batched deletes can still create I/O and undo pressure. For large tables, prefer partitioning or schedule cleanup during off-peak hours. If cleanup becomes slow, consider adding composite indexes for the cutoff columns (for example, `(status, processed_at)` or `(status, updated_at)`).

The CLI reuses the same maintainer logic, so behavior is identical; it just runs under a different user and schedule.

## Practical recommendations

### Polling and batching

- Start with batch size 50 and scale to 100 based on handler latency.
- Set worker count to the number of CPU cores or slightly above.
- Keep handler work short; avoid network retries inside the DB transaction.
- Set `WithHandlerTimeout` to provide a cooperative deadline. Default `0` means no added deadline. The relay waits for
  `Handle` to return, so handlers must observe context cancellation; the option does not forcibly stop stuck code.

### Pending eligibility

- Polling considers every pending row, including rows with old UUID timestamps.
- `outbox.WithPartitionWindow`, `RelayConfig.PartitionWindow`, and `FetchOptions.MinCreatedAt` are deprecated and ignored.
- Set daily or hourly partitions to match operational retention and cleanup needs, not to exclude older pending rows.

### MySQL tuning

Recommended baseline for write-heavy outbox tables:
- `innodb_buffer_pool_size`: 70-80% of RAM
- `innodb_io_capacity`: SSD tuning (2000-5000+)
- `innodb_flush_log_at_trx_commit`: use 1 for strict durability
- `innodb_log_file_size`: large enough for peak write throughput
- `max_allowed_packet`: increase if JSON payloads can be large

### Error handling

- A handler panic becomes a generic per-record failure and does not stop the worker. Other panics before commit roll
  back the batch; `Run` reports `ErrWorkerPanic` without the panic value.
- Provide a `FailureHandler` to capture metrics and logs. It and `FailureClassifier` receive the original returned
  handler error and full record, so configured callbacks must avoid exposing sensitive values.
- Route `status = -1` rows to a manual DLQ workflow.

### Observability

- Track counts for `pending`, `processed`, `dead`.
- Measure `attempt_count` distribution and handler latency.
- Plug in your logger/metrics via `WithLogger` and `WithMetrics`.
- Pending sampling is disabled by default.
- If the consumer implements `PendingCounter` (MySQL does), `Relay` samples pending counts when you enable `WithPendingInterval` and reports them via `Metrics.SetPending`.

Example stub:

```go
type relayMetrics struct{}

func (relayMetrics) ObserveBatchDuration(time.Duration) {}
func (relayMetrics) AddProcessed(int)                   {}
func (relayMetrics) AddErrors(int)                      {}
func (relayMetrics) AddRetries(int)                     {}
func (relayMetrics) AddDead(int)                        {}
func (relayMetrics) SetPending(int)                     {}

relay := outbox.NewRelay(store, handler, outbox.WithMetrics(relayMetrics{}), outbox.WithPendingInterval(5*time.Second))
```

## Extending to other databases

Implement these interfaces in a new package (for example `postgres`):
- `Consumer` and `Batch` for polling + locking
- `Enqueue` using a transaction executor

Keep the same contract:
- `Fetch` returns locked rows in a transaction (or `ErrNoRecords`)
- `Ack` and `Fail` update records inside the same transaction
- `Commit` or `Rollback` finalizes the batch

## Testing and benchmarks

- Complete release gate for all three modules: `./scripts/verify.sh`
- Root-module benchmarks: `go test -bench=. ./...`
- MySQL and CLI integration tests require a working Docker daemon and fail when their containers cannot start.
