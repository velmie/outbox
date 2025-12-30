# outbox guide

This document explains every aspect of the library and summarizes practical recommendations for production use.

## Goals

- Solve the dual-write problem using a transactional outbox.
- Stay polling-based without sacrificing throughput on MySQL 8.0+.
- Provide clean, SOLID abstractions for future adapters (Postgres, SQL Server, etc).

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
- `ID` (if zero, UUID v7 is generated)

### Record

`outbox.Record` is the stored representation returned by polling. It includes:
- `ID`, `AggregateType`, `AggregateID`, `EventType`
- `Payload`, `Headers`
- `CreatedAt`, `Attempts`

### Status

Records move through these states:
- `StatusPending` (0): ready for processing
- `StatusProcessed` (1): processed successfully
- `StatusDead` (-1): exceeded retry attempts and moved to DLQ

## Flow

1. Start a business transaction.
2. Write domain data and call `Store.Enqueue` in the same transaction.
3. Commit the transaction.
4. A `Relay` polls, locks, processes, and updates outbox rows.

At-least-once delivery means handlers must be idempotent.

## Core API

### Handler

`Handler` processes a single record:

```go
err := handler.Handle(ctx, record)
```

Errors trigger retry accounting. You can register an optional `FailureHandler` for logging/metrics.

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
- `WithHandlerTimeout` to cap per-record processing time.
- `WithFailureClassifier` to decide retry vs dead-letter on errors.
- `WithLogger` / `WithMetrics` to plug in observability.
- `WithPendingInterval` to enable pending sampling and set the minimum interval.

## MySQL adapter details

### Polling query

The adapter uses a single optimized query:

```sql
SELECT id, aggregate_type, aggregate_id, event_type, payload, headers
FROM outbox
WHERE status = 0
  AND created_ts >= ? -- optional for pruning
ORDER BY id ASC
LIMIT ?
FOR UPDATE SKIP LOCKED;
```

Key properties:
- `READ COMMITTED` avoids gap locks and blocking inserts.
- `SKIP LOCKED` allows multiple workers to progress without waiting.
- `ORDER BY id` benefits from UUID v7 ordering.
- `LIMIT` keeps transactions short.

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
- sets `last_error` (truncated to 1024 chars)
- keeps `status = pending` until `attempt_count` reaches `MaxAttempts`, then sets `status = dead`

`Relay` can classify failures with `FailureClassifier`. When it returns:
- `FailureRetry`: `Fail` is called and attempts are incremented.
- `FailureDead`: `Dead` is called (if the batch supports `DeadBatch`), otherwise it falls back to `Fail`.

To inspect dead-lettered rows:

```sql
SELECT * FROM outbox WHERE status = -1 ORDER BY created_at DESC;
```

## Schema guidance

### UUID v7 in BINARY(16)

- UUID v7 is time ordered (48-bit Unix milliseconds prefix).
- `BINARY(16)` halves index size vs `CHAR(36)` and improves cache locality.
- `ORDER BY id` is chronological for UUID v7.

### Generated timestamp for partitioning

The schema includes a `created_ts` column derived from UUID v7:

```sql
created_ts BIGINT GENERATED ALWAYS AS (CONV(SUBSTR(HEX(id), 1, 12), 16, 10) DIV 1000) STORED
```

- It stores Unix seconds and supports partition pruning.
- MySQL requires `STORED` when the column participates in the primary key.
- The expression is evaluated on insert. For very high ingest rates, budget CPU accordingly.

### Indexing

`INDEX idx_status_id (status, id)` matches the polling query and avoids filesort.

### Partitioning + cleanup

Use range partitions on `created_ts` and drop old partitions instead of deleting rows:

```sql
ALTER TABLE outbox DROP PARTITION p202501;
```

This is instant and avoids expensive delete/undo work.

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

-- Drop partitions outside your retention window.
ALTER TABLE outbox DROP PARTITION p20250201;
```

Cron example (pseudo):

```
0 2 * * * mysql -u app -p*** appdb -e "ALTER TABLE outbox REORGANIZE PARTITION pmax INTO (PARTITION p$(date -u +%Y%m%d) VALUES LESS THAN (UNIX_TIMESTAMP(DATE_ADD(CURDATE(), INTERVAL 1 DAY))), PARTITION pmax VALUES LESS THAN (MAXVALUE));"
```

#### Built-in partition maintainer

`mysql.PartitionMaintainer` keeps partitions ahead of time and optionally drops old ones. It uses `GET_LOCK` on a single session, reads `information_schema.PARTITIONS`, and splits `pmax` via `REORGANIZE PARTITION` so missing ranges can be inserted before `MAXVALUE`.

Partition names are generated as:
- daily: `pYYYYMMDD`
- monthly: `pYYYYMM`

Use this when:
- your service runs 24/7,
- the DB user can run `ALTER TABLE`,
- you want the simplest deployment with no extra infrastructure.

Permissions required:
- `ALTER` on the outbox table.
- `SELECT` on `information_schema.PARTITIONS`.
- ability to call `GET_LOCK` / `RELEASE_LOCK` (built-in MySQL functions).

Operational note: `ALTER TABLE` can take a metadata lock, so keep `CheckEvery` reasonably large (hourly/daily) and batch changes.
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

go func() {
	_ = maintainer.Run(ctx)
}()
```

#### Standalone CLI (cron / CronJob)

Use the CLI when `ALTER` privileges cannot be granted to the app or when you want a dedicated ops job:

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

go func() {
	_ = maintainer.Run(ctx)
}()
```

Standalone CLI (cron / CronJob):

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

Operational note: batched deletes can still create I/O and undo pressure. For large tables, prefer partitioning or schedule cleanup during off-peak hours. If cleanup becomes slow, consider adding composite indexes for the cutoff columns (for example, `(status, processed_at)` or `(status, updated_at)`).

The CLI reuses the same maintainer logic, so behavior is identical; it just runs under a different user and schedule.

## Practical recommendations

### Polling and batching

- Start with batch size 50 and scale to 100 based on handler latency.
- Set worker count to the number of CPU cores or slightly above.
- Keep handler work short; avoid network retries inside the DB transaction.
- Set `WithHandlerTimeout` to avoid stuck handlers. Default `0` means no timeout.

### Partition pruning

- Use `Relay.WithPartitionWindow` (e.g. 1h) to limit scans to hot partitions.
- Set partitions that match operational retention (daily or hourly partitions).

### MySQL tuning

Recommended baseline for write-heavy outbox tables:
- `innodb_buffer_pool_size`: 70-80% of RAM
- `innodb_io_capacity`: SSD tuning (2000-5000+)
- `innodb_flush_log_at_trx_commit`: use 1 for strict durability
- `innodb_log_file_size`: large enough for peak write throughput
- `max_allowed_packet`: increase if JSON payloads can be large

### Error handling

- Provide a `FailureHandler` to capture metrics and logs.
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

- Unit tests: `go test ./...`
- Integration tests (Docker): `go test -tags=integration ./...`
- Benchmarks: `go test -bench=. ./...`
