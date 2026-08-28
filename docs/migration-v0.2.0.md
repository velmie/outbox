# Upgrade from v0.1.1 to v0.2.0

This guide upgrades the outbox library, MySQL adapter, and command-line tools from v0.1.1 to v0.2.0. Version 0.2.0 requires Go 1.25 or later.

## 1. Align the module and tool versions

Upgrade the root and MySQL modules together:

```bash
go get github.com/velmie/outbox@v0.2.0
go get github.com/velmie/outbox/mysql@v0.2.0
go mod tidy
```

Install each command-line tool that the deployment uses from the matching `cmd` module release:

```bash
go install github.com/velmie/outbox/cmd/outbox-partitions@v0.2.0
go install github.com/velmie/outbox/cmd/outbox-cleanup@v0.2.0
go install github.com/velmie/outbox/cmd/outbox-bench@v0.2.0
```

Do not mix v0.1.1 and v0.2.0 modules or tools. Verify the library versions:

```bash
go version
go list -m -f '{{.Path}} {{.Version}}' \
  github.com/velmie/outbox \
  github.com/velmie/outbox/mysql
```

## 2. Move CLI database credentials out of arguments

Provide the MySQL DSN to every outbox command through `OUTBOX_DSN`, using the deployment's secret injection mechanism. Remove `-dsn` from scripts, process specifications, and examples because command arguments may be visible to other users or observability systems.

The `-dsn` flag remains available for compatibility in v0.2.0 but is deprecated. During a transition, a non-empty `-dsn` value takes precedence over `OUTBOX_DSN`.

## 3. Check ID generation and the pending backlog

Make these changes before starting the upgraded relay:

1. Remove any filtering assumptions based on `WithPartitionWindow`, `RelayConfig.PartitionWindow`, or `FetchOptions.MinCreatedAt`. These APIs remain source-compatible but are deprecated and ignored. The benchmark `-partition-window` flag is also ignored.
2. Measure the full pending backlog. Every pending record is eligible after the upgrade, including records with old UUID timestamps.
3. Ensure every new non-zero `Entry.ID` and every custom `IDGenerator` result is an RFC 9562 UUIDv7. A zero `Entry.ID` still uses the store's UUIDv7 generator.
4. Keep existing stored IDs unchanged. Reading stored IDs does not enforce their UUID version, so legacy records remain readable.
5. Use `Record.ID` as the stable published message ID and downstream deduplication key.

For the default schema, inspect the backlog with:

```sql
SELECT COUNT(*) AS pending
FROM outbox
WHERE status = 0;
```

Delivery remains at least once. A publish can succeed before the relay transaction commits, so downstream effects must be idempotent.

## 4. Audit the table before enabling maintenance

Inspect the existing table rather than relying on the schema helper to modify it:

```sql
SHOW CREATE TABLE outbox;
```

The v0.2.0 schema helpers emit `ENGINE=InnoDB`, but their `CREATE TABLE IF NOT EXISTS` statement does not convert an existing table. Convert a non-InnoDB table with an explicit, reviewed migration before starting partition maintenance.

For `PartitionMaintainer`, verify all of these conditions:

- the table uses InnoDB;
- partitioning is exactly `RANGE(created_ts)` with no subpartitions;
- a `MAXVALUE` tail partition exists;
- every existing partition name matches `[A-Za-z_][A-Za-z0-9_]{0,63}`.

The maintainer rejects unsupported layouts and invalid partition metadata instead of guessing. Grant its account `SELECT`, `ALTER`, `CREATE`, and `INSERT` for the target table and schema. When partition retention is enabled, also grant schema-level `LOCK TABLES` and `DROP`; retention briefly takes an exclusive table lock before dropping terminal-only partitions. If any candidate partition contains a non-terminal row, the entire drop pass stops without removing a partition.

Use one stable named lock for each operation and table on every instance. A lock name must contain 1 to 64 valid UTF-8 Unicode characters. If omitted, the defaults are:

- `outbox:partitions:<table>` for partition maintenance;
- `outbox:cleanup:<table>` for non-partitioned cleanup.

Use `CleanupMaintainer` only for a non-partitioned table. Its account needs `SELECT` and `DELETE`. Set a positive retention period; a cleanup limit of `0` uses the default of 10,000 rows per run.

## 5. Update handler failure and timeout handling

Treat `WithHandlerTimeout` as a cooperative deadline. The relay calls `Handler.Handle` synchronously and waits for it to return, so the handler and its dependencies must observe context cancellation.

A panic inside `Handler.Handle` becomes a per-record failure and follows the normal retry/dead accounting path; its panic value is discarded. Relay-created `last_error` values now contain generic failure categories instead of the original handler error. `FailureHandler` and `FailureClassifier` still receive the original returned error and record. Their implementations own redaction, logging, metric labels, and any other confidentiality controls. Direct `Batch.Fail` and `DeadBatch.Dead` callers remain responsible for sanitizing the errors they supply.

## 6. Verify the upgrade

Run the application's tests and race-sensitive tests that cover producer, relay, and shutdown behavior:

```bash
go test ./...
go test -race ./...
```

In a staging environment, run only the maintenance command that matches the table layout, with `OUTBOX_DSN` supplied by the secret mechanism:

```bash
outbox-partitions -table outbox -period day -once
```

or, for a non-partitioned table:

```bash
outbox-cleanup -table outbox -retention 168h -once
```

Before production rollout, confirm that the applicable one-shot command succeeds, old pending records are draining, retries and dead records remain observable, and logs contain neither DSNs nor handler secrets.
