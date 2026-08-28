# outbox v0.2.0

v0.2.0 is a coordinated release of:

- `github.com/velmie/outbox`;
- `github.com/velmie/outbox/mysql`;
- `github.com/velmie/outbox/cmd`.

Upgrade all three modules and tools together. This release requires Go 1.25 or later.

## Highlights

- All pending records remain eligible for delivery. `WithPartitionWindow`, `RelayConfig.PartitionWindow`, and `FetchOptions.MinCreatedAt` are deprecated compatibility inputs and are now ignored.
- New caller-supplied IDs and custom generator results must be RFC 9562 UUIDv7 values. Existing stored identifiers remain readable.
- MySQL schema generation now declares `ENGINE=InnoDB` and rejects unsafe partition names and bounds.
- Partition retention now fails closed: a candidate partition is removed only when every row is terminal. The final check and drop are protected against concurrent enqueue operations.
- Cleanup and partition maintenance now keep advisory locks on the database session that performs the operation and verify lock release.
- Handler panics are contained per record and follow normal retry/dead accounting. Relay-generated persisted failure details no longer expose handler errors or panic values. Handler deadlines remain cooperative.
- The command-line tools now accept database credentials through `OUTBOX_DSN`. The `-dsn` flag remains available in v0.2.0 but is deprecated.
- The Go dependency graph was refreshed, including `go-sql-driver/mysql` 1.10.0, Testcontainers 0.44.0, and Testify 1.12.1.

## Compatibility and upgrade notes

No exported production identifiers were removed or renamed, but validation and runtime behavior changed:

- Audit an existing table before enabling maintenance. `CREATE TABLE IF NOT EXISTS` does not convert an existing table to InnoDB or change its partition layout.
- Partition maintenance requires an InnoDB table partitioned exactly by `RANGE(created_ts)`, without subpartitions, and with a `MAXVALUE` tail partition.
- Retention additionally requires schema-level `LOCK TABLES` and `DROP` privileges.
- Explicit failure callbacks still receive the original returned handler error and remain responsible for redaction. Direct `Batch.Fail` and `DeadBatch.Dead` callers must also sanitize supplied errors.
- Delivery remains at least once. Publish `Record.ID` as the stable message ID and make downstream processing idempotent.

See [the v0.2.0 migration guide](migration-v0.2.0.md) for the complete upgrade and operational checklist.
