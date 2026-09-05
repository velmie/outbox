# Upgrade to outbox v0.3.0

v0.3.0 adds optional MySQL retry scheduling. Upgrading the modules alone preserves immediate retry eligibility
and requires no schema change. Enabling a positive retry delay requires both the scheduling schema and a
coordinated consumer switch. The MySQL and command modules now require Go 1.26 or later; the root library alone
still supports Go 1.25. MySQL 8.0 remains the database minimum.

## Upgrade modules and tools

Update application build images, CI jobs, and local toolchains to Go 1.26 or later before upgrading the MySQL
adapter or commands. Their `golang.org/x/crypto v0.56.0` dependency fixes SSH vulnerabilities and requires Go 1.26.
This toolchain requirement applies even when retry scheduling is disabled. Repository release checks use Go 1.26.7.

For a service using the MySQL adapter, update both module requirements:

```bash
go get github.com/velmie/outbox@v0.3.0 github.com/velmie/outbox/mysql@v0.3.0
go mod tidy
```

Keep any installed commands aligned with the same release:

```bash
go install github.com/velmie/outbox/cmd/outbox-cleanup@v0.3.0
go install github.com/velmie/outbox/cmd/outbox-partitions@v0.3.0
go install github.com/velmie/outbox/cmd/outbox-bench@v0.3.0
```

Command credentials still come from `OUTBOX_DSN`. Existing producers can continue using `Store.Enqueue` through
the business transaction. No payload, ID, status, or attempt-count rewrite is required.

## Prepare the schema

For a new non-partitioned JSON table, generate DDL with `mysql.RetrySchema("outbox")` and apply it through the
normal migration identity before starting consumers.

For an existing table, replace `outbox` below with its configured table name and apply:

```sql
ALTER TABLE outbox
    ADD COLUMN next_attempt_at DATETIME(6) NULL,
    ADD INDEX idx_status_next_attempt_id (status, next_attempt_at, id);
```

Existing records receive null deadlines and remain eligible. The migration also applies to the library's binary
and partitioned schemas; preserve their payload type, primary key, partition definitions, and existing indexes.
Inspect the deployed DDL first to avoid applying the same migration twice. Plan the DDL's metadata-lock and
rebuild impact for the actual table size and MySQL version.

The original schema helpers do not add the scheduling column, and `CREATE TABLE IF NOT EXISTS` does not upgrade
an existing table. A scheduling-enabled fetch on a table without the column returns a database error.

## Activate scheduling

1. Complete the schema migration before starting a scheduling-enabled consumer.
2. Stop all consumers that ignore deadlines and let their in-flight batch transactions finish.
3. Configure every replacement consumer of that table with a positive retry delay:

   ```go
   store, err := mysql.NewStore(db,
       mysql.WithTable("outbox"),
       mysql.WithRetryDelay(5*time.Second),
   )
   if err != nil {
       return err
   }
   ```

4. Start the replacement consumers. Producers can keep using the existing enqueue contract.

An older consumer or a v0.3.0 store with zero delay ignores stored deadlines. Mixing those consumers with
scheduling-enabled consumers cannot enforce a minimum delay. A consumer's configured delay determines the next
deadline when that consumer records a failure; changing the configuration does not rewrite existing deadlines.

Positive delays round up to microseconds. The database's UTC time at the failure update determines the deadline;
`mysql.WithClock` and the session time zone do not determine eligibility. A deadline at or before the database's
current UTC time is due. Time spent waiting to commit counts toward the delay, so there is no guaranteed full
delay after commit. Rolling back the failure transaction restores both the previous attempt count and deadline.

## Verify delivery

In an integration environment, make one handler invocation return a retryable error. After the batch commits,
confirm that the record remains pending, its attempt count increases once, and its deadline is in the future:

```sql
SELECT id, status, attempt_count, next_attempt_at, UTC_TIMESTAMP(6) AS database_now
FROM outbox
WHERE status = 0
ORDER BY id;
```

A second scheduling-enabled consumer should leave that record alone until its deadline and still deliver other
eligible records. After the deadline, retry must use the same message ID. Pending metrics include delayed rows;
cleanup must preserve them. Exhausted retries and permanent failures remain terminal.

Continue publishing `Record.ID` as the stable message identifier and make downstream processing idempotent.
Delivery remains at least once: a crash before the failure commit can permit an earlier retry, and successful
publication followed by a failed acknowledgement or commit can repeat the same message.

## Disable scheduling or roll back

Stop all scheduling-enabled consumers and finish their batch transactions before switching back to consumers
that ignore deadlines. Existing future deadlines will no longer delay delivery, so plan for an immediate retry
of the pending backlog when the replacement consumers start.

Keep the nullable column and index during the application rollback. v0.2.0 and scheduling-disabled v0.3.0 stores
ignore them and continue using the existing schema contract. Remove them only through a later controlled schema
migration after all consumers that require them have been retired.
