# Upgrade to outbox v0.6.0

The root, MySQL, and command modules form a coordinated release. Update their requirements together.
Existing stores remain unscoped, use immediate retry eligibility, and persist generic relay failure
summaries by default. Empty deduplication keys continue to use the original insert. These defaults do
not require a schema change.

`Entry` and `Failure` have new fields. Use keyed struct literals when constructing them. Existing
IDs and stored rows do not need to be rewritten.

## Prepare the table

Inspect the current table before applying migrations. `SchemaWithOptions` generates DDL for new tables;
`CREATE TABLE IF NOT EXISTS` does not alter an existing table. Add only the capabilities the application
will enable. For an existing table without retry scheduling, a migration can add the deadline and index:

```sql
ALTER TABLE outbox
    ADD COLUMN next_attempt_at DATETIME(6) NULL,
    ADD INDEX idx_status_next_attempt_id (status, next_attempt_at, id);
```

To support aggregate-scoped consumers, add the index that matches the table's scheduling mode:

```sql
-- With retry scheduling:
CREATE INDEX idx_aggregate_type_status_next_attempt_id
    ON outbox (aggregate_type, status, next_attempt_at, id);

-- Without retry scheduling, use this index instead:
-- CREATE INDEX idx_aggregate_type_status_id ON outbox (aggregate_type, status, id);
```

For table-wide deduplication on a non-partitioned table:

```sql
ALTER TABLE outbox
    ADD COLUMN dedup_key VARBINARY(512) NULL,
    ADD UNIQUE KEY uq_dedup_key (dedup_key);
```

Do not combine this unique key with partitioning. MySQL requires every unique key to contain the
partitioning column, which would weaken uniqueness across creation times. New rows with empty keys
store NULL and do not conflict. Backfill keys only from an established business identity. Resolve
collisions and coordinate the admission cutover when existing rows must participate in deduplication.
Use non-secret identities for deduplication keys because a MySQL duplicate-key error can contain the
conflicting value.

Binary payload storage uses `LONGBLOB` and leaves headers as JSON. Converting an existing JSON column
cannot recover original bytes already normalized by MySQL. Use `WithValidatePayload(false)` for binary
payloads. Plan DDL locks and table rebuilds for the deployed table and MySQL version.

## Activate consumers

1. Apply the required columns and indexes before enabling features.
2. Stop unscheduled pollers before any consumer starts delayed delivery. Configure every replacement
   poller with `WithRetryDelay` or `WithRetrySchedule`, including a zero-returning schedule when only
   handler hints are used.
3. Stop unscoped pollers before relying on endpoint isolation. Use one aggregate-scoped store per
   endpoint and an unscoped producer where admission spans endpoints. Database collation determines
   scope query comparisons.
4. Enable deduplication keys after coordinating existing admission paths and any required backfill.

A retryable positive hint without scheduling causes settlement to fail and the batch to roll back.
`Run` stops and returns the failure. `ProcessOnce` returns it to its caller. Terminal classifications and
exhausted attempts do not need to honor a hint. Retry scheduling uses database UTC at the failure update;
positive durations round up to microseconds and the committed deadline survives a restart.

`WithErrorDetails` is an opt-in to storing handler error text and recovered panic values. Enable it only
when those values are safe to persist. MySQL retains the first 1024 Unicode code points and needs an
`utf8mb4` error column for full Unicode. Callbacks and classifiers receive the original error regardless
of this option.

## Recover dead records

Use `Store.Requeue` or `outbox-requeue` with a positive limit and at least one selector. Selectors
intersect, and a scoped store limits the update to its aggregate type. Enable retry scheduling on the
store, or pass `-retry-scheduling` to the command, to clear `next_attempt_at`. Requeue preserves the
stored ID and deduplication key. An error does not prove no rows changed; inspect the table before an
operator decides to repeat the operation. Delivery remains at least once.
