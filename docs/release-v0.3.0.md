# outbox v0.3.0

v0.3.0 adds opt-in durable retry delay to the MySQL adapter. After a failed delivery, a committed database
deadline prevents participating consumers from fetching the record again before it is due, including after a
process restart. Other eligible records can proceed during the delay.

This is a coordinated release of:

- `github.com/velmie/outbox` at `v0.3.0`;
- `github.com/velmie/outbox/mysql` at `mysql/v0.3.0`;
- `github.com/velmie/outbox/cmd` at `cmd/v0.3.0`.

Keep modules and command installations aligned at v0.3.0. The MySQL adapter and command modules now require
Go 1.26 or later. The root library alone retains Go 1.25 support; MySQL 8.0 remains the database minimum.

## Retry scheduling

- `mysql.WithRetryDelay` enables a fixed delay. Zero disables scheduling; negative values return
  `mysql.ErrRetryDelayInvalid`. Positive durations round up to microsecond precision.
- `mysql.RetrySchema` generates a non-partitioned JSON table with a nullable `next_attempt_at` column and a retry
  eligibility index. Existing tables require an explicit migration; the store never runs DDL.
- `Batch.Fail` writes the attempt count, failure, status, and deadline in the same transaction. Deadlines and
  fetch eligibility use the database's UTC clock, independently of `mysql.WithClock`.
- Delayed records remain pending, appear in pending counts, and survive cleanup. Exhausted retries and permanent
  failures still become dead records.

## Compatibility and operation

The MySQL and command module graphs update `golang.org/x/crypto` to v0.56.0, which requires Go 1.26. This resolves
SSH vulnerabilities, including denial-of-service paths reachable through Testcontainers in integration tests.
Update build and CI toolchains before upgrading these modules, even if retry scheduling remains disabled.

Existing schema helpers, enqueue calls, and stores with scheduling disabled keep their v0.2.0 behavior and do
not require a schema change. No exported production identifier is removed or renamed. Binary and partitioned
tables can enable scheduling through the explicit column-and-index migration while retaining their definitions.

Stop consumers that ignore deadlines before enabling scheduling on a shared table. Every replacement consumer
must use a positive retry delay; an older or scheduling-disabled consumer can fetch a delayed record immediately.
Producers can continue using the existing enqueue contract.

Delivery remains at least once, with no strict completion ordering. A deadline becomes durable only when the
failure transaction commits, and the delay is measured from the failure update. Publish-before-acknowledgement
failures can repeat a message with the same `Record.ID`; consumers still need idempotent processing.

See the [v0.3.0 migration guide](migration-v0.3.0.md) for installation, activation, verification, and rollback.
