# outbox v0.5.0

v0.5.0 is a coordinated release of `github.com/velmie/outbox` at `v0.5.0`,
`github.com/velmie/outbox/mysql` at `mysql/v0.5.0`, and `github.com/velmie/outbox/cmd` at `cmd/v0.5.0`.
Keep modules and commands aligned. The root library requires Go 1.25 or later; the MySQL and command modules
require Go 1.26.10 or later. MySQL 8.0 remains the minimum. The Go patch and the MySQL and command modules'
`github.com/klauspost/compress` dependency are updated to address security vulnerabilities.

## Relay shutdown

`Relay.Run` now drains admitted polling iterations for up to five seconds by default after parent cancellation,
parent deadline expiry, or a fatal worker error. A fetch already admitted and its full batch transaction can finish.
All workers share the allowance. The parent deadline requests stopping and no longer bounds an admitted transaction.
Set `outbox.WithShutdownTimeout` to another duration, or to zero or a negative value for immediate cancellation.

`Run` waits for workers after canceling their work. A handler, callback, logger, or driver commit that does not
return promptly can extend the return time beyond the allowance. Keep the database, broker, and telemetry available
until `Run` returns. `ProcessOnce` still uses its caller's context directly.

Parent cancellation and deadline expiry return `nil` if shutdown completes without errors and without exhausting
the drain budget. Inspect the parent context when the distinction matters. Drain timeout errors wrap
`context.DeadlineExceeded`. Actual worker, batch, and rollback failures are joined and retain their causes, including
failures previously hidden by cancellation. Error-valued panics retain their causes; other panic values appear in
diagnostics. A worker panic remains identifiable by `ErrWorkerPanic`. Handler panics remain retry/dead failures,
and persisted handler failure summaries are unchanged.

This is a runtime behavior change for existing `Run` consumers. Review shutdown budgets and dependency ownership
before upgrading. Delivery remains at least once. No schema migration is required. See the [relay guide](guide.md#relay)
and [diagnostic contract](diagnostics.md).
