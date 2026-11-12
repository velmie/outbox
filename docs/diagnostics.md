# Structured diagnostics

Relay and MySQL maintenance use the existing `outbox.Logger` interface. Each diagnostic contains
alternating string keys and values. Use the structured fields to classify events. Message wording,
error text, and incidental SQL metadata are not classification contracts.

Logger implementations must support concurrent calls. Relay workers can emit independently, and an
application may share one logger between relay and maintenance. The default `NopLogger` discards events.

## Event vocabulary

Every event below includes `event`, `operation`, and `outcome` as plain strings. These values are stable.
Future versions may add events or fields, so consumers should ignore fields they do not understand and
handle unknown events without treating them as successful operations.

| `event` | `operation` | `outcome` | Meaning |
| --- | --- | --- | --- |
| `relay.storage_error` | `relay.fetch` or `relay.process_batch` | `retrying` | A storage operation failed and the worker will wait before fetching again. |
| `relay.worker_failed` | `relay.run` | `failed` | A worker returned an error. |
| `relay.worker_panicked` | `relay.run` | `failed` | A worker panic was recovered; `err` includes the panic value. |
| `relay.pending_count_failed` | `relay.pending_count` | `failed` | Pending sampling failed. Delivery can continue. |
| `relay.dead_letter_unsupported` | `relay.dead_letter` | `unsupported` | The batch has no dead-letter support. A retry fallback will be attempted. |
| `cleanup.skipped` | `cleanup.ensure` | `skipped` | Another session holds the advisory lock. No cleanup pass ran. |
| `cleanup.completed` | `cleanup.ensure` | `succeeded` | The cleanup pass and advisory lock release succeeded. |
| `cleanup.failed` | `cleanup.ensure` | `failed` | The cleanup pass or advisory lock release failed. |
| `partitions.skipped` | `partitions.ensure` | `skipped` | Another session holds the advisory lock. No partition pass ran. |
| `partitions.completed` | `partitions.ensure` | `succeeded` | The partition pass and advisory lock release succeeded. |
| `partitions.failed` | `partitions.ensure` | `failed` | The partition pass or advisory lock release failed. |
| `partitions.expansion_started` | `partitions.reorganize` | `started` | Expansion DDL is about to execute. |
| `partitions.expansion_completed` | `partitions.reorganize` | `succeeded` | Expansion DDL succeeded. |
| `partitions.dropped` | `partitions.drop` | `succeeded` | Partition removal DDL succeeded. |

Relay errors and panic use Error, pending and unsupported-capability events use Warn. Maintenance
failures use Warn, skipped and completed passes use Debug, and the DDL events use Info. A sink that
filters Debug will not show successful or skipped passes. Collect before filtering if all outcomes
are needed for metrics or programmatic classification.

The `unsupported` event does not confirm that retry state was written or committed. Existing retry
and dead-letter behavior is unchanged.

## Passes and suboperations

Each normally returning `Ensure` call emits exactly one terminal pass event: skipped, completed, or
failed. `Run` calls `Ensure` and does not emit a duplicate failure. Both methods keep their existing
signatures and cancellation behavior.

A busy lock still returns nil error, and cleanup still returns a zero result for that skip. Use the
`skipped` event to distinguish it from a successful pass that had nothing to change. Completion is
emitted only after checking the advisory lock release.

DDL events describe individual operations. Expansion start is not proof of expansion completion.
A confirmed expansion or DROP remains a useful fact even if later work or lock release fails. For
example, `partitions.dropped` may be followed by `partitions.failed`. Treat only the terminal pass event
as the overall result. Do not infer overall success from the last DDL event.

## Additional fields and error causes

- `worker` is an `int` on relay storage errors, worker failures, and panics.
- `retry_in` is a `time.Duration` on `relay.storage_error`, containing the proposed wait. Cancellation can
  interrupt it. `err` identifies the failed fetch, settlement, commit, or rollback and retains joined causes.
- `count` is an `int` on the unsupported dead-letter event.
- `reason` is the fixed string `lock_busy` on skipped maintenance passes.
- `stage` is a fixed string on failed maintenance passes. Cleanup stages are `connect`, `acquire_lock`,
  `cleanup`, `release_lock`, and `operation_and_release`. Partition stages additionally use
  `resolve_schema`, `inspect`, `plan`, `reorganize`, and `drop` instead of `cleanup`.
- `operation_and_release` means both the operation and advisory lock release failed. The joined error
  retains both causes.
- `err` is the original error, possibly wrapped or joined, on error events. Its text identifies the failure and
  includes the dependency cause. Error-valued panics remain available through `errors.Is` and `errors.As`.

Handler callbacks and classifiers receive the original error or a recovered panic error. Pending-count failures
are available through the logger's `err` value, even though `ProcessOnce` does not return those sampling errors.
Use the standard [Logger example](../logger_example_test.go): `*slog.Logger` implements `Logger` directly.

A retryable positive delay hint on a store without retry scheduling causes a settlement error identifying
the record. The relay rolls back the batch. `Run` reports the worker failure and stops admission;
`ProcessOnce` returns the error directly. No committed-outcome counters are incremented for the rollback.
`WithErrorDetails` opts into persisting handler error text and panic values. Enable it only when these
values are safe to store. Callback and classifier errors remain original and untruncated.

Storage recovery logs one event per failed polling iteration, including any rollback failure in the same
`err` chain. It does not emit `relay.worker_failed` unless the worker actually stops with an error.
Recovery is opt-in through `WithStorageErrorBackoff`; no new logger or metrics methods are required.
