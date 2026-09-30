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
| `relay.worker_failed` | `relay.run` | `failed` | A worker returned an error. |
| `relay.worker_panicked` | `relay.run` | `failed` | A worker panic was recovered. The panic value is discarded. |
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

- `worker` is an `int` on relay worker failures and panics.
- `count` is an `int` on the unsupported dead-letter event.
- `reason` is the fixed string `lock_busy` on skipped maintenance passes.
- `stage` is a fixed string on failed maintenance passes. Cleanup stages are `connect`, `acquire_lock`,
  `cleanup`, `release_lock`, and `operation_and_release`. Partition stages additionally use
  `resolve_schema`, `inspect`, `plan`, `reorganize`, and `drop` instead of `cleanup`.
- `operation_and_release` means both the operation and advisory lock release failed. The joined error
  retains both causes.
- `err` is the original error, possibly wrapped or joined, on error events. Inspect it with
  `errors.Is` or `errors.As` where appropriate. It is not safe to print automatically.

Returned errors retain their existing causes. Handler callbacks also keep receiving the original
handler error, while recovered panic values remain discarded. Pending-count errors are available
through the logger's `err` value, even though `ProcessOnce` does not return those sampling errors.
An adapter may inspect an error before producing a sanitized output record without replacing the
error supplied to other consumers or returned by the public API.

## Safe output adapter

The executable [Logger example](../logger_example_test.go) demonstrates an application-owned adapter
using the standard library. It preserves recognized event fields, validates
the finite code vocabulary, and does not forward the original message, raw errors, arbitrary values,
table names, or partition names. It also omits incidental numeric context such as worker IDs and counts.
Unknown events are omitted by this example. Extend its allowed
vocabulary deliberately when adopting new events.

Apply selection before formatting. Forwarding `args` to a general logger and then removing the `err`
key can already expose a sensitive value through another field, a `String` method, or an `Error`
method. Validate values as well as keys. The library preserves original causes for consumers, so a
logger that prints all supplied fields is responsible for its own confidentiality policy.
