# outbox v0.7.0

v0.7.0 is a coordinated release of the root, MySQL, and command modules. Every addition is opt-in: existing
relays, stores, and cleanup maintainers keep their behavior. Delivery remains at least once.

- `WithStorageErrorBackoff(minimum, maximum)` lets `Run` workers recover independently from storage errors,
  with bounded exponential waits and diagnostics through the existing logger. Contract errors remain fatal.
  MySQL retry-policy failures now match `outbox.ErrInvalidRetryPolicy` while retaining their original causes
  and record IDs. Enable recovery with an adapter that provides this classification.
- `WithIdleBackoff(maximum)` reduces empty polling with a per-worker wait that grows from `PollInterval`,
  resets on records, and bounds the added polling latency after an idle period.

Both options are disabled by default. `ProcessOnce`, logger and metrics interfaces, shutdown behavior, and
at-least-once delivery are unchanged. A failed commit can lead to redelivery with the same record IDs.
No schema migration is required. Use keyed literals when constructing `RelayConfig` directly, because it
has new fields. See the [relay guide](guide.md#storage-recovery-and-idle-backoff).

- `mysql.WithRetryScheduling()` declares a scheduled table without a delay or schedule function. Fetch honors
  deadlines, handler retry hints work without a base delay, and `Requeue` clears deadlines. The existing
  `outbox-requeue -retry-scheduling` flag uses this option with unchanged behavior. The table must already
  contain `next_attempt_at`; existing delay and schedule options retain their behavior.
- `mysql.CleanupMaintainerConfig.DrainTimeout` opts into repeated bounded cleanup batches within one pass
  budget. One connection holds the advisory lock across the pass, and results total confirmed deletions,
  including partial results on error. Zero retains one batch per pass. The first batch always runs after
  taking the lock. Each full batch repeats only while budget remains. The budget never cancels a running
  batch, and exhaustion returns totals with nil error. Caller cancellation retains its existing behavior.

Use keyed literals for `mysql.Config` and `mysql.CleanupMaintainerConfig`, which have new fields.
