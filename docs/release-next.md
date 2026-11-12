# Next release (unreleased)

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
