# outbox v0.6.0

v0.6.0 is a coordinated release of the root, MySQL, and command modules. Existing unscoped stores,
immediate retries, and generic relay error summaries remain the defaults. Delivery remains at least once.

- `SchemaWithOptions` combines binary payloads, retry deadlines, aggregate indexes, deduplication, and
  partitioning where their constraints allow it. Deduplication and partitioning cannot be combined.
- `WithAggregateType` isolates store operations for one aggregate type.
- `Entry.DedupKey` supports optional table-wide, byte-exact admission deduplication. `ErrDuplicate`
  identifies a unique-key conflict and preserves the MySQL cause without asserting payload equality.
- `WithRetrySchedule` and `RetryAfter` support durable attempt-based delays and minimum handler hints.
  A retryable positive hint without scheduling rolls back and stops the affected relay worker.
- `WithErrorDetails` opts into persisting handler error text and panic values. The default remains generic.
- `Store.Requeue` and `outbox-requeue` return a bounded selection of dead records to pending while
  retaining identity and the deduplication key.

The new `Entry` and `Failure` fields require keyed struct literals in consumer code. Existing tables need
explicit migrations before enabling retry scheduling or deduplication, and pollers must be coordinated
before activating deadline or aggregate isolation. See the [migration guide](migration-v0.6.0.md).
