# outbox v0.4.0

v0.4.0 adds stable diagnostic fields for relay and MySQL maintenance, and preserves confirmed cleanup
counts when a later deletion fails. It is a coordinated release of:

- `github.com/velmie/outbox` at `v0.4.0`;
- `github.com/velmie/outbox/mysql` at `mysql/v0.4.0`;
- `github.com/velmie/outbox/cmd` at `cmd/v0.4.0`.

Keep modules and command installations aligned at v0.4.0. The root library requires Go 1.25 or later.
The MySQL and command modules require Go 1.26 or later. MySQL 8.0 remains the database minimum.

## Structured diagnostics

Classify events through the existing `Logger` using the stable string fields `event`, `operation`,
and `outcome`. Human-readable messages and error text are not classification contracts. Relay events
cover worker failures and panic, pending-count failures, and missing dead-letter support.

Cleanup and partition `Ensure` calls now emit one terminal diagnostic per normally returning pass.
An advisory lock held by another session produces `skipped`, while a completed pass produces
`succeeded`, including when no changes were needed. A failed operation or advisory lock release
produces `failed` with a fixed `stage`. `Run` uses the same diagnostics without duplicate warnings.

Partition expansion has separate start and confirmed-completion events. Confirmed partition removal
is a suboperation fact. Either may be followed by a failed overall pass if later work or lock release
fails. Collect Debug events when skipped and completed passes are needed for classification.

Original errors remain available through public error chains and the logger's `err` field. Adapters
must select and validate safe output fields before formatting. The executable safe adapter example
preserves the classification contract while omitting raw errors and arbitrary values. Panic values
remain discarded. See the [diagnostic contract](diagnostics.md) for exact codes, types, and examples.

## Partial cleanup results

`Store.Cleanup` now returns confirmed `Processed` counts together with an error if the subsequent
dead-row DELETE or its `RowsAffected` fails. Previously that error discarded the processed count.
`CleanupMaintainer.Ensure` preserves this partial result as well.

Processed rows are deleted first, and the optional dead DELETE uses the remaining shared limit.
The DELETEs remain separate autocommitted operations. A later failure does not roll back the earlier
deletion. A count is confirmed only after the DELETE and `RowsAffected` both succeed. On an error
path, zero does not prove that the statement deleted nothing.

Inspect the result even when an error is returned. The original cause remains available through
`errors.Is` and `errors.As`. See the [cleanup guide](guide.md#non-partitioned-cleanup-batch-delete).

## Compatibility

The `Logger`, `Ensure`, and `Run` signatures are unchanged. No exported production identifier is
removed or renamed. Existing logger implementations can continue accepting structured arguments,
but consumers classifying message strings should adopt the documented fields.

No schema migration is required. Retention, retry scheduling, dead-letter behavior, delivery
guarantees, and lifecycle behavior are unchanged. This release introduces no runtime dependency or
observer interface. The command module uses the updated library and adapter while retaining its
existing flags and exit behavior.
