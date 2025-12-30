package outbox

import (
	"context"
	"time"
)

// FetchOptions controls how pending records are selected.
type FetchOptions struct {
	BatchSize    int
	MinCreatedAt time.Time
}

// Consumer provides locked batches of outbox records.
type Consumer interface {
	// Fetch returns a batch of pending records locked for processing.
	Fetch(ctx context.Context, opts FetchOptions) (Batch, error)
}

// Batch represents a locked set of records fetched for processing.
type Batch interface {
	// Records returns the fetched records in this batch.
	Records() []Record
	// Ack marks the provided records as processed.
	Ack(ctx context.Context, ids []ID) error
	// Fail records failures and updates retry state for each record.
	Fail(ctx context.Context, failures []Failure) error
	// Commit finalizes the batch transaction.
	Commit() error
	// Rollback releases locks without applying any changes.
	Rollback() error
}

// DeadBatch supports immediate dead-lettering of records.
type DeadBatch interface {
	// Dead marks the provided records as non retryable failures.
	Dead(ctx context.Context, failures []Failure) error
}

// PendingCounter provides a total count of pending records.
type PendingCounter interface {
	// PendingCount returns the current number of pending records.
	PendingCount(ctx context.Context) (int, error)
}
