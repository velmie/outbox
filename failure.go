package outbox

import "context"

const (
	// FailureRetry marks the record as retryable.
	FailureRetry FailureAction = iota
	// FailureDead marks the record as non-retryable and dead-letters it immediately.
	FailureDead
)

// FailureAction defines how a failed record should be handled.
type FailureAction int

// FailureClassifier decides whether a failure is retryable.
// It receives the original handler error or an error containing the panic value.
type FailureClassifier func(ctx context.Context, record Record, err error) FailureAction

func defaultFailureClassifier(context.Context, Record, error) FailureAction {
	return FailureRetry
}
