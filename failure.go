package outbox

import (
	"context"
	"time"
)

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

// RetryAfter adds a minimum retry delay to a nonnil error. The returned error
// preserves the original text and supports errors.Is and errors.As.
func RetryAfter(err error, delay time.Duration) error {
	return &retryAfterError{
		err:   err,
		delay: delay,
	}
}

type retryAfterError struct {
	err   error
	delay time.Duration
}

func (e *retryAfterError) Error() string { return e.err.Error() }

func (e *retryAfterError) Unwrap() error { return e.err }

func defaultFailureClassifier(context.Context, Record, error) FailureAction {
	return FailureRetry
}
