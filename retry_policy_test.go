package outbox

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"
)

func TestRelayRetryHintAndErrorDetails(t *testing.T) {
	cause := errors.New("dependency failed")
	for _, details := range []bool{false, true} {
		t.Run(fmt.Sprint(details), func(t *testing.T) {
			batch := &fakeBatch{
				records: []Record{{
					ID: ID{1},
				}},
			}
			original := fmt.Errorf("delivery: %w", RetryAfter(cause, time.Second))
			var classified error
			var reported error
			opts := []RelayOption{
				WithFailureClassifier(func(_ context.Context, _ Record, err error) FailureAction {
					classified = err
					return FailureRetry
				}),
				WithErrorHandler(func(_ context.Context, _ Record, err error) {
					reported = err
				}),
			}
			if details {
				opts = append(opts, WithErrorDetails())
			}
			relay := NewRelay(
				staticConsumer{
					batch: batch,
				},
				HandlerFunc(func(context.Context, Record) error {
					return original
				}),
				opts...,
			)
			_, err := relay.ProcessOnce(context.Background())
			if err != nil || len(batch.failures) != 1 {
				t.Fatalf("process: %v, failures: %d", err, len(batch.failures))
			}
			failure := batch.failures[0]
			if failure.RetryDelay != time.Second || classified != original || reported != original {
				t.Fatal("retry hint or original callback error was lost")
			}
			want := "outbox handler failed"
			if details {
				want = original.Error()
				if !errors.Is(failure.Err, cause) {
					t.Fatal("detail lost its cause")
				}
			}
			if failure.Err.Error() != want {
				t.Fatalf("persisted error = %q, want %q", failure.Err, want)
			}
		})
	}
}

func TestRelayDetailedPanicAndTimeout(t *testing.T) {
	cause := errors.New("panic cause")
	t.Run("panic", func(t *testing.T) {
		batch := &fakeBatch{
			records: []Record{{
				ID: ID{1},
			}},
		}
		relay := NewRelay(
			staticConsumer{
				batch: batch,
			},
			HandlerFunc(func(context.Context, Record) error {
				panic(cause)
			}),
			WithErrorDetails(),
		)
		_, err := relay.ProcessOnce(context.Background())
		if err != nil || len(batch.failures) != 1 || !errors.Is(batch.failures[0].Err, cause) {
			t.Fatalf("process error %v, failures %v", err, batch.failures)
		}
		if got := batch.failures[0].Err.Error(); got != "outbox handler panicked: panic cause" {
			t.Fatalf("panic detail = %q", got)
		}
	})
	t.Run("timeout", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			batch := &fakeBatch{
				records: []Record{{
					ID: ID{1},
				}},
			}
			relay := NewRelay(
				staticConsumer{
					batch: batch,
				},
				HandlerFunc(func(ctx context.Context, _ Record) error {
					<-ctx.Done()
					return ctx.Err()
				}),
				WithErrorDetails(),
				WithHandlerTimeout(time.Second),
			)
			_, err := relay.ProcessOnce(context.Background())
			if err != nil || len(batch.failures) != 1 || batch.failures[0].Err.Error() != "context deadline exceeded" {
				t.Fatalf("process error %v, failures %v", err, batch.failures)
			}
		})
	})
}

func TestRelayDetailedTimeoutPreservesHandlerOutcome(t *testing.T) {
	cause := errors.New("receiver returned a different error")
	for _, test := range []struct {
		name string
		err  error
	}{
		{
			name: "different error",
			err:  cause,
		},
		{
			name: "success after deadline",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				batch := &fakeBatch{
					records: []Record{{
						ID: ID{1},
					}},
				}
				relay := NewRelay(
					staticConsumer{
						batch: batch,
					},
					HandlerFunc(func(ctx context.Context, _ Record) error {
						<-ctx.Done()
						return test.err
					}),
					WithErrorDetails(),
					WithHandlerTimeout(time.Second),
				)
				processed, err := relay.ProcessOnce(context.Background())
				if err != nil || !processed {
					t.Fatalf("process: processed=%t error=%v", processed, err)
				}
				if test.err == nil {
					if len(batch.ackIDs) != 1 || len(batch.failures) != 0 {
						t.Fatalf(
							"expected acknowledgement, got %d acknowledgements and %d failures",
							len(batch.ackIDs),
							len(batch.failures),
						)
					}
					return
				}
				if len(batch.failures) != 1 || !errors.Is(batch.failures[0].Err, cause) {
					t.Fatalf("failure did not retain handler error: %v", batch.failures)
				}
			})
		})
	}
}
