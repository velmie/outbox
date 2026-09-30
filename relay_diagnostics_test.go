package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRelayDiagnosticsClassifiedByFields(t *testing.T) {
	source := &diagnosticError{}
	wrapped := fmt.Errorf("dependency: %w", source)
	tests := []struct {
		name      string
		event     string
		operation string
		outcome   string
		run       func(Logger) error
		wantErr   error
		logErr    error
	}{
		{
			name:  "worker failure",
			event: "relay.worker_failed", operation: "relay.run", outcome: "failed",
			wantErr: source, logErr: source,
			run: func(logger Logger) error {
				return NewRelay(staticConsumer{err: wrapped}, HandlerFunc(func(context.Context, Record) error {
					return nil
				}), WithLogger(logger)).Run(context.Background())
			},
		},
		{
			name:  "worker panic",
			event: "relay.worker_panicked", operation: "relay.run", outcome: "failed",
			wantErr: ErrWorkerPanic,
			run: func(logger Logger) error {
				batch := &fakeBatch{records: []Record{{ID: ID{1}}}}
				return NewRelay(staticConsumer{batch: batch}, HandlerFunc(func(context.Context, Record) error {
					return wrapped
				}), WithFailureClassifier(func(context.Context, Record, error) FailureAction {
					panic("control-panic-secret")
				}), WithLogger(logger)).Run(context.Background())
			},
		},
		{
			name:  "pending count failure",
			event: "relay.pending_count_failed", operation: "relay.pending_count", outcome: "failed",
			logErr: source,
			run: func(logger Logger) error {
				relay := NewRelay(failedPendingCounter{err: wrapped}, HandlerFunc(func(context.Context, Record) error {
					return nil
				}), WithPendingInterval(time.Second), WithLogger(logger))
				_, err := relay.ProcessOnce(context.Background())
				return err
			},
		},
		{
			name:  "dead-letter capability missing",
			event: "relay.dead_letter_unsupported", operation: "relay.dead_letter", outcome: "unsupported",
			run: func(logger Logger) error {
				batch := &fakeBatchNoDead{records: []Record{{ID: ID{1}}}}
				relay := NewRelay(staticConsumer{batch: batch}, HandlerFunc(func(context.Context, Record) error {
					return wrapped
				}), WithFailureClassifier(func(context.Context, Record, error) FailureAction {
					return FailureDead
				}), WithLogger(logger))
				_, err := relay.ProcessOnce(context.Background())
				return err
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := &fieldLogger{}
			err := tt.run(logger)
			if !errors.Is(err, tt.wantErr) {
				t.Fatal("returned error lost its original cause or changed behavior")
			}
			entries := logger.snapshot()
			if len(entries) != 1 {
				t.Fatalf("got %d events, want one", len(entries))
			}
			entry := entries[0]
			if entry["event"] != tt.event || entry["operation"] != tt.operation || entry["outcome"] != tt.outcome {
				t.Errorf("classification = (%v, %v, %v), want (%s, %s, %s)",
					entry["event"], entry["operation"], entry["outcome"], tt.event, tt.operation, tt.outcome)
			}
			loggedErr, _ := entry["err"].(error)
			if !errors.Is(loggedErr, tt.logErr) {
				t.Fatal("logger lost the original cause")
			}
			if tt.logErr != nil {
				var cause *diagnosticError
				if !errors.As(loggedErr, &cause) || cause != source {
					t.Fatal("logger lost the original typed cause")
				}
			}
		})
	}
}

// fieldLogger deliberately discards messages. Classification uses only fields.
type fieldLogger struct {
	mu      sync.Mutex
	entries []map[string]any
}

func (l *fieldLogger) Debug(_ string, args ...any) { l.capture(args) }
func (l *fieldLogger) Info(_ string, args ...any)  { l.capture(args) }
func (l *fieldLogger) Warn(_ string, args ...any)  { l.capture(args) }
func (l *fieldLogger) Error(_ string, args ...any) { l.capture(args) }

func (l *fieldLogger) capture(args []any) {
	entry := make(map[string]any)
	for i := 0; i+1 < len(args); i += 2 {
		if key, ok := args[i].(string); ok {
			entry[key] = args[i+1]
		}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.entries = append(l.entries, entry)
}

func (l *fieldLogger) snapshot() []map[string]any {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]map[string]any(nil), l.entries...)
}

type failedPendingCounter struct{ err error }

func (failedPendingCounter) Fetch(context.Context, FetchOptions) (Batch, error) {
	return nil, ErrNoRecords
}

func (c failedPendingCounter) PendingCount(context.Context) (int, error) { return 0, c.err }

type diagnosticError struct{}

func (*diagnosticError) Error() string { return "control-error-secret" }
