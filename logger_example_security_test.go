package outbox_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/velmie/outbox"
)

func TestSafeLoggerRelayDiagnostics(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending=%t", pending), func(t *testing.T) {
			var output bytes.Buffer
			cause := &sensitiveDiagnosticError{}
			original := fmt.Errorf("SECRET wrapping: %w", cause)
			logger := &causeInspectingLogger{safeLogger: safeLogger{slog.New(slog.NewJSONHandler(&output, nil))}}
			consumer := diagnosticConsumer{fetchErr: original, pendingErr: original}
			event := "relay.worker_failed"
			if pending {
				consumer.fetchErr = outbox.ErrNoRecords
				event = "relay.pending_count_failed"
			}
			relay := outbox.NewRelay(consumer, outbox.HandlerFunc(func(context.Context, outbox.Record) error { return nil }),
				outbox.WithLogger(logger), outbox.WithPendingInterval(time.Second))
			if pending {
				if _, err := relay.ProcessOnce(context.Background()); err != nil {
					t.Fatal(err)
				}
			} else if err := relay.Run(context.Background()); !errors.Is(err, cause) {
				t.Fatalf("Run lost original cause: %T", err)
			}
			var typed *sensitiveDiagnosticError
			if !errors.Is(logger.cause, cause) || !errors.As(logger.cause, &typed) {
				t.Fatal("diagnostic boundary lost original cause")
			}
			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatalf("missing diagnostic: %v", err)
			}
			if record["event"] != event || record["outcome"] != "failed" {
				t.Fatalf("unexpected fields: %v", record)
			}
			if strings.Contains(output.String(), "SECRET") {
				t.Fatal("sensitive error leaked")
			}
		})
	}
}

func TestSafeLoggerRejectsUntrustedValues(t *testing.T) {
	var output bytes.Buffer
	logger := safeLogger{slog.New(slog.NewJSONHandler(&output, nil))}
	fields := []any{"event", "cleanup.skipped", "operation", "cleanup.ensure", "outcome", "skipped", "reason", "lock_busy"}
	for _, message := range []string{"first wording", "SECRET\nforged log"} {
		logger.Warn(message, append(append([]any{}, fields...), "err", &sensitiveDiagnosticError{}, "object", explosiveFormatter{}, "stage", "SECRET", "worker", explosiveFormatter{}, "sql", "SECRET", explosiveFormatter{}, explosiveFormatter{}, "dangling")...)
	}
	lines := strings.Split(strings.TrimSpace(output.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("records = %d", len(lines))
	}
	for _, line := range lines {
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		if record["event"] != "cleanup.skipped" || record["operation"] != "cleanup.ensure" || record["outcome"] != "skipped" || record["reason"] != "lock_busy" {
			t.Fatalf("classification: %v", record)
		}
		if strings.Contains(line, "SECRET") {
			t.Fatal("sensitive argument leaked")
		}
	}
	before := output.Len()
	logger.Error("SECRET", "event", "SECRET", "operation", "cleanup.ensure", "outcome", "failed")
	logger.Error("SECRET", "event", "cleanup.skipped", "operation", "relay.run", "outcome", "failed")
	logger.Error("SECRET", "event", explosiveFormatter{})
	if output.Len() != before {
		t.Fatal("unknown or inconsistent diagnostic emitted")
	}
}

func TestSafeLoggerMaintenanceClassification(t *testing.T) {
	for _, diagnostic := range []struct{ event, operation, outcome, stage string }{
		{"cleanup.skipped", "cleanup.ensure", "skipped", ""},
		{"cleanup.completed", "cleanup.ensure", "succeeded", ""},
		{"cleanup.failed", "cleanup.ensure", "failed", "cleanup"},
		{"partitions.skipped", "partitions.ensure", "skipped", ""},
		{"partitions.completed", "partitions.ensure", "succeeded", ""},
		{"partitions.failed", "partitions.ensure", "failed", "release_lock"},
		{"partitions.expansion_started", "partitions.reorganize", "started", ""},
		{"partitions.expansion_completed", "partitions.reorganize", "succeeded", ""},
		{"partitions.dropped", "partitions.drop", "succeeded", ""},
		{"relay.worker_panicked", "relay.run", "failed", ""},
		{"relay.dead_letter_unsupported", "relay.dead_letter", "unsupported", ""},
	} {
		t.Run(diagnostic.event, func(t *testing.T) {
			var output bytes.Buffer
			logger := safeLogger{slog.New(slog.NewJSONHandler(&output, nil))}
			logger.Info("arbitrary message", "event", diagnostic.event,
				"operation", diagnostic.operation, "outcome", diagnostic.outcome, "stage", diagnostic.stage)
			var record map[string]any
			if err := json.Unmarshal(output.Bytes(), &record); err != nil {
				t.Fatal(err)
			}
			if record["event"] != diagnostic.event || record["operation"] != diagnostic.operation || record["outcome"] != diagnostic.outcome {
				t.Fatalf("diagnostic classification lost: %v", record)
			}
			if diagnostic.stage != "" && record["stage"] != diagnostic.stage {
				t.Fatal("stage lost")
			}
		})
	}
}

func TestSafeLoggerConcurrent(t *testing.T) {
	var output bytes.Buffer
	logger := safeLogger{slog.New(slog.NewJSONHandler(&output, nil))}
	var workers sync.WaitGroup
	for range 20 {
		workers.Go(func() {
			logger.Error("ignored", "event", "relay.worker_failed", "operation", "relay.run", "outcome", "failed")
		})
	}
	workers.Wait()
	if got := bytes.Count(output.Bytes(), []byte("\n")); got != 20 {
		t.Fatalf("records = %d", got)
	}
}

type sensitiveDiagnosticError struct{}

func (*sensitiveDiagnosticError) Error() string { return "SECRET raw error" }

type explosiveFormatter struct{}

func (explosiveFormatter) String() string { panic("untrusted Stringer invoked") }
func (explosiveFormatter) Error() string  { panic("untrusted Error invoked") }

type diagnosticConsumer struct{ fetchErr, pendingErr error }

func (c diagnosticConsumer) Fetch(context.Context, outbox.FetchOptions) (outbox.Batch, error) {
	return nil, c.fetchErr
}
func (c diagnosticConsumer) PendingCount(context.Context) (int, error) { return 0, c.pendingErr }

type causeInspectingLogger struct {
	safeLogger
	cause error
}

func (l *causeInspectingLogger) Warn(msg string, args ...any) {
	l.inspect(args)
	l.safeLogger.Warn(msg, args...)
}
func (l *causeInspectingLogger) Error(msg string, args ...any) {
	l.inspect(args)
	l.safeLogger.Error(msg, args...)
}
func (l *causeInspectingLogger) inspect(args []any) {
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == "err" {
			l.cause, _ = args[i+1].(error)
		}
	}
}
