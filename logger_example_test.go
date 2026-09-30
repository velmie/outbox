package outbox_test

import (
	"context"
	"log/slog"
	"os"

	"github.com/velmie/outbox"
)

func ExampleLogger() {
	logger := safeLogger{slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}))}
	logger.Warn("message wording is not a contract", "event", "cleanup.skipped",
		"operation", "cleanup.ensure", "outcome", "skipped", "reason", "lock_busy")
	// Output:
	// level=WARN msg="outbox diagnostic" event=cleanup.skipped operation=cleanup.ensure outcome=skipped reason=lock_busy
}

// safeLogger projects only recognized diagnostic fields. Unknown future events
// are omitted until this application's allowlist is updated. It never formats
// the source message, errors, or arbitrary arguments. Consumers needing original
// causes must inspect them before this projection, without logging their text.
// slog serializes writes through its handler, so one adapter can serve workers.
type safeLogger struct{ logger *slog.Logger }

var _ outbox.Logger = safeLogger{}

func (l safeLogger) Debug(_ string, args ...any) { l.log(slog.LevelDebug, args) }
func (l safeLogger) Info(_ string, args ...any)  { l.log(slog.LevelInfo, args) }
func (l safeLogger) Warn(_ string, args ...any)  { l.log(slog.LevelWarn, args) }
func (l safeLogger) Error(_ string, args ...any) { l.log(slog.LevelError, args) }

func (l safeLogger) log(level slog.Level, args []any) {
	fields := make(map[string]string)
	for i := 0; i+1 < len(args); i += 2 {
		key, keyOK := args[i].(string)
		value, valueOK := args[i+1].(string)
		if keyOK && valueOK {
			switch key {
			case "event", "operation", "outcome", "reason", "stage":
				fields[key] = value
			}
		}
	}
	// Validate the tuple, not just independent strings, to preserve its meaning.
	switch fields["event"] + "/" + fields["operation"] + "/" + fields["outcome"] {
	case "relay.worker_failed/relay.run/failed",
		"relay.worker_panicked/relay.run/failed",
		"relay.pending_count_failed/relay.pending_count/failed",
		"relay.dead_letter_unsupported/relay.dead_letter/unsupported",
		"cleanup.skipped/cleanup.ensure/skipped",
		"cleanup.completed/cleanup.ensure/succeeded",
		"cleanup.failed/cleanup.ensure/failed",
		"partitions.skipped/partitions.ensure/skipped",
		"partitions.completed/partitions.ensure/succeeded",
		"partitions.failed/partitions.ensure/failed",
		"partitions.expansion_started/partitions.reorganize/started",
		"partitions.expansion_completed/partitions.reorganize/succeeded",
		"partitions.dropped/partitions.drop/succeeded":
	default:
		return
	}
	attrs := []slog.Attr{
		slog.String("event", fields["event"]),
		slog.String("operation", fields["operation"]),
		slog.String("outcome", fields["outcome"]),
	}
	if fields["reason"] == "lock_busy" && fields["outcome"] == "skipped" {
		attrs = append(attrs, slog.String("reason", "lock_busy"))
	}
	switch fields["stage"] {
	case "connect", "acquire_lock", "cleanup", "release_lock", "operation_and_release",
		"resolve_schema", "inspect", "plan", "reorganize", "drop":
		attrs = append(attrs, slog.String("stage", fields["stage"]))
	}
	l.logger.LogAttrs(context.Background(), level, "outbox diagnostic", attrs...)
}
