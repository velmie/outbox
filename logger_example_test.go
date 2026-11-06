package outbox_test

import (
	"log/slog"
	"os"

	"github.com/velmie/outbox"
)

func ExampleLogger() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
			if attr.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return attr
		},
	}))
	var _ outbox.Logger = logger
	logger.Warn("cleanup skipped", "event", "cleanup.skipped",
		"operation", "cleanup.ensure", "outcome", "skipped", "reason", "lock_busy")
	// Output:
	// level=WARN msg="cleanup skipped" event=cleanup.skipped operation=cleanup.ensure outcome=skipped reason=lock_busy
}
