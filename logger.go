package outbox

// Logger provides structured logging hooks. Implementations must support
// concurrent calls. Arguments are alternating string keys and values.
//
// Relay and maintenance diagnostics include stable string fields named event,
// operation, and outcome. Classify these fields rather than the message or error
// text. Maintenance failures also include a fixed stage, and lock contention
// includes reason=lock_busy. See docs/diagnostics.md for the event vocabulary.
//
// The err field retains the original error for programmatic inspection. Errors
// and other context may contain sensitive data. Adapters must select and validate
// safe fields before formatting or exporting them. Panic values are not supplied.
type Logger interface {
	// Debug logs a debug message.
	Debug(msg string, args ...any)
	// Info logs an informational message.
	Info(msg string, args ...any)
	// Warn logs a warning message.
	Warn(msg string, args ...any)
	// Error logs an error message.
	Error(msg string, args ...any)
}

// NopLogger is a no-op logger.
type NopLogger struct{}

// Debug implements Logger.
func (NopLogger) Debug(string, ...any) {}

// Info implements Logger.
func (NopLogger) Info(string, ...any) {}

// Warn implements Logger.
func (NopLogger) Warn(string, ...any) {}

// Error implements Logger.
func (NopLogger) Error(string, ...any) {}
