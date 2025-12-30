package outbox

// Logger provides structured logging hooks.
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
