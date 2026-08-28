package outbox

import "time"

const (
	defaultBatchSize    = 50
	defaultPollInterval = 50 * time.Millisecond
	defaultWorkers      = 1
	defaultPendingCheck = 0
)

// RelayConfig defines how the Relay polls and processes records.
type RelayConfig struct {
	BatchSize    int
	PollInterval time.Duration
	Workers      int
	// PartitionWindow is retained for source compatibility and is ignored.
	//
	// Deprecated: all pending records are eligible for delivery.
	PartitionWindow   time.Duration
	Clock             Clock
	ErrorHandler      FailureHandler
	Logger            Logger
	Metrics           Metrics
	FailureClassifier FailureClassifier
	// HandlerTimeout is a cooperative per-record context deadline. Relay waits
	// for Handler.Handle to return and does not interrupt it.
	HandlerTimeout  time.Duration
	PendingInterval time.Duration
}

// RelayOption configures Relay behavior.
type RelayOption func(*RelayConfig)

// WithBatchSize sets the number of records processed per batch.
func WithBatchSize(size int) RelayOption {
	return func(c *RelayConfig) {
		c.BatchSize = size
	}
}

// WithPollInterval sets the delay between empty polls.
func WithPollInterval(interval time.Duration) RelayOption {
	return func(c *RelayConfig) {
		c.PollInterval = interval
	}
}

// WithWorkers sets the number of concurrent polling workers.
func WithWorkers(count int) RelayOption {
	return func(c *RelayConfig) {
		c.Workers = count
	}
}

// WithPartitionWindow is retained for source compatibility and has no effect.
//
// Deprecated: all pending records are eligible for delivery.
func WithPartitionWindow(window time.Duration) RelayOption {
	return func(c *RelayConfig) {
		c.PartitionWindow = window
	}
}

// WithClock sets the Relay clock.
func WithClock(clock Clock) RelayOption {
	return func(c *RelayConfig) {
		c.Clock = clock
	}
}

// WithErrorHandler registers a callback for handler failures.
// The callback receives the original error returned by the handler and is
// responsible for handling sensitive error and record data safely.
func WithErrorHandler(handler FailureHandler) RelayOption {
	return func(c *RelayConfig) {
		c.ErrorHandler = handler
	}
}

// WithLogger sets the relay logger.
func WithLogger(logger Logger) RelayOption {
	return func(c *RelayConfig) {
		c.Logger = logger
	}
}

// WithMetrics sets the relay metrics recorder.
func WithMetrics(metrics Metrics) RelayOption {
	return func(c *RelayConfig) {
		c.Metrics = metrics
	}
}

// WithFailureClassifier sets the failure classifier for retry/dead-letter decisions.
// The classifier receives the original error returned by the handler.
func WithFailureClassifier(classifier FailureClassifier) RelayOption {
	return func(c *RelayConfig) {
		c.FailureClassifier = classifier
	}
}

// WithHandlerTimeout sets a cooperative per-record context deadline.
// Relay calls Handler.Handle synchronously and waits for it to return.
func WithHandlerTimeout(timeout time.Duration) RelayOption {
	return func(c *RelayConfig) {
		c.HandlerTimeout = timeout
	}
}

// WithPendingInterval sets the minimum interval between pending count samples.
// Use a positive value to enable sampling or zero to keep it disabled.
// The default is disabled.
func WithPendingInterval(interval time.Duration) RelayOption {
	return func(c *RelayConfig) {
		c.PendingInterval = interval
	}
}

func (c RelayConfig) withDefaults() RelayConfig {
	if c.BatchSize <= 0 {
		c.BatchSize = defaultBatchSize
	}
	if c.PollInterval <= 0 {
		c.PollInterval = defaultPollInterval
	}
	if c.Workers <= 0 {
		c.Workers = defaultWorkers
	}
	if c.Clock == nil {
		c.Clock = SystemClock{}
	}
	if c.Logger == nil {
		c.Logger = NopLogger{}
	}
	if c.Metrics == nil {
		c.Metrics = NopMetrics{}
	}
	if c.FailureClassifier == nil {
		c.FailureClassifier = defaultFailureClassifier
	}
	if c.PendingInterval <= 0 {
		c.PendingInterval = defaultPendingCheck
	}

	return c
}
