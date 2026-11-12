package mysql

import (
	"time"

	"github.com/velmie/outbox"
)

const (
	defaultTable       = "outbox"
	defaultMaxAttempts = 5
)

// Config defines MySQL store behavior.
type Config struct {
	Table         string
	AggregateType string
	MaxAttempts   int
	// RetryScheduling declares that the table has next_attempt_at.
	RetryScheduling    bool
	RetryDelay         time.Duration
	RetrySchedule      func(attempt int) time.Duration
	Clock              outbox.Clock
	Generator          outbox.IDGenerator
	ValidateJSON       bool
	validateJSONSet    bool
	ValidatePayload    bool
	validatePayloadSet bool
	ValidateHeaders    bool
	validateHeadersSet bool
}

// WithAggregateType restricts store operations to one aggregate type.
// Empty preserves the existing unscoped behavior.
// Enqueue requires an exact match; database comparisons follow column collation.
func WithAggregateType(value string) Option {
	return func(c *Config) {
		c.AggregateType = value
	}
}

// Option configures the MySQL store.
type Option func(*Config)

// WithRetryScheduling declares that the table uses durable retry scheduling.
// The table must have next_attempt_at, as provided by RetrySchema. Fetch honors
// deadlines, Requeue clears them, and handler retry hints work without a base delay.
// This option preserves any configured RetryDelay or RetrySchedule and executes no DDL.
func WithRetryScheduling() Option {
	return func(c *Config) {
		c.RetryScheduling = true
	}
}

// WithRetrySchedule enables durable scheduling through next_attempt_at.
// attempt is the just-failed attempt, starting at 1. The schedule must be
// nonnil, concurrency-safe and return a nonnegative delay.
func WithRetrySchedule(schedule func(attempt int) time.Duration) Option {
	return func(c *Config) {
		c.RetrySchedule = schedule
	}
}

// WithTable sets the outbox table name.
func WithTable(name string) Option {
	return func(c *Config) {
		c.Table = name
	}
}

// WithMaxAttempts sets the retry limit before marking a record as dead.
func WithMaxAttempts(attempts int) Option {
	return func(c *Config) {
		c.MaxAttempts = attempts
	}
}

// WithRetryDelay enables durable retry scheduling with a fixed delay.
// The table must include the next_attempt_at column provided by RetrySchema.
// Every consumer of a scheduled table must enable scheduling. Zero disables this delay;
// WithRetryScheduling or WithRetrySchedule can still enable scheduling.
// Negative values are invalid. Positive values round up to microsecond precision.
// Deadlines use database UTC time at the failure update, independently of WithClock.
func WithRetryDelay(delay time.Duration) Option {
	return func(c *Config) {
		c.RetryDelay = delay
	}
}

// WithClock sets the time source used for IDs and acknowledgement timestamps.
// Durable retry deadlines and eligibility always use the database clock.
func WithClock(clock outbox.Clock) Option {
	return func(c *Config) {
		c.Clock = clock
	}
}

// WithGenerator sets a generator that must return RFC 9562 UUIDv7 identifiers.
func WithGenerator(gen outbox.IDGenerator) Option {
	return func(c *Config) {
		c.Generator = gen
	}
}

// WithValidateJSON enables or disables JSON validation for payload and headers.
func WithValidateJSON(enabled bool) Option {
	return func(c *Config) {
		c.ValidateJSON = enabled
		c.validateJSONSet = true
		c.ValidatePayload = enabled
		c.validatePayloadSet = true
		c.ValidateHeaders = enabled
		c.validateHeadersSet = true
	}
}

// WithValidatePayload enables or disables JSON validation on payload.
func WithValidatePayload(enabled bool) Option {
	return func(c *Config) {
		c.ValidatePayload = enabled
		c.validatePayloadSet = true
	}
}

// WithValidateHeaders enables or disables JSON validation on headers.
func WithValidateHeaders(enabled bool) Option {
	return func(c *Config) {
		c.ValidateHeaders = enabled
		c.validateHeadersSet = true
	}
}

func (c Config) retryScheduling() bool {
	return c.RetryScheduling || c.RetryDelay > 0 || c.RetrySchedule != nil
}

func (c Config) withDefaults() Config {
	if c.Table == "" {
		c.Table = defaultTable
	}
	if c.MaxAttempts <= 0 {
		c.MaxAttempts = defaultMaxAttempts
	}
	if c.Clock == nil {
		c.Clock = outbox.SystemClock{}
	}
	if c.Generator == nil {
		c.Generator = outbox.NewUUIDv7Generator(c.Clock)
	}
	if !c.validateJSONSet {
		c.ValidateJSON = true
	}
	if !c.validatePayloadSet {
		c.ValidatePayload = c.ValidateJSON
	}
	if !c.validateHeadersSet {
		c.ValidateHeaders = c.ValidateJSON
	}
	if !c.validateJSONSet {
		c.ValidateJSON = c.ValidatePayload && c.ValidateHeaders
	}

	return c
}
