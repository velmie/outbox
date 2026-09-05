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
	Table              string
	MaxAttempts        int
	RetryDelay         time.Duration
	Clock              outbox.Clock
	Generator          outbox.IDGenerator
	ValidateJSON       bool
	validateJSONSet    bool
	ValidatePayload    bool
	validatePayloadSet bool
	ValidateHeaders    bool
	validateHeadersSet bool
}

// Option configures the MySQL store.
type Option func(*Config)

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
// Every consumer of a scheduled table must enable scheduling. Zero disables it;
// negative values are invalid. Positive values round up to microsecond precision.
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
