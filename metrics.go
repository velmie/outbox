package outbox

import "time"

// Metrics captures relay-level telemetry.
type Metrics interface {
	// ObserveBatchDuration records the time to process a batch.
	ObserveBatchDuration(duration time.Duration)
	// AddProcessed increments the count of processed records.
	AddProcessed(count int)
	// AddErrors increments the count of handler errors.
	AddErrors(count int)
	// AddRetries increments the count of retries.
	AddRetries(count int)
	// AddDead increments the count of dead-lettered records.
	AddDead(count int)
	// SetPending updates the current pending record count.
	SetPending(count int)
}

// NopMetrics is a no-op metrics recorder.
type NopMetrics struct{}

// ObserveBatchDuration implements Metrics.
func (NopMetrics) ObserveBatchDuration(time.Duration) {}

// AddProcessed implements Metrics.
func (NopMetrics) AddProcessed(int) {}

// AddErrors implements Metrics.
func (NopMetrics) AddErrors(int) {}

// AddRetries implements Metrics.
func (NopMetrics) AddRetries(int) {}

// AddDead implements Metrics.
func (NopMetrics) AddDead(int) {}

// SetPending implements Metrics.
func (NopMetrics) SetPending(int) {}
