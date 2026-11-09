package oteloutbox

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/metric"
)

// Metrics records the released relay's committed outcomes without event labels.
// Retry exhaustion belongs to retries. Dead counts explicit permanent decisions.
type Metrics struct {
	processed metric.Int64Counter
	failures  metric.Int64Counter
	retries   metric.Int64Counter
	dead      metric.Int64Counter
	pending   metric.Int64Gauge
	duration  metric.Float64Histogram
}

// NewMetrics creates instruments for outbox relay outcomes.
func NewMetrics(meter metric.Meter) (*Metrics, error) {
	processed, err := meter.Int64Counter("outbox.processed")
	if err != nil {
		return nil, fmt.Errorf("create outbox processed metric: %w", err)
	}
	failures, err := meter.Int64Counter("outbox.errors")
	if err != nil {
		return nil, fmt.Errorf("create outbox error metric: %w", err)
	}
	retries, err := meter.Int64Counter("outbox.retries")
	if err != nil {
		return nil, fmt.Errorf("create outbox retry metric: %w", err)
	}
	dead, err := meter.Int64Counter("outbox.dead")
	if err != nil {
		return nil, fmt.Errorf("create outbox dead metric: %w", err)
	}
	pending, err := meter.Int64Gauge("outbox.pending")
	if err != nil {
		return nil, fmt.Errorf("create outbox pending metric: %w", err)
	}
	duration, err := meter.Float64Histogram("outbox.batch.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, fmt.Errorf("create outbox batch duration metric: %w", err)
	}

	return &Metrics{
		processed: processed,
		failures:  failures,
		retries:   retries,
		dead:      dead,
		pending:   pending,
		duration:  duration,
	}, nil
}

// AddProcessed records processed records.
func (m *Metrics) AddProcessed(count int) { m.processed.Add(context.Background(), int64(count)) }

// AddErrors records handler errors.
func (m *Metrics) AddErrors(count int) { m.failures.Add(context.Background(), int64(count)) }

// AddRetries records retries.
func (m *Metrics) AddRetries(count int) { m.retries.Add(context.Background(), int64(count)) }

// AddDead records permanent failures.
func (m *Metrics) AddDead(count int) { m.dead.Add(context.Background(), int64(count)) }

// SetPending records the pending count.
func (m *Metrics) SetPending(count int) { m.pending.Record(context.Background(), int64(count)) }

// ObserveBatchDuration records batch processing time.
func (m *Metrics) ObserveBatchDuration(duration time.Duration) {
	m.duration.Record(context.Background(), duration.Seconds())
}
