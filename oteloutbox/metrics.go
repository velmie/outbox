package oteloutbox

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics records the released relay's committed outcomes with fixed attributes.
// Retry exhaustion belongs to retries. Dead counts explicit permanent decisions.
type Metrics struct {
	processed metric.Int64Counter
	failures  metric.Int64Counter
	retries   metric.Int64Counter
	dead      metric.Int64Counter
	pending   metric.Int64Gauge
	duration  metric.Float64Histogram
	attrs     metric.MeasurementOption
}

// NewMetrics creates instruments for outbox relay outcomes with fixed attributes.
// Use bounded identifiers; identical attribute sets share a metric series.
// Batch durations are recorded in seconds with suggested explicit bucket
// boundaries from 5 ms to 10 s. The meter provider can override the aggregation.
func NewMetrics(meter metric.Meter, attrs ...attribute.KeyValue) (*Metrics, error) {
	processed, err := meter.Int64Counter(
		"outbox.processed",
		metric.WithUnit("{record}"),
		metric.WithDescription("Number of processed records."),
	)
	if err != nil {
		return nil, fmt.Errorf("create outbox processed metric: %w", err)
	}
	failures, err := meter.Int64Counter(
		"outbox.errors",
		metric.WithUnit("{record}"),
		metric.WithDescription("Number of records with handler errors."),
	)
	if err != nil {
		return nil, fmt.Errorf("create outbox error metric: %w", err)
	}
	retries, err := meter.Int64Counter(
		"outbox.retries",
		metric.WithUnit("{record}"),
		metric.WithDescription("Number of records retried."),
	)
	if err != nil {
		return nil, fmt.Errorf("create outbox retry metric: %w", err)
	}
	dead, err := meter.Int64Counter(
		"outbox.dead",
		metric.WithUnit("{record}"),
		metric.WithDescription("Number of records marked dead by a permanent decision."),
	)
	if err != nil {
		return nil, fmt.Errorf("create outbox dead metric: %w", err)
	}
	pending, err := meter.Int64Gauge(
		"outbox.pending",
		metric.WithUnit("{record}"),
		metric.WithDescription("Number of pending records."),
	)
	if err != nil {
		return nil, fmt.Errorf("create outbox pending metric: %w", err)
	}
	duration, err := meter.Float64Histogram(
		"outbox.batch.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Time spent processing a batch."),
		//nolint:mnd // Standard duration histogram boundaries in seconds.
		metric.WithExplicitBucketBoundaries(0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10),
	)
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
		attrs:     metric.WithAttributes(attrs...),
	}, nil
}

// AddProcessed records processed records.
func (m *Metrics) AddProcessed(count int) {
	m.processed.Add(context.Background(), int64(count), m.attrs)
}

// AddErrors records handler errors.
func (m *Metrics) AddErrors(count int) { m.failures.Add(context.Background(), int64(count), m.attrs) }

// AddRetries records retries.
func (m *Metrics) AddRetries(count int) { m.retries.Add(context.Background(), int64(count), m.attrs) }

// AddDead records permanent failures.
func (m *Metrics) AddDead(count int) { m.dead.Add(context.Background(), int64(count), m.attrs) }

// SetPending records the pending count.
func (m *Metrics) SetPending(count int) {
	m.pending.Record(context.Background(), int64(count), m.attrs)
}

// ObserveBatchDuration records batch processing time.
func (m *Metrics) ObserveBatchDuration(duration time.Duration) {
	m.duration.Record(context.Background(), duration.Seconds(), m.attrs)
}
