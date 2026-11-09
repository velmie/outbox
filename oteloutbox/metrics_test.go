package oteloutbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/velmie/outbox"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestMetricsRecordWithoutAttributes(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	m, err := NewMetrics(provider.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	var recorder outbox.Metrics = m
	recorder.AddProcessed(2)
	recorder.AddErrors(3)
	recorder.AddRetries(2)
	recorder.AddDead(1)
	recorder.SetPending(7)
	recorder.SetPending(0)
	recorder.ObserveBatchDuration(250 * time.Millisecond)
	var result metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &result); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	want := map[string]int64{"outbox.processed": 2, "outbox.errors": 3, "outbox.retries": 2, "outbox.dead": 1}
	for _, scope := range result.ScopeMetrics {
		for _, m := range scope.Metrics {
			seen[m.Name] = true
			switch data := m.Data.(type) {
			case metricdata.Sum[int64]:
				if len(data.DataPoints) != 1 || data.DataPoints[0].Value != want[m.Name] || data.DataPoints[0].Attributes.Len() != 0 {
					t.Fatalf("counter=%+v", m)
				}
			case metricdata.Gauge[int64]:
				if len(data.DataPoints) != 1 || data.DataPoints[0].Value != 0 || data.DataPoints[0].Attributes.Len() != 0 {
					t.Fatalf("gauge=%+v", m)
				}
			case metricdata.Histogram[float64]:
				if m.Unit != "s" || len(data.DataPoints) != 1 || data.DataPoints[0].Count != 1 ||
					data.DataPoints[0].Sum != 0.25 || data.DataPoints[0].Attributes.Len() != 0 {
					t.Fatalf("duration=%+v", m)
				}
			default:
				t.Fatalf("unexpected instrument=%+v", m)
			}
		}
	}
	if len(seen) != 6 {
		t.Fatalf("instruments=%v", seen)
	}
}

func TestMetricsConstructorPreservesCause(t *testing.T) {
	for _, name := range []string{
		"outbox.processed",
		"outbox.errors",
		"outbox.retries",
		"outbox.dead",
		"outbox.pending",
		"outbox.batch.duration",
	} {
		t.Run(name, func(t *testing.T) {
			cause := errors.New("instrument failure")
			m, err := NewMetrics(failingOutboxMeter{
				Meter: noop.NewMeterProvider().Meter("test"),
				name:  name,
				cause: cause,
			})
			if m != nil || !errors.Is(err, cause) {
				t.Fatalf("constructor=%v %v", m, err)
			}
		})
	}
}

type failingOutboxMeter struct {
	metric.Meter
	name  string
	cause error
}

func (m failingOutboxMeter) Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	if name == m.name {
		return nil, m.cause
	}
	return m.Meter.Int64Counter(name, options...)
}
func (m failingOutboxMeter) Int64Gauge(name string, options ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	if name == m.name {
		return nil, m.cause
	}
	return m.Meter.Int64Gauge(name, options...)
}
func (m failingOutboxMeter) Float64Histogram(name string, options ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if name == m.name {
		return nil, m.cause
	}
	return m.Meter.Float64Histogram(name, options...)
}
