package oteloutbox

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/velmie/outbox"
)

func TestMetricsRecordDistinctAttributeSeries(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	meter := provider.Meter("test")
	attrs := []attribute.KeyValue{attribute.String("endpoint", "first")}
	first, err := NewMetrics(meter, attrs...)
	if err != nil {
		t.Fatal(err)
	}
	attrs[0] = attribute.String("endpoint", "changed")
	second, err := NewMetrics(meter, attribute.String("endpoint", "second"))
	if err != nil {
		t.Fatal(err)
	}

	first.AddProcessed(1)
	first.AddErrors(2)
	first.AddRetries(3)
	first.AddDead(4)
	first.SetPending(9)
	first.SetPending(0)
	first.ObserveBatchDuration(250 * time.Millisecond)
	second.AddProcessed(5)
	second.AddErrors(6)
	second.AddRetries(7)
	second.AddDead(8)
	second.SetPending(11)
	second.ObserveBatchDuration(500 * time.Millisecond)

	var result metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &result); err != nil {
		t.Fatal(err)
	}
	want := map[string]map[string]float64{
		"outbox.processed":      {"first": 1, "second": 5},
		"outbox.errors":         {"first": 2, "second": 6},
		"outbox.retries":        {"first": 3, "second": 7},
		"outbox.dead":           {"first": 4, "second": 8},
		"outbox.pending":        {"first": 0, "second": 11},
		"outbox.batch.duration": {"first": 0.25, "second": 0.5},
	}
	seen := make(map[string]map[string]float64)
	for _, scope := range result.ScopeMetrics {
		for _, measurement := range scope.Metrics {
			points := make(map[string]float64)
			switch data := measurement.Data.(type) {
			case metricdata.Sum[int64]:
				for _, point := range data.DataPoints {
					endpoint, ok := point.Attributes.Value("endpoint")
					if !ok || point.Attributes.Len() != 1 {
						t.Fatalf("%s: unexpected attributes %v", measurement.Name, point.Attributes)
					}
					points[endpoint.AsString()] = float64(point.Value)
				}
			case metricdata.Gauge[int64]:
				for _, point := range data.DataPoints {
					endpoint, ok := point.Attributes.Value("endpoint")
					if !ok || point.Attributes.Len() != 1 {
						t.Fatalf("%s: unexpected attributes %v", measurement.Name, point.Attributes)
					}
					points[endpoint.AsString()] = float64(point.Value)
				}
			case metricdata.Histogram[float64]:
				for _, point := range data.DataPoints {
					endpoint, ok := point.Attributes.Value("endpoint")
					if !ok || point.Attributes.Len() != 1 || point.Count != 1 {
						t.Fatalf("%s: unexpected point %+v", measurement.Name, point)
					}
					points[endpoint.AsString()] = point.Sum
				}
			default:
				t.Fatalf("unexpected instrument=%+v", measurement)
			}
			seen[measurement.Name] = points
		}
	}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("measurements = %v, want %v", seen, want)
	}
}

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
				if m.Unit != "{record}" || m.Description == "" {
					t.Errorf("counter metadata: name=%s unit=%q description=%q", m.Name, m.Unit, m.Description)
				}
				if len(data.DataPoints) != 1 || data.DataPoints[0].Value != want[m.Name] || data.DataPoints[0].Attributes.Len() != 0 {
					t.Fatalf("counter=%+v", m)
				}
			case metricdata.Gauge[int64]:
				if m.Unit != "{record}" || m.Description == "" {
					t.Errorf("gauge metadata: name=%s unit=%q description=%q", m.Name, m.Unit, m.Description)
				}
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

func TestBatchDurationBuckets(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	m, err := NewMetrics(provider.Meter("test"))
	if err != nil {
		t.Fatal(err)
	}
	for _, duration := range []time.Duration{3 * time.Millisecond, 40 * time.Millisecond, 250 * time.Millisecond, 2 * time.Second} {
		m.ObserveBatchDuration(duration)
	}

	var result metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &result); err != nil {
		t.Fatal(err)
	}
	for _, scope := range result.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "outbox.batch.duration" {
				continue
			}
			histogram, ok := m.Data.(metricdata.Histogram[float64])
			if !ok || len(histogram.DataPoints) != 1 {
				t.Fatalf("unexpected histogram: %+v", m.Data)
			}
			point := histogram.DataPoints[0]
			wantBounds := []float64{0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10}
			wantCounts := []uint64{1, 0, 0, 1, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0}
			if !slices.Equal(point.Bounds, wantBounds) {
				t.Errorf("bounds = %v, want %v", point.Bounds, wantBounds)
			}
			if !slices.Equal(point.BucketCounts, wantCounts) {
				t.Errorf("bucket counts = %v, want %v", point.BucketCounts, wantCounts)
			}
			return
		}
	}
	t.Fatal("batch duration histogram not collected")
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
