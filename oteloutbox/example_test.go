package oteloutbox_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/oteloutbox"
)

func ExampleNewMetrics() {
	// Configure a reader and exporter for the application in production.
	provider := sdkmetric.NewMeterProvider()
	defer func() { _ = provider.Shutdown(context.Background()) }()

	metrics, err := oteloutbox.NewMetrics(provider.Meter("orders"))
	if err != nil {
		fmt.Println(err)
		return
	}

	consumer := exampleConsumer{}
	handler := outbox.HandlerFunc(func(context.Context, outbox.Record) error {
		return nil
	})
	relay := outbox.NewRelay(
		consumer,
		handler,
		outbox.WithMetrics(metrics),
		outbox.WithPendingInterval(5*time.Second),
	)
	fmt.Println(relay != nil)
	// Output: true
}

type exampleConsumer struct{}

func (exampleConsumer) Fetch(context.Context, outbox.FetchOptions) (outbox.Batch, error) {
	return nil, errors.New("example consumer is not connected")
}
