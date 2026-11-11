package outbox

import (
	"context"
	"testing"
	"time"
)

func TestRelaySamplesPendingDuringBusyProcessing(t *testing.T) {
	for _, mode := range []string{"process once", "worker"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			consumer := &busyPendingConsumer{
				cancel: cancel,
			}
			metrics := &captureMetrics{}
			now := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)
			relay := NewRelay(
				consumer,
				HandlerFunc(func(context.Context, Record) error { return nil }),
				WithMetrics(metrics),
				WithPendingInterval(time.Second),
				WithClock(&sequenceClock{
					times: []time.Time{now, now, now.Add(time.Second)},
				}),
			)
			if mode == "worker" {
				if err := relay.Run(ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				for range 3 {
					processed, err := relay.ProcessOnce(ctx)
					if err != nil || !processed {
						t.Fatalf("ProcessOnce = %t, %v", processed, err)
					}
				}
			}
			if metrics.pendingCalls != 2 || metrics.pending != 2 {
				t.Fatalf("pending measurements = %d, last = %d; want 2, 2", metrics.pendingCalls, metrics.pending)
			}
		})
	}
}

type busyPendingConsumer struct {
	cancel  context.CancelFunc
	fetches int
	samples int
}

func (c *busyPendingConsumer) Fetch(context.Context, FetchOptions) (Batch, error) {
	c.fetches++
	if c.fetches > 3 {
		c.cancel()
		return nil, ErrNoRecords
	}
	return &fakeBatch{
		records: []Record{
			{
				ID: ID{1},
			},
		},
	}, nil
}

func (c *busyPendingConsumer) PendingCount(context.Context) (int, error) {
	c.samples++
	return c.samples, nil
}
