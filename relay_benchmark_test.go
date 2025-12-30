package outbox

import (
	"context"
	"testing"
)

type benchBatch struct {
	records []Record
}

func (b *benchBatch) Records() []Record {
	return b.records
}

func (b *benchBatch) Ack(_ context.Context, _ []ID) error {
	return nil
}

func (b *benchBatch) Fail(_ context.Context, _ []Failure) error {
	return nil
}

func (b *benchBatch) Commit() error {
	return nil
}

func (b *benchBatch) Rollback() error {
	return nil
}

type noopConsumer struct{}

func (noopConsumer) Fetch(context.Context, FetchOptions) (Batch, error) {
	return nil, ErrNoRecords
}

func BenchmarkRelayProcessBatch(b *testing.B) {
	records := make([]Record, 100)
	for i := range records {
		records[i] = Record{ID: ID{byte(i + 1)}}
	}
	batch := &benchBatch{records: records}
	relay := NewRelay(noopConsumer{}, HandlerFunc(func(context.Context, Record) error { return nil }))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := relay.processBatch(context.Background(), batch); err != nil {
			b.Fatalf("process batch: %v", err)
		}
	}
}
