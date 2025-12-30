package outbox

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

type staticConsumer struct {
	batch Batch
	err   error
}

func (c staticConsumer) Fetch(_ context.Context, _ FetchOptions) (Batch, error) {
	return c.batch, c.err
}

type fakeBatch struct {
	records   []Record
	ackIDs    []ID
	failures  []Failure
	dead      []Failure
	committed bool
	rolled    bool
	ackErr    error
	failErr   error
	deadErr   error
	commitErr error
	rollErr   error
}

type fakeBatchNoDead struct {
	records   []Record
	ackIDs    []ID
	failures  []Failure
	committed bool
	rolled    bool
	ackErr    error
	failErr   error
	commitErr error
	rollErr   error
}

func (b *fakeBatchNoDead) Records() []Record {
	return b.records
}

func (b *fakeBatchNoDead) Ack(_ context.Context, ids []ID) error {
	b.ackIDs = append(b.ackIDs, ids...)
	return b.ackErr
}

func (b *fakeBatchNoDead) Fail(_ context.Context, failures []Failure) error {
	b.failures = append(b.failures, failures...)
	return b.failErr
}

func (b *fakeBatchNoDead) Commit() error {
	b.committed = true
	return b.commitErr
}

func (b *fakeBatchNoDead) Rollback() error {
	b.rolled = true
	return b.rollErr
}

func (b *fakeBatch) Records() []Record {
	return b.records
}

func (b *fakeBatch) Ack(_ context.Context, ids []ID) error {
	b.ackIDs = append(b.ackIDs, ids...)
	return b.ackErr
}

func (b *fakeBatch) Fail(_ context.Context, failures []Failure) error {
	b.failures = append(b.failures, failures...)
	return b.failErr
}

func (b *fakeBatch) Dead(_ context.Context, failures []Failure) error {
	b.dead = append(b.dead, failures...)
	return b.deadErr
}

func (b *fakeBatch) Commit() error {
	b.committed = true
	return b.commitErr
}

func (b *fakeBatch) Rollback() error {
	b.rolled = true
	return b.rollErr
}

type captureConsumer struct {
	opts FetchOptions
	err  error
}

func (c *captureConsumer) Fetch(_ context.Context, opts FetchOptions) (Batch, error) {
	c.opts = opts
	if c.err != nil {
		return nil, c.err
	}
	return nil, ErrNoRecords
}

type cancelConsumer struct {
	started  chan struct{}
	allowErr chan struct{}
	err      error
	canceled int32
}

func (c *cancelConsumer) Fetch(ctx context.Context, _ FetchOptions) (Batch, error) {
	c.started <- struct{}{}
	select {
	case <-c.allowErr:
		return nil, c.err
	case <-ctx.Done():
		atomic.StoreInt32(&c.canceled, 1)
		return nil, ctx.Err()
	}
}

type pendingConsumer struct {
	count int
	calls int
}

func (c *pendingConsumer) Fetch(_ context.Context, _ FetchOptions) (Batch, error) {
	return nil, ErrNoRecords
}

func (c *pendingConsumer) PendingCount(_ context.Context) (int, error) {
	c.calls++
	return c.count, nil
}

type captureMetrics struct {
	pending      int
	pendingCalls int
}

func (captureMetrics) ObserveBatchDuration(time.Duration) {}
func (captureMetrics) AddProcessed(int)                   {}
func (captureMetrics) AddErrors(int)                      {}
func (captureMetrics) AddRetries(int)                     {}
func (captureMetrics) AddDead(int)                        {}
func (m *captureMetrics) SetPending(count int) {
	m.pending = count
	m.pendingCalls++
}

func TestRelayProcessOnce(t *testing.T) {
	records := []Record{{ID: ID{1}}, {ID: ID{2}}, {ID: ID{3}}}
	batch := &fakeBatch{records: records}
	consumer := staticConsumer{batch: batch}

	handler := HandlerFunc(func(_ context.Context, record Record) error {
		if record.ID == (ID{2}) {
			return errors.New("fail")
		}
		return nil
	})

	relay := NewRelay(consumer, handler)
	ok, err := relay.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("process once: %v", err)
	}
	if !ok {
		t.Fatalf("expected batch to be processed")
	}
	if len(batch.ackIDs) != 2 {
		t.Fatalf("expected 2 ack ids, got %d", len(batch.ackIDs))
	}
	if len(batch.failures) != 1 {
		t.Fatalf("expected 1 failure, got %d", len(batch.failures))
	}
	if !batch.committed {
		t.Fatalf("expected commit")
	}
}

func TestRelayFailureHandlerCalled(t *testing.T) {
	batch := &fakeBatch{records: []Record{{ID: ID{1}}}}
	var calls int
	relay := NewRelay(staticConsumer{}, HandlerFunc(func(context.Context, Record) error {
		return errors.New("boom")
	}), WithErrorHandler(func(context.Context, Record, error) {
		calls++
	}))

	if err := relay.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("process batch: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected failure handler to be called once, got %d", calls)
	}
}

func TestRelayFailureHandlerNotCalledOnContextCancel(t *testing.T) {
	batch := &fakeBatch{records: []Record{{ID: ID{1}}}}
	var calls int
	relay := NewRelay(staticConsumer{}, HandlerFunc(func(ctx context.Context, _ Record) error {
		return ctx.Err()
	}), WithErrorHandler(func(context.Context, Record, error) {
		calls++
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := relay.processBatch(ctx, batch)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("expected failure handler not to be called, got %d", calls)
	}
}

func TestRelayProcessBatchAckErrorRollback(t *testing.T) {
	batch := &fakeBatch{records: []Record{{ID: ID{1}}}, ackErr: errors.New("ack fail")}
	relay := NewRelay(staticConsumer{}, HandlerFunc(func(context.Context, Record) error { return nil }))

	err := relay.processBatch(context.Background(), batch)
	if err == nil || !errors.Is(err, batch.ackErr) {
		t.Fatalf("expected ack error, got %v", err)
	}
	if !batch.rolled {
		t.Fatalf("expected rollback on ack error")
	}
	if batch.committed {
		t.Fatalf("expected no commit on ack error")
	}
}

func TestRelayProcessBatchFailErrorRollback(t *testing.T) {
	batch := &fakeBatch{records: []Record{{ID: ID{1}}}, failErr: errors.New("fail update")}
	relay := NewRelay(staticConsumer{}, HandlerFunc(func(context.Context, Record) error { return errors.New("boom") }))

	err := relay.processBatch(context.Background(), batch)
	if err == nil || !errors.Is(err, batch.failErr) {
		t.Fatalf("expected fail error, got %v", err)
	}
	if !batch.rolled {
		t.Fatalf("expected rollback on fail error")
	}
	if batch.committed {
		t.Fatalf("expected no commit on fail error")
	}
}

func TestRelayProcessBatchCommitErrorRollback(t *testing.T) {
	batch := &fakeBatch{records: []Record{{ID: ID{1}}}, commitErr: errors.New("commit fail")}
	relay := NewRelay(staticConsumer{}, HandlerFunc(func(context.Context, Record) error { return nil }))

	err := relay.processBatch(context.Background(), batch)
	if err == nil || !errors.Is(err, batch.commitErr) {
		t.Fatalf("expected commit error, got %v", err)
	}
	if !batch.rolled {
		t.Fatalf("expected rollback on commit error")
	}
	if !batch.committed {
		t.Fatalf("expected commit to be attempted")
	}
}

func TestRelayProcessBatchDeadClassifier(t *testing.T) {
	records := []Record{{ID: ID{1}}, {ID: ID{2}}}
	batch := &fakeBatch{records: records}
	relay := NewRelay(staticConsumer{}, HandlerFunc(func(_ context.Context, record Record) error {
		if record.ID == (ID{2}) {
			return errors.New("boom")
		}
		return nil
	}), WithFailureClassifier(func(context.Context, Record, error) FailureAction {
		return FailureDead
	}))

	if err := relay.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("process batch: %v", err)
	}
	if len(batch.dead) != 1 {
		t.Fatalf("expected 1 dead failure, got %d", len(batch.dead))
	}
	if len(batch.failures) != 0 {
		t.Fatalf("expected no retry failures, got %d", len(batch.failures))
	}
	if len(batch.ackIDs) != 1 {
		t.Fatalf("expected 1 ack id, got %d", len(batch.ackIDs))
	}
	if !batch.committed {
		t.Fatalf("expected commit")
	}
}

func TestRelayProcessBatchDeadFallback(t *testing.T) {
	records := []Record{{ID: ID{1}}}
	batch := &fakeBatchNoDead{records: records}
	relay := NewRelay(staticConsumer{}, HandlerFunc(func(context.Context, Record) error {
		return errors.New("boom")
	}), WithFailureClassifier(func(context.Context, Record, error) FailureAction {
		return FailureDead
	}))

	if err := relay.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("process batch: %v", err)
	}
	if len(batch.failures) != 1 {
		t.Fatalf("expected 1 failure fallback, got %d", len(batch.failures))
	}
	if !batch.committed {
		t.Fatalf("expected commit")
	}
}

func TestRelayHandlerTimeoutApplied(t *testing.T) {
	batch := &fakeBatch{records: []Record{{ID: ID{1}}}}
	deadlineCh := make(chan time.Time, 1)
	relay := NewRelay(staticConsumer{}, HandlerFunc(func(ctx context.Context, _ Record) error {
		if deadline, ok := ctx.Deadline(); ok {
			deadlineCh <- deadline
		} else {
			deadlineCh <- time.Time{}
		}
		return nil
	}), WithHandlerTimeout(10*time.Millisecond))

	if err := relay.processBatch(context.Background(), batch); err != nil {
		t.Fatalf("process batch: %v", err)
	}
	deadline := <-deadlineCh
	if deadline.IsZero() {
		t.Fatalf("expected handler deadline")
	}
}

func TestRelayProcessOnceNoRecords(t *testing.T) {
	relay := NewRelay(staticConsumer{err: ErrNoRecords}, HandlerFunc(func(context.Context, Record) error { return nil }))
	ok, err := relay.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("process once: %v", err)
	}
	if ok {
		t.Fatalf("expected no batch")
	}
}

func TestRelayRunContextCancel(t *testing.T) {
	consumer := staticConsumer{err: ErrNoRecords}
	relay := NewRelay(consumer, HandlerFunc(func(context.Context, Record) error { return nil }), WithPollInterval(5*time.Millisecond))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := relay.Run(ctx); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRelayRunCancelsOtherWorkers(t *testing.T) {
	consumer := &cancelConsumer{
		started:  make(chan struct{}, 2),
		allowErr: make(chan struct{}, 1),
		err:      errors.New("boom"),
	}
	relay := NewRelay(consumer, HandlerFunc(func(context.Context, Record) error { return nil }), WithWorkers(2))

	errCh := make(chan error, 1)
	go func() {
		errCh <- relay.Run(context.Background())
	}()

	<-consumer.started
	<-consumer.started
	consumer.allowErr <- struct{}{}

	err := <-errCh
	if err == nil || err.Error() != "boom" {
		t.Fatalf("expected boom error, got %v", err)
	}
	if atomic.LoadInt32(&consumer.canceled) != 1 {
		t.Fatalf("expected other worker to observe cancellation")
	}
}

func TestRelayProcessBatchContextCanceled(t *testing.T) {
	batch := &fakeBatch{records: []Record{{ID: ID{1}}}}
	relay := NewRelay(staticConsumer{}, HandlerFunc(func(ctx context.Context, _ Record) error {
		return ctx.Err()
	}))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := relay.processBatch(ctx, batch)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if !batch.rolled {
		t.Fatalf("expected rollback on context cancel")
	}
	if batch.committed {
		t.Fatalf("expected no commit on context cancel")
	}
	if len(batch.ackIDs) != 0 || len(batch.failures) != 0 {
		t.Fatalf("expected no ack/fail on context cancel")
	}
}

func TestRelayProcessBatchEmpty(t *testing.T) {
	batch := &fakeBatch{}
	relay := NewRelay(staticConsumer{}, HandlerFunc(func(context.Context, Record) error { return nil }))

	err := relay.processBatch(context.Background(), batch)
	if !errors.Is(err, ErrEmptyBatch) {
		t.Fatalf("expected ErrEmptyBatch, got %v", err)
	}
	if !batch.rolled {
		t.Fatalf("expected rollback on empty batch")
	}
}

func TestRelayProcessBatchNil(t *testing.T) {
	relay := NewRelay(staticConsumer{}, HandlerFunc(func(context.Context, Record) error { return nil }))

	err := relay.processBatch(context.Background(), nil)
	if !errors.Is(err, ErrNilBatch) {
		t.Fatalf("expected ErrNilBatch, got %v", err)
	}
}

func TestRelayPartitionWindowApplied(t *testing.T) {
	now := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)
	window := 2 * time.Hour
	consumer := &captureConsumer{}
	relay := NewRelay(consumer, HandlerFunc(func(context.Context, Record) error { return nil }), WithClock(fixedClock{now: now}), WithPartitionWindow(window))

	ok, err := relay.ProcessOnce(context.Background())
	if err != nil {
		t.Fatalf("process once: %v", err)
	}
	if ok {
		t.Fatalf("expected no batch")
	}
	expected := now.Add(-window)
	if !consumer.opts.MinCreatedAt.Equal(expected) {
		t.Fatalf("expected MinCreatedAt %v, got %v", expected, consumer.opts.MinCreatedAt)
	}
}

func TestRelayPendingCountDisabledByDefault(t *testing.T) {
	consumer := &pendingConsumer{count: 10}
	metrics := &captureMetrics{}
	relay := NewRelay(consumer, HandlerFunc(func(context.Context, Record) error { return nil }), WithMetrics(metrics))

	relay.maybeRecordPending(context.Background())

	if consumer.calls != 0 {
		t.Fatalf("expected no pending count calls, got %d", consumer.calls)
	}
	if metrics.pendingCalls != 0 {
		t.Fatalf("expected no pending metric updates, got %d", metrics.pendingCalls)
	}
}

func TestRelayPendingCountEnabled(t *testing.T) {
	now := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)
	clock := &sequenceClock{times: []time.Time{now, now, now.Add(time.Second)}}
	consumer := &pendingConsumer{count: 42}
	metrics := &captureMetrics{}
	relay := NewRelay(
		consumer,
		HandlerFunc(func(context.Context, Record) error { return nil }),
		WithClock(clock),
		WithMetrics(metrics),
		WithPendingInterval(time.Second),
	)

	relay.maybeRecordPending(context.Background())
	relay.maybeRecordPending(context.Background())
	relay.maybeRecordPending(context.Background())

	if consumer.calls != 2 {
		t.Fatalf("expected 2 pending count calls, got %d", consumer.calls)
	}
	if metrics.pendingCalls != 2 {
		t.Fatalf("expected 2 pending metric updates, got %d", metrics.pendingCalls)
	}
	if metrics.pending != 42 {
		t.Fatalf("expected pending count 42, got %d", metrics.pending)
	}
}
