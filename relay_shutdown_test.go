package outbox

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestRelayRunDrainsAdmittedBatch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		batch := &fakeBatch{
			records: []Record{{
				ID: ID{1},
			}},
		}
		started := make(chan context.Context, 1)
		release := make(chan struct{})
		done := make(chan error, 1)
		parent, cancel := context.WithCancel(context.WithValue(t.Context(), testContextKey{}, "request value"))
		defer cancel()
		handler := HandlerFunc(func(ctx context.Context, _ Record) error {
			started <- ctx
			<-release
			return ctx.Err()
		})
		relay := NewRelay(
			staticConsumer{
				batch: batch,
			},
			handler,
		)
		go func() { done <- relay.Run(parent) }()
		work := <-started
		if work.Value(testContextKey{}) != "request value" {
			t.Fatal("work context lost the parent value")
		}
		cancel()
		synctest.Wait()
		if err := work.Err(); err != nil {
			close(release)
			<-done
			t.Fatalf("admitted work canceled before draining: %v", err)
		}
		close(release)
		if err := <-done; err != nil {
			t.Fatalf("drain: %v", err)
		}
		if !batch.committed || batch.rolled {
			t.Fatalf("committed=%t rolled back=%t", batch.committed, batch.rolled)
		}
	})
}

func TestRelayRunAlreadyStoppedDoesNotPoll(t *testing.T) {
	for _, expired := range []bool{false, true} {
		t.Run(fmt.Sprintf("expired=%t", expired), func(t *testing.T) {
			parent := t.Context()
			var cancel context.CancelFunc
			if expired {
				parent, cancel = context.WithDeadline(parent, time.Now())
			} else {
				parent, cancel = context.WithCancel(parent)
				cancel()
			}
			defer cancel()
			consumer := &idleConsumer{
				fetched:  make(chan struct{}, 1),
				sampling: make(chan struct{}),
			}
			relay := NewRelay(consumer, HandlerFunc(func(context.Context, Record) error { return nil }))
			if err := relay.Run(parent); err != nil {
				t.Fatalf("already stopped: %v", err)
			}
			select {
			case <-consumer.fetched:
				t.Fatal("already stopped relay polled")
			default:
			}
		})
	}
}

func TestRelayRunParentDeadlineDrainsAndReturnsNil(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		batch := &fakeBatch{
			records: []Record{{
				ID: ID{1},
			}},
		}
		started := make(chan context.Context, 1)
		release := make(chan struct{})
		parent, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		done := make(chan error, 1)
		handler := HandlerFunc(func(ctx context.Context, _ Record) error {
			started <- ctx
			<-release
			return ctx.Err()
		})
		relay := NewRelay(
			staticConsumer{
				batch: batch,
			},
			handler,
		)
		go func() { done <- relay.Run(parent) }()
		work := <-started
		<-parent.Done()
		if work.Err() != nil {
			t.Fatalf("work context ended with parent: %v", work.Err())
		}
		close(release)
		if err := <-done; err != nil || !batch.committed {
			t.Fatalf("deadline drain error=%v committed=%t", err, batch.committed)
		}
	})
}

func TestRelayRunDrainsAdmittedFetch(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		batch := &fakeBatch{
			records: []Record{{
				ID: ID{1},
			}},
		}
		consumer := &pausedFetchConsumer{
			batch:   batch,
			entered: make(chan context.Context, 1),
			release: make(chan struct{}),
		}
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		relay := NewRelay(consumer, HandlerFunc(func(context.Context, Record) error { return nil }))
		go func() { done <- relay.Run(parent) }()
		work := <-consumer.entered
		cancel()
		synctest.Wait()
		if err := work.Err(); err != nil {
			t.Fatalf("admitted fetch canceled: %v", err)
		}
		close(consumer.release)
		if err := <-done; err != nil || !batch.committed {
			t.Fatalf("fetch drain error=%v committed=%t", err, batch.committed)
		}
	})
}

func TestRelayProcessOnceUsesCallerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		batch := &fakeBatch{
			records: []Record{{
				ID: ID{1},
			}},
		}
		started := make(chan struct{})
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		handler := HandlerFunc(func(ctx context.Context, _ Record) error {
			close(started)
			<-ctx.Done()
			return ctx.Err()
		})
		relay := NewRelay(
			staticConsumer{
				batch: batch,
			},
			handler,
		)
		go func() {
			_, err := relay.ProcessOnce(parent)
			done <- err
		}()
		<-started
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) || !batch.rolled {
			t.Fatalf("ProcessOnce cancellation error=%v rollback=%t", err, batch.rolled)
		}
	})
}

func TestRelayRunImmediatePreservesHandlerAndRollbackErrors(t *testing.T) {
	for _, timeout := range []time.Duration{0, -time.Second} {
		t.Run(timeout.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				failure := errors.New("publication interrupted")
				rollback := errors.New("rollback failed")
				batch := &fakeBatch{
					records: []Record{{
						ID: ID{1},
					}},
					rollErr: rollback,
				}
				started := make(chan struct{})
				canceled := make(chan struct{})
				release := make(chan struct{})
				parent, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				handler := HandlerFunc(func(ctx context.Context, _ Record) error {
					close(started)
					<-ctx.Done()
					close(canceled)
					<-release
					return failure
				})
				relay := NewRelay(
					staticConsumer{
						batch: batch,
					},
					handler,
					WithShutdownTimeout(timeout),
				)
				go func() { done <- relay.Run(parent) }()
				<-started
				cancel()
				<-canceled
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("returned before handler finished: %v", err)
				default:
				}
				close(release)
				err := <-done
				if !errors.Is(err, failure) || !errors.Is(err, rollback) || !errors.Is(err, context.Canceled) ||
					errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("immediate shutdown lost causes: %v", err)
				}
			})
		})
	}
}

func TestRelayRunTimeoutJoinsHandlerEvenAfterSuccessfulWork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		batch := &fakeBatch{
			records: []Record{{
				ID: ID{1},
			}},
		}
		started := make(chan struct{})
		expired := make(chan struct{})
		release := make(chan struct{})
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		handler := HandlerFunc(func(ctx context.Context, _ Record) error {
			close(started)
			<-ctx.Done()
			close(expired)
			<-release
			return nil
		})
		relay := NewRelay(
			staticConsumer{
				batch: batch,
			},
			handler,
			WithShutdownTimeout(time.Second),
		)
		go func() { done <- relay.Run(parent) }()
		<-started
		cancel()
		<-expired
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("returned before handler finished: %v", err)
		default:
		}
		close(release)
		err := <-done
		if !errors.Is(err, context.DeadlineExceeded) || !batch.committed {
			t.Fatalf("timed out work error=%v committed=%t", err, batch.committed)
		}
	})
}

func TestRelayRunJoinsFailuresFromAdmittedWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		first := errors.New("classifier panic")
		second := errors.New("second settlement failed")
		consumer := &queuedConsumer{
			batches: make(chan Batch, 2),
		}
		consumer.batches <- &fakeBatch{
			records: []Record{{
				ID: ID{1},
			}},
		}
		consumer.batches <- &fakeBatch{
			records: []Record{{
				ID: ID{2},
			}},
			ackErr: second,
		}
		started := make(chan byte, 2)
		release := make(chan struct{})
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		handler := HandlerFunc(func(_ context.Context, record Record) error {
			started <- record.ID[0]
			<-release
			if record.ID[0] == 1 {
				return errors.New("handler failed")
			}
			return nil
		})
		classifier := func(context.Context, Record, error) FailureAction {
			panic(first)
		}
		relay := NewRelay(consumer, handler, WithWorkers(2), WithFailureClassifier(classifier))
		go func() { done <- relay.Run(parent) }()
		<-started
		<-started
		cancel()
		synctest.Wait()
		close(release)
		err := <-done
		if !errors.Is(err, ErrWorkerPanic) || !errors.Is(err, first) || !errors.Is(err, second) {
			t.Fatalf("worker failures not joined: %v", err)
		}
		if calls := consumer.calls.Load(); calls != 2 {
			t.Fatalf("fetches after stop: %d", calls)
		}
	})
}

func TestRelayRunDrainsSiblingAfterFatalFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("settlement failed")
		consumer := &queuedConsumer{
			batches: make(chan Batch, 2),
		}
		consumer.batches <- &fakeBatch{
			records: []Record{{
				ID: ID{1},
			}},
			ackErr: failure,
		}
		sibling := &fakeBatch{
			records: []Record{{
				ID: ID{2},
			}},
		}
		consumer.batches <- sibling
		started := make(chan byte, 2)
		siblingCtx := make(chan context.Context, 1)
		releaseFirst := make(chan struct{})
		releaseSibling := make(chan struct{})
		failed := make(chan struct{}, 1)
		done := make(chan error, 1)
		handler := HandlerFunc(func(ctx context.Context, record Record) error {
			started <- record.ID[0]
			if record.ID[0] == 1 {
				<-releaseFirst
			} else {
				siblingCtx <- ctx
				<-releaseSibling
			}
			return nil
		})
		relay := NewRelay(
			consumer,
			handler,
			WithWorkers(2),
			WithLogger(signalLogger{
				failed: failed,
			}),
		)
		go func() { done <- relay.Run(t.Context()) }()
		<-started
		<-started
		work := <-siblingCtx
		close(releaseFirst)
		<-failed
		if err := work.Err(); err != nil {
			t.Fatalf("sibling canceled before draining: %v", err)
		}
		close(releaseSibling)
		if err := <-done; !errors.Is(err, failure) || !sibling.committed {
			t.Fatalf("sibling drain error=%v committed=%t", err, sibling.committed)
		}
		if calls := consumer.calls.Load(); calls != 2 {
			t.Fatalf("fetches after failure: %d", calls)
		}
	})
}

func TestRelayRunStopsIdlePollingAndPendingSampling(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending=%t", pending), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				consumer := &idleConsumer{
					fetched:  make(chan struct{}, 1),
					sampling: make(chan struct{}),
				}
				options := []RelayOption{WithPollInterval(time.Hour)}
				if pending {
					options = append(options, WithPendingInterval(time.Second))
				}
				parent, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan error, 1)
				relay := NewRelay(consumer, HandlerFunc(func(context.Context, Record) error { return nil }), options...)
				go func() { done <- relay.Run(parent) }()
				<-consumer.fetched
				if pending {
					<-consumer.sampling
				}
				synctest.Wait()
				stoppedAt := time.Now()
				cancel()
				if err := <-done; err != nil {
					t.Fatalf("idle shutdown: %v", err)
				}
				if elapsed := time.Since(stoppedAt); elapsed != 0 {
					t.Fatalf("idle shutdown waited %s", elapsed)
				}
			})
		})
	}
}

func TestRelayRunDoesNotRestartSharedBudgetOnWorkerFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		settlement := errors.New("settlement failed")
		interrupted := errors.New("publication interrupted")
		consumer := &queuedConsumer{
			batches: make(chan Batch, 2),
		}
		consumer.batches <- &fakeBatch{
			records: []Record{{
				ID: ID{1},
			}},
			ackErr: settlement,
		}
		consumer.batches <- &fakeBatch{
			records: []Record{{
				ID: ID{2},
			}},
		}
		started := make(chan byte, 2)
		release := make(chan struct{})
		expired := make(chan time.Duration, 1)
		parent, cancel := context.WithCancel(t.Context())
		defer cancel()
		done := make(chan error, 1)
		var stopAt time.Time
		handler := HandlerFunc(func(ctx context.Context, record Record) error {
			started <- record.ID[0]
			if record.ID[0] == 1 {
				<-release
				return nil
			}
			<-ctx.Done()
			expired <- time.Since(stopAt)
			return interrupted
		})
		relay := NewRelay(consumer, handler, WithWorkers(2), WithShutdownTimeout(time.Second))
		go func() { done <- relay.Run(parent) }()
		<-started
		<-started
		stopAt = time.Now()
		cancel()
		synctest.Wait()
		halfBudget, cancelHalfBudget := context.WithTimeout(t.Context(), 500*time.Millisecond)
		defer cancelHalfBudget()
		<-halfBudget.Done()
		close(release)
		synctest.Wait()
		if elapsed := <-expired; elapsed != time.Second {
			t.Fatalf("shared budget elapsed=%s", elapsed)
		}
		err := <-done
		if !errors.Is(err, settlement) || !errors.Is(err, interrupted) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shared shutdown failures: %v", err)
		}
	})
}

func TestRelayPanicCausesReachCallbacksAndLogs(t *testing.T) {
	for _, value := range []any{errors.New("handler panic cause"), "handler panic value"} {
		t.Run("handler/"+fmt.Sprint(value), func(t *testing.T) {
			batch := &fakeBatch{
				records: []Record{{
					ID: ID{1},
				}},
			}
			var callbackErr, classifierErr error
			handler := HandlerFunc(func(context.Context, Record) error {
				panic(value)
			})
			callback := func(_ context.Context, _ Record, err error) {
				callbackErr = err
			}
			classifier := func(_ context.Context, _ Record, err error) FailureAction {
				classifierErr = err
				return FailureRetry
			}
			relay := NewRelay(
				staticConsumer{
					batch: batch,
				},
				handler,
				WithErrorHandler(callback),
				WithFailureClassifier(classifier),
			)
			if _, err := relay.ProcessOnce(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(callbackErr.Error(), fmt.Sprint(value)) || classifierErr != callbackErr ||
				len(batch.failures) != 1 || batch.failures[0].Err != errHandlerPanicked || !batch.committed {
				t.Fatalf("handler panic callback=%v classifier=%v batch=%+v", callbackErr, classifierErr, batch)
			}
			if cause, ok := value.(error); ok && !errors.Is(callbackErr, cause) {
				t.Fatalf("handler panic cause lost: %v", callbackErr)
			}
		})
	}
	for _, value := range []any{errors.New("worker panic cause"), "worker panic value"} {
		t.Run("worker/"+fmt.Sprint(value), func(t *testing.T) {
			rollback := errors.New("rollback failure")
			batch := &fakeBatch{
				records: []Record{{
					ID: ID{1},
				}},
				rollErr: rollback,
			}
			var logs bytes.Buffer
			handler := HandlerFunc(func(context.Context, Record) error {
				return errors.New("handler failed")
			})
			classifier := func(context.Context, Record, error) FailureAction {
				panic(value)
			}
			relay := NewRelay(
				staticConsumer{
					batch: batch,
				},
				handler,
				WithFailureClassifier(classifier),
				WithLogger(slog.New(slog.NewJSONHandler(&logs, nil))),
			)
			err := relay.Run(t.Context())
			if !errors.Is(err, ErrWorkerPanic) || !errors.Is(err, rollback) ||
				!strings.Contains(err.Error(), fmt.Sprint(value)) || !strings.Contains(logs.String(), fmt.Sprint(value)) ||
				!strings.Contains(logs.String(), rollback.Error()) {
				t.Fatalf("worker panic error=%v log=%s", err, logs.String())
			}
			if cause, ok := value.(error); ok && !errors.Is(err, cause) {
				t.Fatalf("worker panic cause lost: %v", err)
			}
		})
	}
}

type queuedConsumer struct {
	batches chan Batch
	calls   atomic.Int32
}

type testContextKey struct{}

type idleConsumer struct {
	fetched  chan struct{}
	sampling chan struct{}
}

type signalLogger struct {
	NopLogger
	failed chan struct{}
}

func (l signalLogger) Error(string, ...any) {
	l.failed <- struct{}{}
}

type pausedFetchConsumer struct {
	batch   Batch
	entered chan context.Context
	release chan struct{}
}

func (c *pausedFetchConsumer) Fetch(ctx context.Context, _ FetchOptions) (Batch, error) {
	c.entered <- ctx
	<-c.release
	return c.batch, nil
}

func (c *idleConsumer) Fetch(context.Context, FetchOptions) (Batch, error) {
	c.fetched <- struct{}{}
	return nil, ErrNoRecords
}

func (c *idleConsumer) PendingCount(ctx context.Context) (int, error) {
	close(c.sampling)
	<-ctx.Done()
	return 0, ctx.Err()
}

func (c *queuedConsumer) Fetch(ctx context.Context, _ FetchOptions) (Batch, error) {
	c.calls.Add(1)
	select {
	case batch := <-c.batches:
		return batch, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
