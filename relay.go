package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// FailureHandler is called when record processing returns an error.
// It receives the original handler error or an error containing the panic value.
type FailureHandler func(ctx context.Context, record Record, err error)

// Relay polls a Consumer and invokes a Handler for each record.
type Relay struct {
	consumer Consumer
	handler  Handler
	cfg      RelayConfig

	pendingMu sync.Mutex
	pendingAt time.Time
}

// NewRelay constructs a Relay with defaults and optional settings.
func NewRelay(consumer Consumer, handler Handler, opts ...RelayOption) *Relay {
	if consumer == nil {
		panic("outbox: nil Consumer")
	}
	if handler == nil {
		panic("outbox: nil Handler")
	}

	cfg := RelayConfig{
		ShutdownTimeout: defaultShutdownTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	cfg = cfg.withDefaults()

	return &Relay{
		consumer: consumer,
		handler:  handler,
		cfg:      cfg,
	}
}

// Run polls with the configured workers until the parent stops or a worker
// fails. It stops admitting polls and lets admitted batches finish within the
// shared shutdown timeout. Parent cancellation and deadlines request shutdown
// and return nil if the drain succeeds. Run cancels work when the budget expires
// and waits for every worker, so handlers and dependencies must cooperate with
// cancellation. Parent values are available to work without the parent deadline.
func (r *Relay) Run(ctx context.Context) error {
	if ctx.Err() != nil {
		return nil
	}
	stopCtx, stop := context.WithCancel(ctx)
	defer stop()
	workCtx, cancelWork := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelWork()

	errCh := make(chan error, r.cfg.Workers)
	finished := make(chan time.Time, 1)
	var remaining atomic.Int64
	remaining.Store(int64(r.cfg.Workers))

	for i := 0; i < r.cfg.Workers; i++ {
		workerID := i
		go func() {
			defer func() {
				if remaining.Add(-1) == 0 {
					finished <- time.Now()
				}
			}()
			var workerErr error
			defer func() {
				if value := recover(); value != nil {
					panicErr := recoveredError(ErrWorkerPanic, value)
					workerErr = errors.Join(workerErr, panicErr)
					stop()
					r.reportWorkerPanic(workerID, panicErr)
				}
				if workerErr != nil {
					errCh <- workerErr
				}
			}()

			if err := r.runWorker(stopCtx, workCtx); err != nil {
				workerErr = err
				stop()
				r.cfg.Logger.Error("outbox worker error",
					"event", "relay.worker_failed", "operation", "relay.run", "outcome", "failed",
					"worker", workerID, "err", err,
				)
			}
		}()
	}

	<-stopCtx.Done()
	var timedOut bool
	if r.cfg.ShutdownTimeout <= 0 {
		cancelWork()
		<-finished
	} else {
		deadline := time.Now().Add(r.cfg.ShutdownTimeout)
		timer := time.NewTimer(r.cfg.ShutdownTimeout)
		defer timer.Stop()
		select {
		case completedAt := <-finished:
			timedOut = !completedAt.Before(deadline)
		case <-timer.C:
			cancelWork()
			completedAt := <-finished
			timedOut = !completedAt.Before(deadline)
		}
	}
	close(errCh)
	var result error
	for err := range errCh {
		result = errors.Join(result, err)
	}
	if timedOut {
		result = errors.Join(result, fmt.Errorf(
			"outbox relay shutdown timed out after %s: %w",
			r.cfg.ShutdownTimeout,
			context.DeadlineExceeded,
		))
	}

	return result
}

// ProcessOnce fetches and processes a single batch.
func (r *Relay) ProcessOnce(ctx context.Context) (bool, error) {
	batch, err := r.fetchBatch(ctx)
	if err != nil {
		if errors.Is(err, ErrNoRecords) {
			r.maybeRecordPending(ctx)

			return false, nil
		}

		return false, err
	}

	if err := r.processBatch(ctx, batch); err != nil {
		return false, err
	}

	return true, nil
}

type batchOutcome struct {
	successful []ID
	failed     []Failure
	dead       []Failure
}

func (r *Relay) runWorker(stopCtx, workCtx context.Context) error {
	for {
		select {
		case <-stopCtx.Done():
			return nil
		default:
		}

		batch, err := r.fetchBatch(workCtx)
		if err != nil {
			if errors.Is(err, ErrNoRecords) {
				r.maybeRecordPending(stopCtx)
				if !r.sleep(stopCtx, r.cfg.PollInterval) {
					return nil
				}

				continue
			}

			return fmt.Errorf("outbox fetch failed: %w", err)
		}

		if err := r.processBatch(workCtx, batch); err != nil {
			return err
		}
	}
}

func (r *Relay) fetchBatch(ctx context.Context) (Batch, error) {
	return r.consumer.Fetch(ctx, FetchOptions{BatchSize: r.cfg.BatchSize})
}

func (r *Relay) processBatch(ctx context.Context, batch Batch) (err error) {
	start := time.Now()
	defer func() {
		r.cfg.Metrics.ObserveBatchDuration(time.Since(start))
	}()

	if batch == nil {
		return ErrNilBatch
	}
	committed := false
	defer func() {
		value := recover()
		if !committed {
			if rollbackErr := batch.Rollback(); rollbackErr != nil {
				err = errors.Join(err, fmt.Errorf("outbox rollback failed: %w", rollbackErr))
			}
		}
		if value != nil {
			if err != nil {
				panic(errors.Join(panicCause(value), err))
			}
			panic(value)
		}
	}()

	records := batch.Records()
	if len(records) == 0 {
		return ErrEmptyBatch
	}

	outcome, err := r.collectBatchResults(ctx, records)
	if err != nil {
		return err
	}

	if err := r.applyBatchResults(ctx, batch, outcome); err != nil {
		return err
	}
	if err := batch.Commit(); err != nil {
		return fmt.Errorf("outbox commit failed: %w", err)
	}
	committed = true

	r.cfg.Metrics.AddProcessed(len(outcome.successful))
	r.cfg.Metrics.AddErrors(len(outcome.failed) + len(outcome.dead))
	r.cfg.Metrics.AddRetries(len(outcome.failed))
	r.cfg.Metrics.AddDead(len(outcome.dead))

	return nil
}

func (r *Relay) collectBatchResults(ctx context.Context, records []Record) (batchOutcome, error) {
	outcome := batchOutcome{
		successful: make([]ID, 0, len(records)),
		failed:     make([]Failure, 0),
		dead:       make([]Failure, 0),
	}
	for i := range records {
		if err := ctx.Err(); err != nil {
			return outcome, fmt.Errorf("outbox batch interrupted: %w", err)
		}
		record := records[i]
		handleCtx := ctx
		cancel := func() {}
		if r.cfg.HandlerTimeout > 0 {
			handleCtx, cancel = context.WithTimeout(ctx, r.cfg.HandlerTimeout)
		}
		panicked, handlerErr := r.invokeHandler(handleCtx, record)
		timedOut := r.cfg.HandlerTimeout > 0 && errors.Is(handleCtx.Err(), context.DeadlineExceeded)
		cancel()

		if handlerErr != nil {
			if ctx.Err() != nil {
				return outcome, errors.Join(
					fmt.Errorf("outbox handler failed for record %s: %w", record.ID, handlerErr),
					fmt.Errorf("outbox batch interrupted: %w", ctx.Err()),
				)
			}

			persistedErr := r.failureDetail(handlerErr, panicked, timedOut)
			r.recordFailure(ctx, record, handlerErr, persistedErr, &outcome)

			continue
		}
		outcome.successful = append(outcome.successful, record.ID)
	}

	return outcome, nil
}

func (r *Relay) failureDetail(err error, panicked, timedOut bool) error {
	if r.cfg.ErrorDetails {
		return err
	}
	if panicked {
		return errHandlerPanicked
	}
	if timedOut {
		return errHandlerTimedOut
	}

	return errHandlerFailed
}

func (r *Relay) invokeHandler(ctx context.Context, record Record) (panicked bool, err error) {
	defer func() {
		if value := recover(); value != nil {
			err = recoveredError(errHandlerPanicked, value)
			panicked = true
		}
	}()

	return false, r.handler.Handle(ctx, record)
}

func (r *Relay) recordFailure(
	ctx context.Context,
	record Record,
	handlerErr error,
	persistedErr error,
	outcome *batchOutcome,
) {
	if r.cfg.ErrorHandler != nil {
		r.cfg.ErrorHandler(ctx, record, handlerErr)
	}

	action := r.cfg.FailureClassifier(ctx, record, handlerErr)
	failure := Failure{
		ID:  record.ID,
		Err: persistedErr,
	}
	var retry *retryAfterError
	if errors.As(handlerErr, &retry) {
		failure.RetryDelay = retry.delay
	}
	if action == FailureDead {
		outcome.dead = append(outcome.dead, failure)

		return
	}
	outcome.failed = append(outcome.failed, failure)
}

func (r *Relay) applyBatchResults(ctx context.Context, batch Batch, outcome batchOutcome) error {
	if len(outcome.successful) > 0 {
		if err := batch.Ack(ctx, outcome.successful); err != nil {
			return fmt.Errorf("outbox ack failed: %w", err)
		}
	}
	if len(outcome.failed) > 0 {
		if err := batch.Fail(ctx, outcome.failed); err != nil {
			return fmt.Errorf("outbox fail update failed: %w", err)
		}
	}
	if len(outcome.dead) > 0 {
		if err := r.handleDead(ctx, batch, outcome.dead); err != nil {
			return err
		}
	}

	return nil
}

func (r *Relay) handleDead(ctx context.Context, batch Batch, dead []Failure) error {
	deadBatch, ok := batch.(DeadBatch)
	if ok {
		if err := deadBatch.Dead(ctx, dead); err != nil {
			return fmt.Errorf("outbox dead-letter update failed: %w", err)
		}

		return nil
	}

	r.cfg.Logger.Warn("outbox batch does not support dead-lettering; falling back to retry",
		"event", "relay.dead_letter_unsupported", "operation", "relay.dead_letter", "outcome", "unsupported",
		"count", len(dead),
	)
	if err := batch.Fail(ctx, dead); err != nil {
		return fmt.Errorf("outbox dead-letter fallback failed: %w", err)
	}

	return nil
}

func (r *Relay) reportWorkerPanic(workerID int, err error) {
	defer func() {
		_ = recover()
	}()

	r.cfg.Logger.Error("outbox worker panic",
		"event", "relay.worker_panicked", "operation", "relay.run", "outcome", "failed",
		"worker", workerID, "err", err,
	)
}

func recoveredError(kind error, value any) error {
	if cause, ok := value.(error); ok {
		return fmt.Errorf("%w: %w", kind, cause)
	}

	return fmt.Errorf("%w: %v", kind, value)
}

func panicCause(value any) error {
	if cause, ok := value.(error); ok {
		return fmt.Errorf("panic: %w", cause)
	}

	return fmt.Errorf("panic: %v", value) //nolint:err113 // A recovered panic value is dynamic.
}

func (r *Relay) sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return true
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (r *Relay) maybeRecordPending(ctx context.Context) {
	counter, ok := r.consumer.(PendingCounter)
	if !ok {
		return
	}
	if r.cfg.PendingInterval <= 0 {
		return
	}
	if ctx.Err() != nil {
		return
	}

	now := r.cfg.Clock.Now()
	r.pendingMu.Lock()
	nextAllowed := r.pendingAt.Add(r.cfg.PendingInterval)
	if !r.pendingAt.IsZero() && now.Before(nextAllowed) {
		r.pendingMu.Unlock()

		return
	}
	r.pendingAt = now
	r.pendingMu.Unlock()

	count, err := counter.PendingCount(ctx)
	if err != nil {
		r.cfg.Logger.Warn("outbox pending count failed",
			"event", "relay.pending_count_failed", "operation", "relay.pending_count", "outcome", "failed",
			"err", err,
		)

		return
	}

	r.cfg.Metrics.SetPending(count)
}
