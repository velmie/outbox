package outbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// FailureHandler is called when record processing returns an error.
// It receives the original handler error. Implementations that log or persist
// the record or error are responsible for handling sensitive data safely.
// A handler panic is represented by a generic error without the panic value.
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

	var cfg RelayConfig
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

// Run starts the polling loop with the configured number of workers.
func (r *Relay) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, r.cfg.Workers)
	var wg sync.WaitGroup

	for i := 0; i < r.cfg.Workers; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			defer func() {
				if recover() != nil {
					errCh <- ErrWorkerPanic
					cancel()
					r.reportWorkerPanic(workerID)
				}
			}()

			if err := r.runWorker(ctx); err != nil && !errors.Is(err, context.Canceled) {
				r.cfg.Logger.Error("outbox worker error",
					"event", "relay.worker_failed", "operation", "relay.run", "outcome", "failed",
					"worker", workerID, "err", err,
				)
				errCh <- err
				cancel()
			}
		}()
	}

	wg.Wait()
	close(errCh)

	if err := <-errCh; err != nil {
		return err
	}
	if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
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

func (r *Relay) runWorker(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		batch, err := r.fetchBatch(ctx)
		if err != nil {
			if errors.Is(err, ErrNoRecords) {
				r.maybeRecordPending(ctx)
				if sleepErr := r.sleep(ctx, r.cfg.PollInterval); sleepErr != nil {
					return sleepErr
				}

				continue
			}

			return err
		}

		if err := r.processBatch(ctx, batch); err != nil {
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
		if committed {
			return
		}
		if rollbackErr := batch.Rollback(); rollbackErr != nil {
			err = errors.Join(err, fmt.Errorf("outbox rollback failed: %w", rollbackErr))
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
				return outcome, ctx.Err()
			}

			persistedErr := errHandlerFailed
			if panicked {
				persistedErr = errHandlerPanicked
			} else if timedOut {
				persistedErr = errHandlerTimedOut
			}
			r.recordFailure(ctx, record, handlerErr, persistedErr, &outcome)

			continue
		}
		outcome.successful = append(outcome.successful, record.ID)
	}

	return outcome, nil
}

func (r *Relay) invokeHandler(ctx context.Context, record Record) (panicked bool, err error) {
	defer func() {
		if recover() != nil {
			err = errHandlerPanicked
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
	if action == FailureDead {
		outcome.dead = append(outcome.dead, Failure{ID: record.ID, Err: persistedErr})

		return
	}
	outcome.failed = append(outcome.failed, Failure{ID: record.ID, Err: persistedErr})
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

func (r *Relay) reportWorkerPanic(workerID int) {
	defer func() {
		_ = recover()
	}()

	r.cfg.Logger.Error("outbox worker panic",
		"event", "relay.worker_panicked", "operation", "relay.run", "outcome", "failed",
		"worker", workerID,
	)
}

func (r *Relay) sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
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
