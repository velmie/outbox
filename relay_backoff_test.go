package outbox

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"
)

func TestRelayStorageErrorBackoffRecovery(t *testing.T) {
	synctest.Test(
		t,
		func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source := &diagnosticError{}
			logger := &fieldLogger{}
			batch := &fakeBatch{
				records: []Record{
					{
						ID: ID{1},
					},
				},
			}
			var fetched []time.Time
			consumer := fetchConsumerFunc(
				func(context.Context, FetchOptions) (Batch, error) {
					fetched = append(fetched, time.Now())
					switch len(fetched) {
					case 1, 2, 3, 4, 6, 8:
						return nil, source
					case 5:
						return nil, ErrNoRecords
					case 7:
						return batch, nil
					default:
						cancel()
						return nil, ErrNoRecords
					}
				},
			)
			handled := 0
			relay := NewRelay(
				consumer,
				HandlerFunc(
					func(context.Context, Record) error {
						handled++
						return nil
					},
				),
				WithStorageErrorBackoff(time.Second, 3*time.Second),
				WithPollInterval(100*time.Millisecond),
				WithLogger(logger),
			)
			if err := relay.Run(ctx); err != nil {
				t.Fatal(err)
			}
			want := []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second, 100 * time.Millisecond, time.Second, 0, time.Second}
			checkCallIntervals(t, fetched, want)
			if handled != 1 || !batch.committed {
				t.Fatalf("handled=%d committed=%v", handled, batch.committed)
			}
			entries := logger.snapshot()
			if len(entries) != 6 {
				t.Fatalf("logs=%d, want 6", len(entries))
			}
			for _, entry := range entries {
				err, _ := entry["err"].(error)
				var cause *diagnosticError
				if !errors.Is(err, source) || !errors.As(err, &cause) || cause != source {
					t.Fatalf("lost cause: %v", entry)
				}
				if entry["event"] != "relay.storage_error" || entry["operation"] != "relay.fetch" || entry["outcome"] != "retrying" || entry["worker"] != 0 {
					t.Fatalf("unexpected diagnostic: %v", entry)
				}
			}
		},
	)
}

func TestRelayStorageBatchRecovery(t *testing.T) {
	source := errors.New("storage unavailable")
	cleanup := errors.New("rollback unavailable")
	for _, stage := range []string{"ack", "fail", "dead", "fallback", "commit", "rollback"} {
		t.Run(
			stage,
			func(t *testing.T) {
				synctest.Test(
					t,
					func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						records := []Record{
							{
								ID: ID{1},
							},
						}
						failed := &fakeBatch{
							records: records,
						}
						switch stage {
						case "ack":
							failed.ackErr = source
						case "fail", "fallback":
							failed.failErr = source
						case "dead":
							failed.deadErr = source
						case "commit":
							failed.commitErr = source
						case "rollback":
							failed.ackErr, failed.rollErr = source, cleanup
						}
						var first Batch = failed
						if stage == "fallback" {
							first = &fakeBatchNoDead{
								records: records,
								failErr: source,
							}
						}
						recovered := &fakeBatch{
							records: records,
						}
						calls := 0
						consumer := fetchConsumerFunc(
							func(context.Context, FetchOptions) (Batch, error) {
								calls++
								if calls == 1 {
									return first, nil
								}
								if calls == 2 {
									return recovered, nil
								}
								cancel()
								return nil, ErrNoRecords
							},
						)
						var handled []ID
						handler := HandlerFunc(
							func(_ context.Context, record Record) error {
								handled = append(handled, record.ID)
								if stage == "fail" || stage == "dead" || stage == "fallback" {
									return errors.New("delivery failed")
								}
								return nil
							},
						)
						logger := &fieldLogger{}
						relay := NewRelay(
							consumer,
							handler,
							WithStorageErrorBackoff(time.Second, time.Minute),
							WithLogger(logger),

							WithFailureClassifier(
								func(context.Context, Record, error) FailureAction {
									if stage == "dead" || stage == "fallback" {
										return FailureDead
									}
									return FailureRetry
								},
							),
						)
						if err := relay.Run(ctx); err != nil {
							t.Fatal(err)
						}
						if !reflect.DeepEqual(handled, []ID{ID{1}, ID{1}}) || !recovered.committed {
							t.Fatalf("handled=%v committed=%v", handled, recovered.committed)
						}
						if stage != "fallback" && failed.rollCalls != 1 {
							t.Fatalf("rollback calls=%d", failed.rollCalls)
						}
						found := false
						for _, entry := range logger.snapshot() {
							if entry["event"] != "relay.storage_error" {
								continue
							}
							found = true
							err, _ := entry["err"].(error)
							if !errors.Is(err, source) || (stage == "rollback" && !errors.Is(err, cleanup)) {
								t.Fatalf("lost cause: %v", err)
							}
						}
						if !found {
							t.Fatal("missing storage diagnostic")
						}
					},
				)
			},
		)
	}
}

func TestRelayBackoffCancellation(t *testing.T) {
	for _, storage := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "idle", true: "storage"}[storage],
			func(t *testing.T) {
				synctest.Test(
					t,
					func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						source := ErrNoRecords
						if storage {
							source = errors.New("database down")
						}
						relay := NewRelay(
							staticConsumer{
								err: source,
							},
							HandlerFunc(func(context.Context, Record) error { return nil }),

							WithPollInterval(time.Hour),
							WithIdleBackoff(2*time.Hour),
							WithStorageErrorBackoff(time.Hour, 2*time.Hour),
						)
						done := make(chan error, 1)
						go func() { done <- relay.Run(ctx) }()
						synctest.Wait()
						start := time.Now()
						cancel()
						if err := <-done; err != nil {
							t.Fatal(err)
						}
						if elapsed := time.Since(start); elapsed != 0 {
							t.Fatalf("cancellation waited %s", elapsed)
						}
					},
				)
			},
		)
	}
}

func TestRelayIdleBackoff(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "default", true: "enabled"}[enabled],
			func(t *testing.T) {
				synctest.Test(
					t,
					func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						defer cancel()
						var fetched []time.Time
						consumer := fetchConsumerFunc(
							func(context.Context, FetchOptions) (Batch, error) {
								fetched = append(fetched, time.Now())
								if len(fetched) == 5 {
									return &fakeBatch{
										records: []Record{
											{
												ID: ID{1},
											},
										},
									}, nil
								}
								if len(fetched) == 8 {
									cancel()
								}
								return nil, ErrNoRecords
							},
						)
						opts := []RelayOption{WithPollInterval(time.Second)}
						if enabled {
							opts = append(opts, WithIdleBackoff(3*time.Second))
						}
						relay := NewRelay(consumer, HandlerFunc(func(context.Context, Record) error { return nil }), opts...)
						if err := relay.Run(ctx); err != nil {
							t.Fatal(err)
						}
						want := []time.Duration{time.Second, time.Second, time.Second, time.Second, 0, time.Second, time.Second}
						if enabled {
							want = []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second, 0, time.Second, 2 * time.Second}
						}
						checkCallIntervals(t, fetched, want)
					},
				)
			},
		)
	}
}

func TestRelayBackoffContractErrorsRemainFatal(t *testing.T) {
	for _, source := range []error{ErrNilBatch, ErrEmptyBatch, ErrInvalidBatchSize, ErrInvalidRetryPolicy, ErrInvalidID} {
		t.Run(
			source.Error(),
			func(t *testing.T) {
				synctest.Test(
					t,
					func(t *testing.T) {
						calls := 0
						consumer := fetchConsumerFunc(
							func(context.Context, FetchOptions) (Batch, error) {
								calls++
								if calls > 1 {
									t.Error("contract error retried")
									return nil, ErrInvalidBatchSize
								}
								switch source {
								case ErrNilBatch:
									return nil, nil
								case ErrEmptyBatch:
									return &fakeBatch{
										rollErr: errors.New("cleanup failed"),
									}, nil
								case ErrInvalidRetryPolicy:
									return &fakeBatch{
										records: []Record{
											{
												ID: ID{1},
											},
										},
										ackErr: source,
									}, nil
								default:
									return nil, source
								}
							},
						)
						relay := NewRelay(consumer, HandlerFunc(func(context.Context, Record) error { return nil }), WithStorageErrorBackoff(time.Second, time.Minute))
						if err := relay.Run(context.Background()); !errors.Is(err, source) {
							t.Fatalf("got %v, want %v", err, source)
						}
					},
				)
			},
		)
	}
}

func TestRelayStorageBackoffWorkerIsolation(t *testing.T) {
	synctest.Test(
		t,
		func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			batch := &fakeBatch{
				records: []Record{
					{
						ID: ID{1},
					},
				},
			}
			calls := make(chan int, 3)
			calls <- 1
			calls <- 2
			calls <- 3
			consumer := fetchConsumerFunc(
				func(context.Context, FetchOptions) (Batch, error) {
					switch <-calls {
					case 1:
						return nil, errors.New("one worker lost its connection")
					case 2:
						return batch, nil
					default:
						return nil, ErrNoRecords
					}
				},
			)
			entered := make(chan struct{})
			release := make(chan struct{})
			relay := NewRelay(
				consumer,
				HandlerFunc(
					func(ctx context.Context, _ Record) error {
						close(entered)
						select {
						case <-ctx.Done():
							return ctx.Err()
						case <-release:
							return nil
						}
					},
				),
				WithWorkers(2),
				WithStorageErrorBackoff(time.Hour, time.Hour),
				WithPollInterval(time.Hour),
			)
			done := make(chan error, 1)
			go func() { done <- relay.Run(ctx) }()
			<-entered
			synctest.Wait()
			close(release)
			synctest.Wait()
			if !batch.committed {
				t.Fatal("healthy worker did not commit while the other waited")
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		},
	)
}

func TestRelayStorageBackoffCommitFailuresGrowAndReset(t *testing.T) {
	synctest.Test(
		t,
		func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			calls := 0
			consumer := fetchConsumerFunc(
				func(context.Context, FetchOptions) (Batch, error) {
					calls++
					var commitErr error
					if calls != 5 && calls != 7 {
						commitErr = errors.New("commit failed")
					}
					return &fakeBatch{
						records: []Record{
							{
								ID: ID{1},
							},
						},
						commitErr: commitErr,
					}, nil
				},
			)
			var handled []time.Time
			relay := NewRelay(
				consumer,
				HandlerFunc(
					func(context.Context, Record) error {
						handled = append(handled, time.Now())
						if len(handled) == 7 {
							cancel()
						}
						return nil
					},
				),
				WithStorageErrorBackoff(time.Second, 3*time.Second),
			)
			if err := relay.Run(ctx); err != nil {
				t.Fatal(err)
			}
			checkCallIntervals(t, handled, []time.Duration{time.Second, 2 * time.Second, 3 * time.Second, 3 * time.Second, 0, time.Second})
		},
	)
}

func TestRelayStorageBackoffDoesNotChangeProcessOnce(t *testing.T) {
	synctest.Test(
		t,
		func(t *testing.T) {
			source := errors.New("connection lost")
			relay := NewRelay(
				staticConsumer{
					err: source,
				},
				HandlerFunc(func(context.Context, Record) error { return nil }),
				WithStorageErrorBackoff(time.Hour, time.Hour),
				WithIdleBackoff(time.Hour),
			)
			start := time.Now()
			processed, err := relay.ProcessOnce(context.Background())
			if processed || !errors.Is(err, source) || time.Since(start) != 0 {
				t.Fatalf("processed=%v err=%v elapsed=%s", processed, err, time.Since(start))
			}
		},
	)
}

func TestRelayStorageBackoffPreservesShutdownFailure(t *testing.T) {
	synctest.Test(
		t,
		func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			source := errors.New("commit failed during drain")
			batch := &fakeBatch{
				records: []Record{
					{
						ID: ID{1},
					},
				},
				commitErr: source,
			}
			relay := NewRelay(
				staticConsumer{
					batch: batch,
				},
				HandlerFunc(
					func(context.Context, Record) error {
						cancel()
						return nil
					},
				),
				WithStorageErrorBackoff(time.Hour, time.Hour),
			)
			if err := relay.Run(ctx); !errors.Is(err, source) {
				t.Fatalf("drain error lost: %v", err)
			}
			if batch.rollCalls != 1 {
				t.Fatalf("rollback calls=%d", batch.rollCalls)
			}
		},
	)
}

func TestRelayStorageBackoffDisabled(t *testing.T) {
	for _, option := range []RelayOption{WithStorageErrorBackoff(0, time.Second), WithStorageErrorBackoff(-time.Second, time.Second)} {
		source := errors.New("storage failed")
		relay := NewRelay(
			staticConsumer{
				err: source,
			},
			HandlerFunc(func(context.Context, Record) error { return nil }),
			option,
		)
		if err := relay.Run(context.Background()); !errors.Is(err, source) {
			t.Fatalf("got %v, want storage failure", err)
		}
	}
}

func TestRelayBackoffBounds(t *testing.T) {
	synctest.Test(
		t,
		func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var fetched []time.Time
			consumer := fetchConsumerFunc(
				func(context.Context, FetchOptions) (Batch, error) {
					fetched = append(fetched, time.Now())
					if len(fetched) == 4 {
						cancel()
						return nil, ErrNoRecords
					}
					return nil, errors.New("database down")
				},
			)
			relay := NewRelay(consumer, HandlerFunc(func(context.Context, Record) error { return nil }), WithStorageErrorBackoff(time.Second, 0))
			if err := relay.Run(ctx); err != nil {
				t.Fatal(err)
			}
			checkCallIntervals(t, fetched, []time.Duration{time.Second, time.Second, time.Second})
		},
	)
}

type fetchConsumerFunc func(context.Context, FetchOptions) (Batch, error)

func (f fetchConsumerFunc) Fetch(ctx context.Context, opts FetchOptions) (Batch, error) {
	return f(ctx, opts)
}

func checkCallIntervals(t *testing.T, calls []time.Time, want []time.Duration) {
	t.Helper()
	var got []time.Duration
	for i := 1; i < len(calls); i++ {
		got = append(got, calls[i].Sub(calls[i-1]))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("call intervals=%v, want %v", got, want)
	}
}
