//go:build integration

package mysql_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

func TestRelayRetryHintPersistsWithGenericError(t *testing.T) {
	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, container.Terminate(ctx))
	})
	resetRetrySchema(t, ctx, db)
	store, err := mysql.NewStore(db, mysql.WithRetrySchedule(func(int) time.Duration {
		return time.Minute
	}))
	require.NoError(t, err)
	insertEntries(t, ctx, db, store, []outbox.Entry{
		validIntegrationEntry("delivery"),
	})
	var observed outbox.ID
	relay := outbox.NewRelay(store, outbox.HandlerFunc(func(_ context.Context, record outbox.Record) error {
		observed = record.ID
		return outbox.RetryAfter(errors.New("private detail"), time.Hour)
	}))
	processed, err := relay.ProcessOnce(ctx)
	require.NoError(t, err)
	require.True(t, processed)
	state := readRetryState(t, ctx, db, observed)
	require.Equal(t, outbox.StatusPending, state.status)
	require.Equal(t, 1, state.attempts)
	require.Equal(t, "outbox handler failed", state.lastError.String)
	var earliest time.Time
	require.NoError(t, db.QueryRowContext(ctx, "SELECT DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 59 MINUTE)").Scan(&earliest))
	require.True(t, state.deadline.Valid)
	require.True(t, state.deadline.Time.After(earliest))
	restarted, err := mysql.NewStore(db, mysql.WithRetrySchedule(func(int) time.Duration {
		return 0
	}))
	require.NoError(t, err)
	requireNoRetryRecords(t, ctx, restarted)

	resetRetrySchema(t, ctx, db)
	var attempts []int
	scheduled, err := mysql.NewStore(db, mysql.WithMaxAttempts(3), mysql.WithRetrySchedule(func(attempt int) time.Duration {
		attempts = append(attempts, attempt)
		return time.Duration(attempt) * time.Microsecond
	}))
	require.NoError(t, err)
	insertEntries(t, ctx, db, scheduled, []outbox.Entry{
		validIntegrationEntry("attempts"),
	})
	for i := 1; i <= 3; i++ {
		batch, fetchErr := scheduled.Fetch(ctx, outbox.FetchOptions{
			BatchSize: 1,
		})
		require.NoError(t, fetchErr)
		id := batch.Records()[0].ID
		var hint time.Duration
		if i == 3 {
			hint = -time.Second
		}
		require.NoError(t, batch.Fail(ctx, []outbox.Failure{{
			ID:         id,
			Err:        errors.New("retry"),
			RetryDelay: hint,
		}}))
		require.NoError(t, batch.Commit())
		state = readRetryState(t, ctx, db, id)
		require.Equal(t, i, state.attempts)
		if i == 3 {
			require.Equal(t, outbox.StatusDead, state.status)
		} else {
			require.Equal(t, outbox.StatusPending, state.status)
			makeRetryDue(t, ctx, db, id)
		}
	}
	require.Equal(t, []int{1, 2}, attempts)

	resetRetrySchema(t, ctx, db)
	repeated, err := mysql.NewStore(db, mysql.WithMaxAttempts(2))
	require.NoError(t, err)
	insertEntries(t, ctx, db, repeated, []outbox.Entry{
		validIntegrationEntry("repeated"),
	})
	repeatedBatch, err := repeated.Fetch(ctx, outbox.FetchOptions{
		BatchSize: 1,
	})
	require.NoError(t, err)
	repeatedID := repeatedBatch.Records()[0].ID
	require.NoError(t, repeatedBatch.Fail(ctx, []outbox.Failure{{
		ID: repeatedID,
	}}))
	require.NoError(t, repeatedBatch.Fail(ctx, []outbox.Failure{{
		ID:         repeatedID,
		RetryDelay: time.Second,
	}}))
	require.NoError(t, repeatedBatch.Commit())
	repeatedState := readRetryState(t, ctx, db, repeatedID)
	require.Equal(t, 2, repeatedState.attempts)
	require.Equal(t, outbox.StatusDead, repeatedState.status)
	resetRetrySchema(t, ctx, db)
	attempts = nil
	repeated, err = mysql.NewStore(db, mysql.WithMaxAttempts(3), mysql.WithRetrySchedule(func(attempt int) time.Duration {
		attempts = append(attempts, attempt)
		return 0
	}))
	require.NoError(t, err)
	insertEntries(t, ctx, db, repeated, []outbox.Entry{
		validIntegrationEntry("repeated-scheduled"),
	})
	repeatedBatch, err = repeated.Fetch(ctx, outbox.FetchOptions{
		BatchSize: 1,
	})
	require.NoError(t, err)
	repeatedID = repeatedBatch.Records()[0].ID
	for i := 0; i < 2; i++ {
		require.NoError(t, repeatedBatch.Fail(ctx, []outbox.Failure{{
			ID: repeatedID,
		}}))
	}
	require.Zero(t, repeatedBatch.Records()[0].Attempts)
	require.NoError(t, repeatedBatch.Commit())
	require.Equal(t, []int{1, 2}, attempts)

	resetRetrySchema(t, ctx, db)
	unscheduled, err := mysql.NewStore(db)
	require.NoError(t, err)
	insertEntries(t, ctx, db, unscheduled, []outbox.Entry{
		validIntegrationEntry("no-scheduling"),
	})
	var unscheduledID outbox.ID
	metrics := &committedCounts{}
	unscheduledRelay := outbox.NewRelay(
		unscheduled,
		outbox.HandlerFunc(func(_ context.Context, record outbox.Record) error {
			unscheduledID = record.ID
			return outbox.RetryAfter(errors.New("temporary"), time.Second)
		}),
		outbox.WithMetrics(metrics),
	)
	processed, err = unscheduledRelay.ProcessOnce(ctx)
	require.False(t, processed)
	require.ErrorContains(t, err, "retry delay requires retry scheduling (next_attempt_at)")
	require.ErrorIs(t, err, outbox.ErrInvalidRetryPolicy)
	state = readRetryState(t, ctx, db, unscheduledID)
	require.Equal(t, outbox.StatusPending, state.status)
	require.Zero(t, state.attempts)
	require.False(t, state.lastError.Valid)
	require.False(t, state.deadline.Valid)
	require.Zero(t, metrics.processed)
	require.Zero(t, metrics.failures)
	require.Zero(t, metrics.retries)
	require.Zero(t, metrics.dead)

	var calls int
	logger := &workerLogger{}
	unscheduledRelay = outbox.NewRelay(
		unscheduled,
		outbox.HandlerFunc(func(_ context.Context, _ outbox.Record) error {
			calls++
			return outbox.RetryAfter(errors.New("temporary"), time.Second)
		}),
		outbox.WithLogger(logger),
	)
	err = unscheduledRelay.Run(ctx)
	require.ErrorContains(t, err, "retry delay requires retry scheduling (next_attempt_at)")
	require.ErrorIs(t, err, outbox.ErrInvalidRetryPolicy)
	require.Equal(t, 1, calls)
	require.Len(t, logger.errors, 1)
	require.ErrorContains(t, logger.errors[0], unscheduledID.String())
	require.ErrorContains(t, logger.errors[0], "retry delay requires retry scheduling (next_attempt_at)")
	require.Zero(t, readRetryState(t, ctx, db, unscheduledID).attempts)

	terminal := outbox.NewRelay(unscheduled, outbox.HandlerFunc(func(context.Context, outbox.Record) error {
		return outbox.RetryAfter(errors.New("permanent"), -time.Second)
	}), outbox.WithFailureClassifier(func(context.Context, outbox.Record, error) outbox.FailureAction {
		return outbox.FailureDead
	}))
	processed, err = terminal.ProcessOnce(ctx)
	require.NoError(t, err)
	require.True(t, processed)
	require.Equal(t, outbox.StatusDead, readRetryState(t, ctx, db, unscheduledID).status)

	for _, test := range []struct {
		name     string
		schedule func(int) time.Duration
		hint     time.Duration
		want     string
	}{
		{
			name:     "negative hint",
			schedule: func(int) time.Duration { return 0 },
			hint:     -time.Nanosecond,
			want:     "retry delay hint must be nonnegative",
		},
		{
			name:     "negative schedule",
			schedule: func(int) time.Duration { return -time.Nanosecond },
			want:     "retry schedule must return a nonnegative delay",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			resetRetrySchema(t, ctx, db)
			candidate, createErr := mysql.NewStore(db, mysql.WithRetrySchedule(test.schedule))
			require.NoError(t, createErr)
			insertEntries(t, ctx, db, candidate, []outbox.Entry{
				validIntegrationEntry(test.name),
			})
			batch, fetchErr := candidate.Fetch(ctx, outbox.FetchOptions{
				BatchSize: 1,
			})
			require.NoError(t, fetchErr)
			id := batch.Records()[0].ID
			failErr := batch.Fail(ctx, []outbox.Failure{{
				ID:         id,
				RetryDelay: test.hint,
			}})
			require.ErrorContains(t, failErr, test.want)
			require.ErrorIs(t, failErr, outbox.ErrInvalidRetryPolicy)
			require.NoError(t, batch.Rollback())
			require.Zero(t, readRetryState(t, ctx, db, id).attempts)
		})
	}

	resetRetrySchema(t, ctx, db)
	_, err = mysql.NewStore(db, mysql.WithRetryDelay(time.Second), mysql.WithRetrySchedule(func(int) time.Duration {
		return 0
	}))
	require.ErrorContains(t, err, "cannot be combined")
	rounded, err := mysql.NewStore(db, mysql.WithRetrySchedule(func(int) time.Duration {
		return time.Hour + time.Nanosecond
	}))
	require.NoError(t, err)
	insertEntries(t, ctx, db, rounded, []outbox.Entry{
		validIntegrationEntry("precision"),
	})
	batch, err := rounded.Fetch(ctx, outbox.FetchOptions{
		BatchSize: 1,
	})
	require.NoError(t, err)
	roundedID := batch.Records()[0].ID
	require.NoError(t, batch.Fail(ctx, []outbox.Failure{{
		ID: roundedID,
	}}))
	require.NoError(t, batch.Commit())
	var micros int64
	require.NoError(t, db.QueryRowContext(
		ctx,
		"SELECT TIMESTAMPDIFF(MICROSECOND, updated_at, next_attempt_at) FROM outbox WHERE id = ?",
		roundedID,
	).Scan(&micros))
	require.EqualValues(t, time.Hour/time.Microsecond+1, micros)

	resetRetrySchema(t, ctx, db)
	store, err = mysql.NewStore(db)
	require.NoError(t, err)
	insertEntries(t, ctx, db, store, []outbox.Entry{
		validIntegrationEntry("unicode"),
	})
	detail := strings.Repeat("🙂", 1023) + string([]byte{0xff}) + "tail"
	var detailID outbox.ID
	detailed := outbox.NewRelay(store, outbox.HandlerFunc(func(_ context.Context, record outbox.Record) error {
		detailID = record.ID
		return errors.New(detail)
	}), outbox.WithErrorDetails())
	processed, err = detailed.ProcessOnce(ctx)
	require.NoError(t, err)
	require.True(t, processed)
	require.Equal(t, strings.Repeat("🙂", 1023)+"�", readRetryState(t, ctx, db, detailID).lastError.String)
}

type committedCounts struct {
	outbox.NopMetrics
	processed int
	failures  int
	retries   int
	dead      int
}

func (m *committedCounts) AddProcessed(n int) { m.processed += n }
func (m *committedCounts) AddErrors(n int)    { m.failures += n }
func (m *committedCounts) AddRetries(n int)   { m.retries += n }
func (m *committedCounts) AddDead(n int)      { m.dead += n }

type workerLogger struct {
	outbox.NopLogger
	errors []error
}

func (l *workerLogger) Error(_ string, args ...any) {
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == "err" {
			if err, ok := args[i+1].(error); ok {
				l.errors = append(l.errors, err)
			}
		}
	}
}
