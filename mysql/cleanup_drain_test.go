package mysql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox/mysql"
)

func TestCleanupMaintainerDrain(t *testing.T) {
	failure := errors.New("cleanup storage failure")
	releaseFailure := errors.New("release failure")
	tests := []struct {
		name           string
		timeout        time.Duration
		counts         []int64
		failure        error
		releaseFailure error
		total          int64
	}{
		{
			name:   "default single batch",
			counts: []int64{2},
			total:  2,
		},
		{
			name:    "drain through short batch",
			timeout: time.Minute,
			counts:  []int64{2, 2, 1},
			total:   5,
		},
		{
			name:    "exact multiple needs empty batch",
			timeout: time.Minute,
			counts:  []int64{2, 2, 0},
			total:   4,
		},
		{
			name:           "later failure retains earlier totals and release cause",
			timeout:        time.Minute,
			counts:         []int64{2, 2},
			failure:        failure,
			releaseFailure: releaseFailure,
			total:          4,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			calls := 0
			acquired, released := 0, 0
			var cutoff driver.Value
			connector := maintenanceConnector{
				releaseErr: test.releaseFailure,
				onQuery: func(ctx context.Context, query string) {
					require.NoError(t, ctx.Err())
					if strings.Contains(query, "GET_LOCK") {
						acquired++
					}
					if strings.Contains(query, "RELEASE_LOCK") {
						released++
					}
				},
				exec: func(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
					require.EqualValues(t, 2, args[2].Value)
					if calls == 0 {
						cutoff = args[1].Value
					} else {
						require.Equal(t, cutoff, args[1].Value)
					}
					if calls == len(test.counts) {
						require.Error(t, test.failure, "unexpected extra batch")
						calls++
						return nil, test.failure
					}
					count := test.counts[calls]
					calls++
					return driver.RowsAffected(count), nil
				},
			}
			db := sql.OpenDB(connector)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			logger := &maintenanceLogger{}
			maintainer, err := mysql.NewCleanupMaintainer(
				db,
				mysql.CleanupMaintainerConfig{
					Retention:    time.Hour,
					Limit:        2,
					DrainTimeout: test.timeout,
					Logger:       logger,
				},
			)
			require.NoError(t, err)
			result, err := maintainer.Ensure(context.Background())
			require.EqualValues(t, test.total, result.Processed)
			require.Zero(t, result.Dead)
			if test.failure != nil {
				require.ErrorIs(t, err, test.failure)
				require.ErrorIs(t, err, test.releaseFailure)
				require.Len(t, logger.events, 1)
				require.Same(t, err, logger.events[0]["err"])
				require.Equal(t, "operation_and_release", logger.events[0]["stage"])
				require.Equal(t, "cleanup.failed", logger.events[0]["event"])
				require.Equal(t, len(test.counts)+1, calls)
			} else {
				require.NoError(t, err)
				require.Equal(t, []string{"cleanup.completed"}, maintenanceEvents(logger))
				require.Equal(t, len(test.counts), calls)
			}
			require.Equal(t, 1, acquired)
			require.Equal(t, 1, released)
		})
	}
}

func TestCleanupMaintainerDrainBudgetAndCallerCancellation(t *testing.T) {
	for _, parentStops := range []bool{false, true} {
		t.Run(map[bool]string{false: "budget completion", true: "parent cancellation"}[parentStops], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				parent, cancel := context.WithCancel(context.Background())
				defer cancel()
				calls := 0
				released := false
				connector := maintenanceConnector{
					onQuery: func(ctx context.Context, query string) {
						if strings.Contains(query, "RELEASE_LOCK") {
							require.NoError(t, ctx.Err())
							released = true
						}
					},
					exec: func(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
						calls++
						if calls == 1 {
							return driver.RowsAffected(2), nil
						}
						if parentStops {
							cancel()
							return nil, ctx.Err()
						}
						time.Sleep(2 * time.Second)
						if err := ctx.Err(); err != nil {
							return nil, err
						}
						return driver.RowsAffected(2), nil
					},
				}
				db := sql.OpenDB(connector)
				defer db.Close()
				logger := &maintenanceLogger{}
				maintainer, err := mysql.NewCleanupMaintainer(
					db,
					mysql.CleanupMaintainerConfig{
						Retention:    time.Hour,
						Limit:        2,
						DrainTimeout: time.Second,
						Logger:       logger,
					},
				)
				require.NoError(t, err)
				result, err := maintainer.Ensure(parent)
				require.Equal(t, 2, calls)
				require.True(t, released)
				require.Len(t, logger.events, 1)
				if parentStops {
					require.ErrorIs(t, err, context.Canceled)
					require.EqualValues(t, 2, result.Processed)
					require.Equal(t, "cleanup.failed", logger.events[0]["event"])
					require.Same(t, err, logger.events[0]["err"])
				} else {
					require.NoError(t, err)
					require.EqualValues(t, 4, result.Processed)
					require.Equal(t, "cleanup.completed", logger.events[0]["event"])
					require.Equal(t, "succeeded", logger.events[0]["outcome"])
					require.Equal(t, "budget_exhausted", logger.events[0]["reason"])
					require.NotContains(t, logger.events[0], "err")
				}
			})
		})
	}
}

func TestCleanupMaintainerDrainTimeoutValidation(t *testing.T) {
	_, err := mysql.NewCleanupMaintainer(
		&sql.DB{},
		mysql.CleanupMaintainerConfig{
			Retention:    time.Hour,
			DrainTimeout: -time.Second,
		},
	)
	require.ErrorIs(t, err, mysql.ErrCleanupDrainTimeoutInvalid)
}

func TestCleanupMaintainerDrainRunsFirstBatchAfterConnectionWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		db := sql.OpenDB(maintenanceConnector{
			exec: func(ctx context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
				require.NoError(t, ctx.Err())
				require.EqualValues(t, 2, args[2].Value)
				calls++
				return driver.RowsAffected(2), nil
			},
		})
		defer db.Close()
		db.SetMaxOpenConns(1)
		held, err := db.Conn(context.Background())
		require.NoError(t, err)
		closed := make(chan error, 1)
		go func() {
			time.Sleep(2 * time.Second)
			closed <- held.Close()
		}()
		logger := &maintenanceLogger{}
		maintainer, err := mysql.NewCleanupMaintainer(db, mysql.CleanupMaintainerConfig{
			Retention:    time.Hour,
			Limit:        2,
			DrainTimeout: time.Second,
			Logger:       logger,
		})
		require.NoError(t, err)
		result, err := maintainer.Ensure(context.Background())
		require.NoError(t, <-closed)
		require.NoError(t, err)
		require.Equal(t, 1, calls)
		require.Equal(t, mysql.CleanupResult{
			Processed: 2,
		}, result)
		require.Equal(t, []string{"cleanup.completed"}, maintenanceEvents(logger))
		require.Equal(t, "succeeded", logger.events[0]["outcome"])
		require.Equal(t, "budget_exhausted", logger.events[0]["reason"])
		require.NotContains(t, logger.events[0], "err")
	})
}
