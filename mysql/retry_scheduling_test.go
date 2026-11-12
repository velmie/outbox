package mysql

import (
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox"
)

func TestWithRetryScheduling(t *testing.T) {
	tests := []struct {
		name    string
		options []Option
		delay   time.Duration
	}{
		{
			name:    "schema only",
			options: []Option{WithRetryScheduling()},
		},
		{
			name:    "zero delay retains explicit scheduling",
			options: []Option{WithRetryScheduling(), WithRetryDelay(0)},
		},
		{
			name:    "fixed delay before",
			options: []Option{WithRetryDelay(time.Second), WithRetryScheduling()},
			delay:   time.Second,
		},
		{
			name:    "fixed delay after",
			options: []Option{WithRetryScheduling(), WithRetryDelay(time.Second)},
			delay:   time.Second,
		},
		{
			name:    "schedule retained",
			options: []Option{WithRetrySchedule(func(int) time.Duration { return time.Minute }), WithRetryScheduling()},
			delay:   time.Minute,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store, err := NewStore(&sql.DB{}, test.options...)
			require.NoError(t, err)
			delay, err := store.retryDelay(outbox.Failure{}, 1)
			require.NoError(t, err)
			require.Equal(t, test.delay, delay)
			delay, err = store.retryDelay(
				outbox.Failure{
					RetryDelay: time.Hour,
				},
				1,
			)
			require.NoError(t, err)
			require.Equal(t, time.Hour, delay)
		})
	}
}
