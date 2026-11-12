//go:build integration

package mysql_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

func TestRelayRecoversAfterDatabaseRestartIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test disabled in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	// Docker can change an automatically assigned host port on restart.
	// Select an explicit port so the existing connection pool keeps its endpoint.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	_, hostPort, err := net.SplitHostPort(listener.Addr().String())
	require.NoError(t, err)
	require.NoError(t, listener.Close())
	container, db := startMySQLContainer(
		t,
		ctx,
		func(config *container.HostConfig) {
			config.PortBindings = network.PortMap{
				network.MustParsePort("3306/tcp"): []network.PortBinding{
					{
						HostPort: hostPort,
					},
				},
			}
		},
	)
	t.Cleanup(
		func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
			if err := container.Terminate(context.Background()); err != nil {
				t.Error(err)
			}
		},
	)
	setupSchema(t, ctx, db)
	store, err := mysql.NewStore(db)
	require.NoError(t, err)
	insertEntries(t, ctx, db, store, []outbox.Entry{validIntegrationEntry("restart")})
	delivered := make(chan outbox.ID, 4)
	release := make(chan struct{})
	parent, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	logger := &recoveryLogger{
		failures: make(chan error, 32),
	}
	relay := outbox.NewRelay(
		store,
		outbox.HandlerFunc(
			func(ctx context.Context, record outbox.Record) error {
				delivered <- record.ID
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		),
		outbox.WithStorageErrorBackoff(50*time.Millisecond, 200*time.Millisecond),
		outbox.WithLogger(logger),
	)
	go func() { done <- relay.Run(parent) }()
	t.Cleanup(
		func() {
			stop()
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(10 * time.Second):
				t.Error("relay did not stop")
			}
		},
	)
	var first outbox.ID
	select {
	case first = <-delivered:
	case err := <-done:
		t.Fatalf("relay stopped before delivery: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	timeout := time.Second
	require.NoError(t, container.Stop(ctx, &timeout))
	close(release)
	// Observe a real settlement failure, then failed fetches while the DB is down.
	for range 2 {
		select {
		case err := <-logger.failures:
			require.Error(t, err)
		case err := <-done:
			t.Fatalf("relay stopped during outage: %v", err)
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	require.NoError(t, container.Start(ctx))
	select {
	case repeated := <-delivered:
		require.Equal(t, first, repeated)
	case err := <-done:
		t.Fatalf("relay stopped during recovery: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.Eventually(
		t,
		func() bool {
			var count int
			err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM outbox WHERE status = ?", outbox.StatusProcessed).Scan(&count)
			return err == nil && count == 1
		},
		10*time.Second,
		10*time.Millisecond,
	)
}

func TestRelayRetryPolicyStillFatalWithStorageRecoveryIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test disabled in short mode")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(
		func() {
			if err := db.Close(); err != nil {
				t.Error(err)
			}
			if err := container.Terminate(context.Background()); err != nil {
				t.Error(err)
			}
		},
	)
	setupSchema(t, ctx, db)
	store, err := mysql.NewStore(db)
	require.NoError(t, err)
	insertEntries(t, ctx, db, store, []outbox.Entry{validIntegrationEntry("unscheduled")})
	calls := 0
	relay := outbox.NewRelay(
		store,
		outbox.HandlerFunc(
			func(context.Context, outbox.Record) error {
				calls++
				return outbox.RetryAfter(context.DeadlineExceeded, time.Second)
			},
		),
		outbox.WithStorageErrorBackoff(time.Millisecond, time.Millisecond),
	)
	require.ErrorIs(t, relay.Run(ctx), outbox.ErrInvalidRetryPolicy)
	require.Equal(t, 1, calls)
	require.Equal(t, 1, countByStatus(t, ctx, db, outbox.StatusPending))
}

type recoveryLogger struct {
	outbox.NopLogger
	failures chan error
}

func (l *recoveryLogger) Error(_ string, args ...any) {
	for i := 0; i+1 < len(args); i += 2 {
		if args[i] == "err" {
			err, _ := args[i+1].(error)
			select {
			case l.failures <- err:
			default:
			}
		}
	}
}
