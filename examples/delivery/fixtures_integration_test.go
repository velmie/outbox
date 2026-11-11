//go:build integration

package delivery_test

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/moby/moby/api/types/network"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

const (
	mysqlImage = "mysql:8.4.11@sha256:b3b90af2a6552ae30c266fdb7d5dd55f3afb72404bb78d37fe8a23eb857fd3fb"
	natsImage  = "nats:2.15.0@sha256:cd3fcd4ecdda44e3a66728a5334af0a959bc3979b32810e033d1c547241cd0f4"
)

func startMySQLContainer(t *testing.T, ctx context.Context) *sql.DB {
	t.Helper()
	password := rand.Text()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        mysqlImage,
			ExposedPorts: []string{"3306/tcp"},
			Env: map[string]string{
				"MYSQL_ROOT_PASSWORD": password,
				"MYSQL_DATABASE":      "outbox",
			},
			WaitingFor: wait.ForSQL("3306/tcp", "mysql", func(host string, port network.Port) string {
				return fmt.Sprintf("root:%s@tcp(%s:%s)/outbox?parseTime=true", password, host, port.Port())
			}).WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, container)
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "3306/tcp")
	require.NoError(t, err)
	db, err := sql.Open("mysql", fmt.Sprintf("root:%s@tcp(%s:%s)/outbox?parseTime=true", password, host, port.Port()))
	require.NoError(t, err)
	return db
}

func startNATS(t *testing.T, ctx context.Context) nats.JetStreamContext {
	t.Helper()
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        natsImage,
			Cmd:          []string{"-js"},
			ExposedPorts: []string{"4222/tcp"},
			WaitingFor:   wait.ForListeningPort("4222/tcp"),
		},
		Started: true,
	})
	testcontainers.CleanupContainer(t, container)
	require.NoError(t, err)
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "4222/tcp")
	require.NoError(t, err)
	conn, err := nats.Connect("nats://" + host + ":" + port.Port())
	require.NoError(t, err)
	t.Cleanup(conn.Close)
	js, err := conn.JetStream()
	require.NoError(t, err)
	return js
}

type retryState struct {
	status    outbox.Status
	attempts  int
	lastError sql.NullString
	deadline  sql.NullTime
}

func readRetryState(t *testing.T, ctx context.Context, db *sql.DB, id outbox.ID) retryState {
	t.Helper()
	var state retryState
	err := db.QueryRowContext(ctx, "SELECT status, attempt_count, last_error, next_attempt_at FROM outbox WHERE id = ?", id).
		Scan(&state.status, &state.attempts, &state.lastError, &state.deadline)
	require.NoError(t, err)
	return state
}

func requireNoRetryRecords(t *testing.T, ctx context.Context, store *mysql.Store) {
	t.Helper()
	batch, err := store.Fetch(ctx, outbox.FetchOptions{
		BatchSize: 1,
	})
	if batch != nil {
		require.NoError(t, batch.Rollback())
	}
	require.ErrorIs(t, err, outbox.ErrNoRecords)
}

func makeRetryDue(t *testing.T, ctx context.Context, db *sql.DB, id outbox.ID) {
	t.Helper()
	_, err := db.ExecContext(ctx, "UPDATE outbox SET next_attempt_at = UTC_TIMESTAMP(6) WHERE id = ?", id)
	require.NoError(t, err)
}

func fetchStatus(t *testing.T, ctx context.Context, db *sql.DB, id outbox.ID) (outbox.Status, int) {
	t.Helper()
	state := readRetryState(t, ctx, db, id)
	return state.status, state.attempts
}

func admissionKey(route, messageID string) string {
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(route)))
	return string(prefix[:]) + route + messageID
}
