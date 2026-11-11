//go:build integration

package mysql_test

import (
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

func TestDeduplicationTransactions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, container.Terminate(context.Background()))
	})
	ddl, err := mysql.SchemaWithOptions("outbox", mysql.SchemaOptions{
		Deduplication: true,
	})
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, ddl)
	require.NoError(t, err)
	store, err := mysql.NewStore(db)
	require.NoError(t, err)
	entry := outbox.Entry{
		AggregateType: "endpoint",
		AggregateID:   "route",
		EventType:     "delivery",
		Payload:       []byte(`{"original":true}`),
		DedupKey:      "route\x00message",
	}
	tx, err := db.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer tx.Rollback()
	id, err := store.Enqueue(ctx, tx, entry)
	require.NoError(t, err)
	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	defer conn.Close()
	var connectionID int64
	require.NoError(t, conn.QueryRowContext(ctx, "SELECT CONNECTION_ID()").Scan(&connectionID))
	otherTx, err := conn.BeginTx(ctx, nil)
	require.NoError(t, err)
	defer otherTx.Rollback()
	type outcome struct {
		id  outbox.ID
		err error
	}
	results := make(chan outcome, 1)
	go func() {
		other := entry
		other.Payload = []byte(`{"original":false}`)
		otherID, insertErr := store.Enqueue(ctx, otherTx, other)
		results <- outcome{
			id:  otherID,
			err: insertErr,
		}
	}()
	require.Eventually(t, func() bool {
		var waiting int
		queryErr := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM information_schema.INNODB_TRX
WHERE trx_mysql_thread_id = ? AND trx_state = 'LOCK WAIT'`, connectionID).Scan(&waiting)
		return queryErr == nil && waiting == 1
	}, 10*time.Second, 50*time.Millisecond, "competing transaction did not wait for the key")
	require.NoError(t, tx.Commit())
	result := <-results
	require.NoError(t, otherTx.Rollback())
	require.True(t, result.id.IsZero())
	require.ErrorIs(t, result.err, outbox.ErrDuplicate)
	var mysqlErr *mysqldriver.MySQLError
	require.True(t, errors.As(result.err, &mysqlErr))
	require.EqualValues(t, 1062, mysqlErr.Number)
	require.Contains(t, mysqlErr.Message, "route")
	var payload string
	require.NoError(t, db.QueryRowContext(ctx, "SELECT payload FROM outbox WHERE id = ?", id).Scan(&payload))
	require.JSONEq(t, `{"original":true}`, payload)
	withSameID := entry
	withSameID.ID = id
	withSameID.DedupKey = ""
	duplicateID, duplicateErr := store.Enqueue(ctx, db, withSameID)
	require.True(t, duplicateID.IsZero())
	require.ErrorIs(t, duplicateErr, outbox.ErrDuplicate)
	var duplicateMySQL *mysqldriver.MySQLError
	require.ErrorAs(t, duplicateErr, &duplicateMySQL)
	require.EqualValues(t, 1062, duplicateMySQL.Number)
	_, err = db.ExecContext(ctx, "DELETE FROM outbox WHERE id = ?", id)
	require.NoError(t, err)
	_, err = store.Enqueue(ctx, db, entry)
	require.NoError(t, err)

	entry.DedupKey = "rollback"
	tx, err = db.BeginTx(ctx, nil)
	require.NoError(t, err)
	_, err = store.Enqueue(ctx, tx, entry)
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())
	_, err = store.Enqueue(ctx, db, entry)
	require.NoError(t, err)
	entry.DedupKey = ""
	_, err = store.Enqueue(ctx, db, entry)
	require.NoError(t, err)
	_, err = store.Enqueue(ctx, db, entry)
	require.NoError(t, err)
	entry.DedupKey = admissionKey("ab", "c")
	_, err = store.Enqueue(ctx, db, entry)
	require.NoError(t, err)
	entry.DedupKey = admissionKey("a", "bc")
	_, err = store.Enqueue(ctx, db, entry)
	require.NoError(t, err)
	entry.DedupKey = admissionKey("AB", "c")
	_, err = store.Enqueue(ctx, db, entry)
	require.NoError(t, err)

	legacyDDL, err := mysql.Schema("legacy_outbox")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, legacyDDL)
	require.NoError(t, err)
	legacy, err := mysql.NewStore(db, mysql.WithTable("legacy_outbox"))
	require.NoError(t, err)
	entry.DedupKey = ""
	_, err = legacy.Enqueue(ctx, db, entry)
	require.NoError(t, err)
}

func admissionKey(route, messageID string) string {
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(route)))
	return string(prefix[:]) + route + messageID
}
