//go:build integration

package mysql_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/velmie/outbox"
	"github.com/velmie/outbox/mysql"
)

func TestSchemaOptionsBinaryRetryRoundTrip(t *testing.T) {
	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
		require.NoError(t, container.Terminate(ctx))
	})
	schema, err := mysql.SchemaWithOptions("outbox", mysql.SchemaOptions{
		BinaryPayload:      true,
		Retry:              true,
		AggregateTypeIndex: true,
		Deduplication:      true,
	})
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, schema)
	require.NoError(t, err)
	store, err := mysql.NewStore(db, mysql.WithValidatePayload(false), mysql.WithRetryDelay(time.Second))
	require.NoError(t, err)
	payload := []byte{0xff, 0x00, 0x01}
	insertEntries(t, ctx, db, store, []outbox.Entry{{
		AggregateType: "endpoint",
		AggregateID:   "1",
		EventType:     "delivery",
		Payload:       payload,
	}})
	batch, err := store.Fetch(ctx, outbox.FetchOptions{
		BatchSize: 1,
	})
	require.NoError(t, err)
	require.Len(t, batch.Records(), 1)
	require.Equal(t, payload, []byte(batch.Records()[0].Payload))
	require.NoError(t, batch.Rollback())

	var columns string
	err = db.QueryRowContext(ctx, `SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX)
FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'outbox'
AND INDEX_NAME = 'idx_aggregate_type_status_next_attempt_id'`).Scan(&columns)
	require.NoError(t, err)
	require.Equal(t, "aggregate_type,status,next_attempt_at,id", columns)

	for _, test := range []struct {
		table   string
		opts    mysql.SchemaOptions
		index   string
		columns string
	}{
		{
			table: "scoped",
			opts: mysql.SchemaOptions{
				AggregateTypeIndex: true,
			},
			index:   "idx_aggregate_type_status_id",
			columns: "aggregate_type,status,id",
		},
		{
			table: "partitioned",
			opts: mysql.SchemaOptions{
				BinaryPayload:      true,
				Retry:              true,
				AggregateTypeIndex: true,
				Partitions: []mysql.Partition{{
					Name:     "pmax",
					LessThan: "MAXVALUE",
				}},
			},
			index:   "idx_aggregate_type_status_next_attempt_id",
			columns: "aggregate_type,status,next_attempt_at,id",
		},
	} {
		ddl, err := mysql.SchemaWithOptions(test.table, test.opts)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, ddl)
		require.NoError(t, err)
		err = db.QueryRowContext(ctx, `SELECT GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX)
FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
AND INDEX_NAME = ?`, test.table, test.index).Scan(&columns)
		require.NoError(t, err)
		require.Equal(t, test.columns, columns)
	}
}
