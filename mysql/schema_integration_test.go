//go:build integration

package mysql_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox/mysql"
)

func TestSchemasUseInnoDBWhenSessionDefaultDiffers(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test disabled in short mode")
	}

	ctx := context.Background()
	container, db := startMySQLContainer(t, ctx)
	t.Cleanup(func() {
		_ = db.Close()
		_ = container.Terminate(ctx)
	})

	conn, err := db.Conn(ctx)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = conn.Close()
	})

	_, err = conn.ExecContext(ctx, "SET SESSION default_storage_engine = MyISAM")
	require.NoError(t, err)

	tests := []struct {
		name  string
		table string
		build func() (string, error)
	}{
		{name: "json", table: "schema_json", build: func() (string, error) { return mysql.Schema("schema_json") }},
		{name: "retry json", table: "schema_retry", build: func() (string, error) { return mysql.RetrySchema("schema_retry") }},
		{name: "binary", table: "schema_binary", build: func() (string, error) { return mysql.SchemaBinary("schema_binary") }},
		{
			name:  "partitioned json",
			table: "schema_partitioned_json",
			build: func() (string, error) {
				return mysql.PartitionedSchema(
					"schema_partitioned_json",
					[]mysql.Partition{
						{Name: "select", LessThan: "10"},
						{Name: "pmax", LessThan: "MAXVALUE"},
					},
				)
			},
		},
		{
			name:  "partitioned binary",
			table: "schema_partitioned_binary",
			build: func() (string, error) {
				return mysql.PartitionedSchemaBinary(
					"schema_partitioned_binary",
					[]mysql.Partition{
						{Name: "select", LessThan: "10"},
						{Name: "pmax", LessThan: "MAXVALUE"},
					},
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema, err := test.build()
			require.NoError(t, err)
			_, err = conn.ExecContext(ctx, schema)
			require.NoError(t, err)

			var engine sql.NullString
			err = conn.QueryRowContext(ctx, `
SELECT ENGINE
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?
`, test.table).Scan(&engine)
			require.NoError(t, err)
			require.Equal(t, "InnoDB", engine.String)
		})
	}
}
