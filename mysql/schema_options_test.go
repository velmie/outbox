package mysql_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/velmie/outbox/mysql"
)

func TestSchemaOptionsCompatibility(t *testing.T) {
	parts := []mysql.Partition{{
		Name:     "pmax",
		LessThan: "MAXVALUE",
	}}
	tests := []struct {
		options mysql.SchemaOptions
		legacy  func() (string, error)
	}{
		{
			options: mysql.SchemaOptions{},
			legacy:  func() (string, error) { return mysql.Schema("outbox") },
		},
		{
			options: mysql.SchemaOptions{
				Retry: true,
			},
			legacy: func() (string, error) { return mysql.RetrySchema("outbox") },
		},
		{
			options: mysql.SchemaOptions{
				BinaryPayload: true,
			},
			legacy: func() (string, error) { return mysql.SchemaBinary("outbox") },
		},
		{
			options: mysql.SchemaOptions{
				Partitions: parts,
			},
			legacy: func() (string, error) { return mysql.PartitionedSchema("outbox", parts) },
		},
		{
			options: mysql.SchemaOptions{
				BinaryPayload: true,
				Partitions:    parts,
			},
			legacy: func() (string, error) { return mysql.PartitionedSchemaBinary("outbox", parts) },
		},
	}
	for _, test := range tests {
		got, err := mysql.SchemaWithOptions("outbox", test.options)
		require.NoError(t, err)
		want, err := test.legacy()
		require.NoError(t, err)
		require.Equal(t, want, got)
	}
	_, err := mysql.SchemaWithOptions("outbox", mysql.SchemaOptions{
		Deduplication: true,
		Partitions:    parts,
	})
	require.ErrorContains(t, err, "deduplication cannot be combined with partitions")
}

func ExampleSchemaWithOptions() {
	ddl, err := mysql.SchemaWithOptions("outbox", mysql.SchemaOptions{
		BinaryPayload:      true,
		Retry:              true,
		AggregateTypeIndex: true,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(len(ddl) > 0)
	// Output: true
}
