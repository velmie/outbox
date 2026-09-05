package mysql

import (
	"errors"
	"strings"
	"testing"
)

func TestSchemasDeclareInnoDB(t *testing.T) {
	tests := []struct {
		name  string
		build func() (string, error)
	}{
		{name: "json", build: func() (string, error) { return Schema("outbox") }},
		{name: "retry json", build: func() (string, error) { return RetrySchema("outbox") }},
		{name: "binary", build: func() (string, error) { return SchemaBinary("outbox") }},
		{
			name: "partitioned json",
			build: func() (string, error) {
				return PartitionedSchema("outbox", []Partition{{Name: "pmax", LessThan: "MAXVALUE"}})
			},
		},
		{
			name: "partitioned binary",
			build: func() (string, error) {
				return PartitionedSchemaBinary("outbox", []Partition{{Name: "pmax", LessThan: "MAXVALUE"}})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			schema, err := test.build()
			if err != nil {
				t.Fatalf("build schema: %v", err)
			}
			if !strings.Contains(schema, "ENGINE=InnoDB") {
				t.Fatal("schema does not declare ENGINE=InnoDB")
			}
		})
	}
}

func TestPartitionedSchemasRejectInvalidDefinitions(t *testing.T) {
	builders := []struct {
		name  string
		build func(string, []Partition) (string, error)
	}{
		{name: "json", build: PartitionedSchema},
		{name: "binary", build: PartitionedSchemaBinary},
	}
	invalid := []Partition{
		{Name: "", LessThan: "10"},
		{Name: "p-1", LessThan: "10"},
		{Name: "p.1", LessThan: "10"},
		{Name: "p 1", LessThan: "10"},
		{Name: "`p1`", LessThan: "10"},
		{Name: "pä", LessThan: "10"},
		{Name: strings.Repeat("p", 65), LessThan: "10"},
		{Name: "p1", LessThan: ""},
		{Name: "p1", LessThan: "maxvalue"},
		{Name: "p1", LessThan: " 10"},
		{Name: "p1", LessThan: "10 + 1"},
		{Name: "p1", LessThan: "9223372036854775808"},
		{Name: "p1", LessThan: "10)); DROP TABLE users; --"},
	}

	for _, builder := range builders {
		for _, part := range invalid {
			t.Run(builder.name+"/"+part.Name+"/"+part.LessThan, func(t *testing.T) {
				schema, err := builder.build("outbox", []Partition{part})
				if !errors.Is(err, ErrInvalidPartition) {
					t.Fatalf("error = %v, want ErrInvalidPartition", err)
				}
				if schema != "" {
					t.Fatalf("schema = %q, want empty", schema)
				}
			})
		}
	}
}

func TestPartitionedSchemasCanonicalizeIntegerBounds(t *testing.T) {
	builders := []struct {
		name  string
		build func(string, []Partition) (string, error)
	}{
		{name: "json", build: PartitionedSchema},
		{name: "binary", build: PartitionedSchemaBinary},
	}

	for _, builder := range builders {
		t.Run(builder.name, func(t *testing.T) {
			schema, err := builder.build("outbox", []Partition{
				{Name: "p1", LessThan: "+10"},
				{Name: "pmax", LessThan: "MAXVALUE"},
			})
			if err != nil {
				t.Fatalf("build schema: %v", err)
			}
			if !strings.Contains(schema, "VALUES LESS THAN (10)") {
				t.Fatalf("schema does not contain canonical integer bound: %s", schema)
			}
			if strings.Contains(schema, "VALUES LESS THAN (+10)") {
				t.Fatalf("schema contains non-canonical integer bound: %s", schema)
			}
		})
	}
}

func TestPartitionedSchemasQuoteValidNames(t *testing.T) {
	builders := []struct {
		name  string
		build func(string, []Partition) (string, error)
	}{
		{name: "json", build: PartitionedSchema},
		{name: "binary", build: PartitionedSchemaBinary},
	}
	longName := strings.Repeat("p", 64)

	for _, builder := range builders {
		t.Run(builder.name, func(t *testing.T) {
			schema, err := builder.build("outbox", []Partition{
				{Name: "select", LessThan: "10"},
				{Name: longName, LessThan: "20"},
				{Name: "pmax", LessThan: "MAXVALUE"},
			})
			if err != nil {
				t.Fatalf("build schema: %v", err)
			}
			if !strings.Contains(schema, "PARTITION `select` VALUES LESS THAN (10)") {
				t.Fatalf("schema does not quote reserved partition name: %s", schema)
			}
			if !strings.Contains(schema, "PARTITION `"+longName+"` VALUES LESS THAN (20)") {
				t.Fatalf("schema does not quote 64-byte partition name: %s", schema)
			}
		})
	}
}

func TestSchemaBinary(t *testing.T) {
	schema, err := SchemaBinary("outbox")
	if err != nil {
		t.Fatalf("schema binary: %v", err)
	}
	if !strings.Contains(schema, "payload LONGBLOB") {
		t.Fatalf("expected LONGBLOB payload in schema")
	}
	if !strings.Contains(schema, "headers JSON") {
		t.Fatalf("expected JSON headers in schema")
	}
}

func TestPartitionedSchemaBinary(t *testing.T) {
	parts := []Partition{{Name: "p1", LessThan: "10"}}
	schema, err := PartitionedSchemaBinary("outbox", parts)
	if err != nil {
		t.Fatalf("partitioned schema binary: %v", err)
	}
	if !strings.Contains(schema, "PARTITION BY RANGE") {
		t.Fatalf("expected partition clause")
	}
	if !strings.Contains(schema, "payload LONGBLOB") {
		t.Fatalf("expected LONGBLOB payload in schema")
	}
	if !strings.Contains(schema, "headers JSON") {
		t.Fatalf("expected JSON headers in schema")
	}
}
