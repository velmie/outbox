package mysql

import (
	"strings"
	"testing"
)

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
