package mysql

import "testing"

func TestSanitizeTableName(t *testing.T) {
	valid := []string{"outbox", "schema.outbox", "OUTBOX_1"}
	for _, name := range valid {
		if _, err := sanitizeTableName(name); err != nil {
			t.Fatalf("expected valid name %q: %v", name, err)
		}
	}

	invalid := []string{"", "outbox;drop", "outbox-1", "schema..outbox", "schema.outbox;"}
	for _, name := range invalid {
		if _, err := sanitizeTableName(name); err == nil {
			t.Fatalf("expected invalid name %q", name)
		}
	}
}
