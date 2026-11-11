package outbox_test

import (
	"strings"
	"testing"

	"github.com/velmie/outbox"
)

func TestDedupKeyLength(t *testing.T) {
	entry := outbox.Entry{
		AggregateType: "endpoint",
		EventType:     "delivery",
		Payload:       []byte(`{}`),
		DedupKey:      strings.Repeat("é", 256),
	}
	if err := entry.Validate(); err != nil {
		t.Fatalf("512-byte key rejected: %v", err)
	}
	entry.DedupKey += "x"
	if err := entry.Validate(); err == nil || !strings.Contains(err.Error(), "512 bytes") {
		t.Fatalf("513-byte key error = %v", err)
	}
}
