package outbox

import (
	"encoding/json"
	"testing"
)

func TestEntryValidate(t *testing.T) {
	validPayload := json.RawMessage(`{"ok":true}`)

	cases := []struct {
		name  string
		entry Entry
		err   error
	}{
		{
			name:  "missing aggregate type",
			entry: Entry{EventType: "event", Payload: validPayload},
			err:   ErrAggregateTypeRequired,
		},
		{
			name:  "missing event type",
			entry: Entry{AggregateType: "order", Payload: validPayload},
			err:   ErrEventTypeRequired,
		},
		{
			name:  "missing payload",
			entry: Entry{AggregateType: "order", EventType: "event"},
			err:   ErrPayloadRequired,
		},
		{
			name:  "invalid payload",
			entry: Entry{AggregateType: "order", EventType: "event", Payload: json.RawMessage(`{`)},
			err:   ErrInvalidPayload,
		},
		{
			name:  "invalid headers",
			entry: Entry{AggregateType: "order", EventType: "event", Payload: validPayload, Headers: json.RawMessage(`{`)},
			err:   ErrInvalidHeaders,
		},
		{
			name:  "valid",
			entry: Entry{AggregateType: "order", EventType: "event", Payload: validPayload},
			err:   nil,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.entry.Validate()
			if tc.err == nil && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.err != nil && err != tc.err {
				t.Fatalf("expected %v, got %v", tc.err, err)
			}
		})
	}
}

func TestValidateEntrySkipJSON(t *testing.T) {
	entry := Entry{
		AggregateType: "order",
		EventType:     "event",
		Payload:       json.RawMessage(`{`),
		Headers:       json.RawMessage(`{`),
	}

	if err := ValidateEntry(entry, false); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}

func TestValidateEntryWithOptions(t *testing.T) {
	entry := Entry{
		AggregateType: "order",
		EventType:     "event",
		Payload:       json.RawMessage(`{`),
		Headers:       json.RawMessage(`{`),
	}

	if err := ValidateEntryWithOptions(entry, false, true); err != ErrInvalidHeaders {
		t.Fatalf("expected invalid headers, got %v", err)
	}
	if err := ValidateEntryWithOptions(entry, true, false); err != ErrInvalidPayload {
		t.Fatalf("expected invalid payload, got %v", err)
	}
	if err := ValidateEntryWithOptions(entry, false, false); err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
}
