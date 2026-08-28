package outbox

import (
	"encoding/json"
	"errors"
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

func TestEntryValidateID(t *testing.T) {
	valid, err := ParseID("017f22e2-79b0-7cc3-98c4-dc0c0c07398f")
	if err != nil {
		t.Fatalf("parse valid UUIDv7: %v", err)
	}
	wrongVersion := valid
	wrongVersion[6] = (wrongVersion[6] & 0x0f) | 0x40
	wrongVariant00 := valid
	wrongVariant00[8] &= 0x3f
	wrongVariant11 := valid
	wrongVariant11[8] |= 0xc0

	tests := []struct {
		name string
		id   ID
		err  error
	}{
		{name: "generated", id: ID{}},
		{name: "old UUIDv7", id: valid},
		{name: "wrong version", id: wrongVersion, err: ErrInvalidID},
		{name: "wrong variant 00", id: wrongVariant00, err: ErrInvalidID},
		{name: "wrong variant 11", id: wrongVariant11, err: ErrInvalidID},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			entry := Entry{
				ID:            test.id,
				AggregateType: "order",
				EventType:     "created",
				Payload:       json.RawMessage(`{"id":1}`),
			}
			err := entry.Validate()
			if !errors.Is(err, test.err) {
				t.Fatalf("error = %v, want %v", err, test.err)
			}
		})
	}
}
