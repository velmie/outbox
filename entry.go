package outbox

import "encoding/json"

// Entry describes a new outbox message to be persisted.
type Entry struct {
	// ID is optional, if zero, the store generator assigns a UUID v7.
	ID ID
	// AggregateType is a coarse-grained stream identifier (e.g., "order").
	AggregateType string
	// AggregateID optionally identifies the stream instance (e.g., order ID).
	AggregateID string
	// EventType names the specific event (e.g., "order.created").
	EventType string
	// Payload is stored as JSON by default, binary schemas may store raw bytes.
	Payload json.RawMessage
	// Headers is optional metadata (JSON object recommended).
	Headers json.RawMessage
}

// Validate checks required fields and JSON validity.
func (e Entry) Validate() error {
	return ValidateEntry(e, true)
}

// ValidateEntry validates an entry with optional JSON validation for payload and headers.
func ValidateEntry(entry Entry, validateJSON bool) error {
	return entry.validate(validateJSON, validateJSON)
}

// ValidateEntryWithOptions validates payload and headers independently.
func ValidateEntryWithOptions(entry Entry, validatePayload, validateHeaders bool) error {
	return entry.validate(validatePayload, validateHeaders)
}

func (e Entry) validate(validatePayload, validateHeaders bool) error {
	if e.AggregateType == "" {
		return ErrAggregateTypeRequired
	}
	if e.EventType == "" {
		return ErrEventTypeRequired
	}
	if len(e.Payload) == 0 {
		return ErrPayloadRequired
	}
	if validatePayload && !json.Valid(e.Payload) {
		return ErrInvalidPayload
	}
	if validateHeaders && len(e.Headers) > 0 && !json.Valid(e.Headers) {
		return ErrInvalidHeaders
	}

	return nil
}
