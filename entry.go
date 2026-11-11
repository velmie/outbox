package outbox

import "encoding/json"

const maxDedupKeyBytes = 512

// Entry describes a new outbox message to be persisted.
type Entry struct {
	// ID is optional. If zero, the store generator assigns a UUIDv7.
	// A nonzero ID must be an RFC 9562 UUIDv7; its timestamp may be historical.
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
	// DedupKey is an optional opaque identity, at most 512 bytes. Empty disables
	// deduplication. Its lifetime equals the stored row's lifetime. Use a
	// non-secret identity: a database conflict may include the key in its error.
	DedupKey string
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
	if len(e.DedupKey) > maxDedupKeyBytes {
		return errDedupKeyTooLong
	}
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
	if !e.ID.IsZero() && !e.ID.isUUIDv7() {
		return ErrInvalidID
	}

	return nil
}
