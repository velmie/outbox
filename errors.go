package outbox

import "errors"

var (
	// ErrInvalidBatchSize indicates that the requested batch size is not positive.
	ErrInvalidBatchSize = errors.New("outbox batch size must be positive")
	// ErrNoRecords signals that no records are available for processing.
	ErrNoRecords = errors.New("outbox has no pending records")
	// ErrNilBatch indicates that a consumer returned a nil batch.
	ErrNilBatch = errors.New("outbox batch is nil")
	// ErrEmptyBatch indicates that a consumer returned a batch with no records.
	ErrEmptyBatch = errors.New("outbox batch has no records")
	// ErrAggregateTypeRequired is returned when Entry.AggregateType is empty.
	ErrAggregateTypeRequired = errors.New("outbox aggregate type is required")
	// ErrEventTypeRequired is returned when Entry.EventType is empty.
	ErrEventTypeRequired = errors.New("outbox event type is required")
	// ErrPayloadRequired is returned when Entry.Payload is empty.
	ErrPayloadRequired = errors.New("outbox payload is required")
	// ErrInvalidPayload is returned when Entry.Payload is not valid JSON.
	ErrInvalidPayload = errors.New("outbox payload must be valid JSON")
	// ErrInvalidHeaders is returned when Entry.Headers is not valid JSON.
	ErrInvalidHeaders = errors.New("outbox headers must be valid JSON")
	// ErrInvalidID is returned when parsing or scanning an ID fails.
	ErrInvalidID = errors.New("outbox id is invalid")
	// ErrWorkerPanic indicates a relay worker panic.
	ErrWorkerPanic = errors.New("outbox worker panic")
)
