package outbox

import (
	"encoding/json"
	"time"
)

// Record is a stored outbox message fetched for processing.
type Record struct {
	ID            ID
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       json.RawMessage
	Headers       json.RawMessage
	CreatedAt     time.Time
	Attempts      int
}

// Failure captures a processing error for a record.
type Failure struct {
	ID  ID
	Err error
}
