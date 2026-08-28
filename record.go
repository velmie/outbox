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

// Failure describes the error detail a Batch may persist for a record.
// Relay-created failures contain stable summaries without handler error details.
type Failure struct {
	ID ID
	// Err is the persistence detail. Direct Batch callers are responsible for
	// excluding secrets from errors they provide.
	Err error
}
