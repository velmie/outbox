package outbox

// Status represents the lifecycle state of an outbox record.
type Status int16

const (
	// StatusPending indicates the record is ready for processing.
	StatusPending Status = 0
	// StatusProcessed indicates the record was processed successfully.
	StatusProcessed Status = 1
	// StatusDead indicates the record exceeded retry attempts and is dead-lettered.
	StatusDead Status = -1
)
