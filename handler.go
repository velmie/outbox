package outbox

import "context"

// Handler processes a single outbox record.
type Handler interface {
	// Handle processes a single record and returns an error on failure.
	Handle(ctx context.Context, record Record) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, record Record) error

// Handle implements Handler.
func (fn HandlerFunc) Handle(ctx context.Context, record Record) error {
	return fn(ctx, record)
}
