package mysql

import (
	"context"
	"database/sql"
	"errors"

	"github.com/velmie/outbox"
)

type batch struct {
	tx       *sql.Tx
	store    *Store
	records  []outbox.Record
	attempts map[outbox.ID]int
}

// Records returns the records fetched for this batch.
func (b *batch) Records() []outbox.Record {
	return b.records
}

// Ack marks the provided records as processed.
func (b *batch) Ack(ctx context.Context, ids []outbox.ID) error {
	return b.store.ack(ctx, b.tx, ids)
}

// Fail records failures and updates retry state for each record.
func (b *batch) Fail(ctx context.Context, failures []outbox.Failure) error {
	if b.attempts == nil {
		b.attempts = make(map[outbox.ID]int, len(b.records))
		for i := range b.records {
			b.attempts[b.records[i].ID] = b.records[i].Attempts
		}
	}

	return b.store.fail(ctx, b.tx, failures, b.attempts)
}

// Dead marks the provided records as dead.
func (b *batch) Dead(ctx context.Context, failures []outbox.Failure) error {
	return b.store.dead(ctx, b.tx, failures)
}

// Commit finalizes the batch transaction.
func (b *batch) Commit() error {
	return b.tx.Commit()
}

// Rollback releases locks without applying any changes.
func (b *batch) Rollback() error {
	err := b.tx.Rollback()
	if errors.Is(err, sql.ErrTxDone) {
		return nil
	}

	return err
}
