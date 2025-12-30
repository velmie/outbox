// Package outbox provides a transactional outbox engine with pluggable storage backends.
//
// Typical flow:
//  1. Within a business transaction, enqueue outbox entries using a storage-specific writer.
//  2. Run a Relay with a storage-specific Consumer to poll, lock, and process entries.
//  3. On success, Relay marks entries as processed; on failure it increments attempts and can move entries to a dead-letter state.
//
// For the MySQL implementation (optimized for polling with SKIP LOCKED), see the mysql package.
package outbox
