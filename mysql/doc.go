// Package mysql provides a high-performance MySQL 8.0+ outbox implementation.
//
// The consumer uses:
//   - READ COMMITTED isolation (to avoid gap locks)
//   - SELECT ... FOR UPDATE SKIP LOCKED
//   - ORDER BY id ASC (UUID v7 time ordering)
//   - LIMIT for batching
//
// See Schema/PartitionedSchema (JSON payloads) or SchemaBinary/PartitionedSchemaBinary (raw bytes),
// PartitionMaintainer for partition rotation, and CleanupMaintainer for periodic row cleanup when
// partitions are not used.
package mysql
