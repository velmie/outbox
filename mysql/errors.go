package mysql

import "errors"

var (
	// ErrDBRequired is returned when a nil *sql.DB is provided.
	ErrDBRequired = errors.New("outbox mysql: db is required")
	// ErrExecutorRequired is returned when enqueue is called with a nil executor.
	ErrExecutorRequired = errors.New("outbox mysql: executor is required")
	// ErrTableNameRequired is returned when the table name is empty.
	ErrTableNameRequired = errors.New("outbox mysql: table name is required")
	// ErrInvalidTableName is returned when the table name has disallowed characters.
	ErrInvalidTableName = errors.New("outbox mysql: invalid table name")
	// ErrPartitionsRequired is returned when partition definitions are missing.
	ErrPartitionsRequired = errors.New("outbox mysql: partitions are required")
	// ErrInvalidPartition is returned when a partition definition is invalid.
	ErrInvalidPartition = errors.New("outbox mysql: invalid partition definition")
	// ErrPartitionPeriodRequired is returned when the partition period is missing or invalid.
	ErrPartitionPeriodRequired = errors.New("outbox mysql: partition period is required")
	// ErrPartitionRetentionInvalid is returned when retention is negative.
	ErrPartitionRetentionInvalid = errors.New("outbox mysql: partition retention must be non-negative")
	// ErrPartitionSchemaRequired is returned when the database name cannot be resolved.
	ErrPartitionSchemaRequired = errors.New("outbox mysql: database name is required for partition maintenance")
	// ErrPartitionDescriptionInvalid is returned when partition description cannot be parsed.
	ErrPartitionDescriptionInvalid = errors.New("outbox mysql: invalid partition description")
	// ErrPartitionNameConflict is returned when a generated partition name already exists.
	ErrPartitionNameConflict = errors.New("outbox mysql: partition name conflict")
	// ErrPartitionedTableRequired is returned when the table is not partitioned.
	ErrPartitionedTableRequired = errors.New("outbox mysql: table is not partitioned")
	// ErrPartitionMaxRequired is returned when MAXVALUE partition is missing.
	ErrPartitionMaxRequired = errors.New("outbox mysql: MAXVALUE partition is required")
	// ErrCleanupBeforeRequired is returned when cleanup cutoff is missing.
	ErrCleanupBeforeRequired = errors.New("outbox mysql: cleanup before time is required")
	// ErrCleanupLimitInvalid is returned when cleanup limit is negative.
	ErrCleanupLimitInvalid = errors.New("outbox mysql: cleanup limit must be non-negative")
	// ErrCleanupRetentionInvalid is returned when cleanup retention is not positive.
	ErrCleanupRetentionInvalid = errors.New("outbox mysql: cleanup retention must be positive")
)
