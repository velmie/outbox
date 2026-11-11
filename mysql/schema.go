package mysql

import (
	"fmt"
	"strconv"
)

const schemaTemplate = `CREATE TABLE IF NOT EXISTS %s (
	id BINARY(16) NOT NULL,
	aggregate_type VARCHAR(128) NOT NULL,
	aggregate_id VARCHAR(128) NOT NULL,
	event_type VARCHAR(128) NOT NULL,
	payload %s NOT NULL,
	headers %s NULL,
	status SMALLINT NOT NULL DEFAULT 0,
	attempt_count INT NOT NULL DEFAULT 0,
%s	last_error VARCHAR(1024) NULL,
	created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
	processed_at TIMESTAMP(6) NULL,
	created_ts BIGINT GENERATED ALWAYS AS (CONV(SUBSTR(HEX(id), 1, 12), 16, 10) DIV 1000) STORED,
	PRIMARY KEY (id, created_ts),
	INDEX idx_status_id (status, id)%s
) ENGINE=InnoDB%s;`

const (
	payloadJSON           = "JSON"
	payloadBinary         = "LONGBLOB"
	headersJSON           = "JSON"
	partitionClausePrefix = "\nPARTITION BY RANGE (created_ts) ("
	partitionClauseSuffix = "\n)"
	retryColumn           = "\tnext_attempt_at DATETIME(6) NULL,\n"
	retryIndex            = ",\n\tINDEX idx_status_next_attempt_id (status, next_attempt_at, id)"
)

// Partition defines a range partition for created_ts.
// Name must match [A-Za-z_][A-Za-z0-9_]{0,63}.
// LessThan must be a base-10 integer or the exact string MAXVALUE.
type Partition struct {
	Name     string
	LessThan string
}

// SchemaOptions selects optional table capabilities. Its zero value produces
// the base JSON schema.
type SchemaOptions struct {
	BinaryPayload      bool
	Retry              bool
	AggregateTypeIndex bool
	Deduplication      bool
	Partitions         []Partition
}

// SchemaWithOptions returns migration DDL without executing SQL.
func SchemaWithOptions(table string, opts SchemaOptions) (string, error) {
	if opts.Deduplication && len(opts.Partitions) > 0 {
		return "", errDeduplicationPartitioned
	}
	clause := ""
	if len(opts.Partitions) > 0 {
		var err error
		clause, err = buildPartitionClause(opts.Partitions)
		if err != nil {
			return "", err
		}
	}
	payload := payloadJSON
	if opts.BinaryPayload {
		payload = payloadBinary
	}

	return buildSchema(table, payload, clause, opts)
}

// Schema returns the base schema for an outbox table (without partitioning).
func Schema(table string) (string, error) {
	return SchemaWithOptions(table, SchemaOptions{})
}

// RetrySchema returns a non-partitioned JSON schema supporting WithRetryDelay.
// A null deadline makes a new row immediately eligible. Apply the schema through
// migrations; CREATE TABLE IF NOT EXISTS does not upgrade an existing table.
func RetrySchema(table string) (string, error) {
	return SchemaWithOptions(table, SchemaOptions{
		Retry: true,
	})
}

// SchemaBinary returns a schema with LONGBLOB payload and JSON headers.
func SchemaBinary(table string) (string, error) {
	return SchemaWithOptions(table, SchemaOptions{
		BinaryPayload: true,
	})
}

// PartitionedSchema returns a schema with RANGE partitions on created_ts.
func PartitionedSchema(table string, partitions []Partition) (string, error) {
	clause, err := buildPartitionClause(partitions)
	if err != nil {
		return "", err
	}

	return buildSchema(table, payloadJSON, clause, SchemaOptions{})
}

// PartitionedSchemaBinary returns a schema with LONGBLOB payload and RANGE partitions on created_ts.
func PartitionedSchemaBinary(table string, partitions []Partition) (string, error) {
	clause, err := buildPartitionClause(partitions)
	if err != nil {
		return "", err
	}

	return buildSchema(table, payloadBinary, clause, SchemaOptions{})
}

func buildSchema(table, payloadType, partitionClause string, opts SchemaOptions) (string, error) {
	name, err := sanitizeTableName(table)
	if err != nil {
		return "", err
	}

	column, index := "", ""
	if opts.Retry {
		column, index = retryColumn, retryIndex
	}
	if opts.Deduplication {
		column += "\tdedup_key VARBINARY(512) NULL,\n"
		index += ",\n\tUNIQUE KEY uq_dedup_key (dedup_key)"
	}
	if opts.AggregateTypeIndex {
		if opts.Retry {
			index += ",\n\tINDEX idx_aggregate_type_status_next_attempt_id (aggregate_type, status, next_attempt_at, id)"
		} else {
			index += ",\n\tINDEX idx_aggregate_type_status_id (aggregate_type, status, id)"
		}
	}

	return fmt.Sprintf(schemaTemplate, name, payloadType, headersJSON, column, index, partitionClause), nil
}

func buildPartitionClause(partitions []Partition) (string, error) {
	if len(partitions) == 0 {
		return "", ErrPartitionsRequired
	}

	clause := partitionClausePrefix
	for i, part := range partitions {
		name, err := quotePartitionName(part.Name)
		if err != nil {
			return "", err
		}
		bound, err := normalizePartitionBound(part.LessThan)
		if err != nil {
			return "", err
		}
		if i > 0 {
			clause += ","
		}
		clause += fmt.Sprintf("\n\tPARTITION %s VALUES LESS THAN (%s)", name, bound)
	}
	clause += partitionClauseSuffix

	return clause, nil
}

func normalizePartitionBound(value string) (string, error) {
	if value == "MAXVALUE" {
		return value, nil
	}

	bound, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return "", ErrInvalidPartition
	}

	return strconv.FormatInt(bound, 10), nil
}
