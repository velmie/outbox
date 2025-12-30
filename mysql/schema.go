package mysql

import (
	"fmt"
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
	last_error VARCHAR(1024) NULL,
	created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
	updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
	processed_at TIMESTAMP(6) NULL,
	created_ts BIGINT GENERATED ALWAYS AS (CONV(SUBSTR(HEX(id), 1, 12), 16, 10) DIV 1000) STORED,
	PRIMARY KEY (id, created_ts),
	INDEX idx_status_id (status, id)
)%s;`

const (
	payloadJSON           = "JSON"
	payloadBinary         = "LONGBLOB"
	headersJSON           = "JSON"
	partitionClausePrefix = "\nPARTITION BY RANGE (created_ts) ("
	partitionClauseSuffix = "\n)"
)

// Partition defines a range partition for created_ts.
type Partition struct {
	Name     string
	LessThan string
}

// Schema returns the base schema for an outbox table (without partitioning).
func Schema(table string) (string, error) {
	return buildSchema(table, payloadJSON, "")
}

// SchemaBinary returns a schema with LONGBLOB payload and JSON headers.
func SchemaBinary(table string) (string, error) {
	return buildSchema(table, payloadBinary, "")
}

// PartitionedSchema returns a schema with RANGE partitions on created_ts.
func PartitionedSchema(table string, partitions []Partition) (string, error) {
	if len(partitions) == 0 {
		return "", ErrPartitionsRequired
	}

	clause := partitionClausePrefix
	for i, part := range partitions {
		if part.Name == "" || part.LessThan == "" {
			return "", ErrInvalidPartition
		}
		if i > 0 {
			clause += ","
		}
		clause += fmt.Sprintf("\n\tPARTITION %s VALUES LESS THAN (%s)", part.Name, part.LessThan)
	}
	clause += partitionClauseSuffix

	return buildSchema(table, payloadJSON, clause)
}

// PartitionedSchemaBinary returns a schema with LONGBLOB payload and RANGE partitions on created_ts.
func PartitionedSchemaBinary(table string, partitions []Partition) (string, error) {
	if len(partitions) == 0 {
		return "", ErrPartitionsRequired
	}

	clause := partitionClausePrefix
	for i, part := range partitions {
		if part.Name == "" || part.LessThan == "" {
			return "", ErrInvalidPartition
		}
		if i > 0 {
			clause += ","
		}
		clause += fmt.Sprintf("\n\tPARTITION %s VALUES LESS THAN (%s)", part.Name, part.LessThan)
	}
	clause += partitionClauseSuffix

	return buildSchema(table, payloadBinary, clause)
}

func buildSchema(table, payloadType, partitionClause string) (string, error) {
	name, err := sanitizeTableName(table)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf(schemaTemplate, name, payloadType, headersJSON, partitionClause), nil
}
