package mysql

import (
	"fmt"
	"strings"
)

const maxPartitionNameLength = 64

func sanitizeTableName(name string) (string, error) {
	if name == "" {
		return "", ErrTableNameRequired
	}
	parts := strings.Split(name, ".")
	for _, part := range parts {
		if part == "" {
			return "", fmt.Errorf("%w: %s", ErrInvalidTableName, name)
		}
		for _, r := range part {
			if r == '_' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
				continue
			}

			return "", fmt.Errorf("%w: %s", ErrInvalidTableName, name)
		}
	}

	return name, nil
}

func quotePartitionName(name string) (string, error) {
	if name == "" || len(name) > maxPartitionNameLength {
		return "", ErrInvalidPartition
	}
	for i := 0; i < len(name); i++ {
		char := name[i]
		if char == '_' || (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') {
			continue
		}
		if i > 0 && char >= '0' && char <= '9' {
			continue
		}

		return "", ErrInvalidPartition
	}

	return "`" + name + "`", nil
}
