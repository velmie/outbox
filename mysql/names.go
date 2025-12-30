package mysql

import (
	"fmt"
	"strings"
)

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
