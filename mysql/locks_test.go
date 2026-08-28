package mysql

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestMaintainerLockNameValidation(t *testing.T) {
	db := &sql.DB{}
	validUnicodeName := strings.Repeat("é", 64)
	tooLongName := strings.Repeat("a", 65)
	longTableName := strings.Repeat("a", 64)

	tests := []struct {
		name    string
		lock    string
		table   string
		wantErr bool
	}{
		{name: "64 Unicode characters", lock: validUnicodeName},
		{name: "65 characters", lock: tooLongName, wantErr: true},
		{name: "default exceeds bound", table: longTableName, wantErr: true},
	}

	for _, tt := range tests {
		t.Run("cleanup "+tt.name, func(t *testing.T) {
			table := tt.table
			if table == "" {
				table = "outbox"
			}
			_, err := NewCleanupMaintainer(db, CleanupMaintainerConfig{
				Table:     table,
				Retention: time.Hour,
				LockName:  tt.lock,
			})
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewCleanupMaintainer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})

		t.Run("partitions "+tt.name, func(t *testing.T) {
			table := tt.table
			if table == "" {
				table = "outbox"
			}
			_, err := NewPartitionMaintainer(db, PartitionMaintainerConfig{
				Table:    table,
				Period:   PartitionDay,
				LockName: tt.lock,
			})
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewPartitionMaintainer() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateNamedLockRelease(t *testing.T) {
	tests := []struct {
		name    string
		result  sql.NullInt64
		wantErr bool
	}{
		{name: "released", result: sql.NullInt64{Int64: 1, Valid: true}},
		{name: "not owned", result: sql.NullInt64{Valid: true}, wantErr: true},
		{name: "not found", result: sql.NullInt64{}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateNamedLockRelease(tt.result)
			if (err != nil) != tt.wantErr {
				t.Fatalf("validateNamedLockRelease() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
