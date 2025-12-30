package mysql

import (
	"database/sql"
	"testing"
	"time"
)

func TestNewCleanupMaintainerDefaults(t *testing.T) {
	db := &sql.DB{}
	maintainer, err := NewCleanupMaintainer(db, CleanupMaintainerConfig{
		Table:     "outbox",
		Retention: 24 * time.Hour,
	})
	if err != nil {
		t.Fatalf("expected maintainer, got %v", err)
	}
	if maintainer.cfg.CheckEvery != defaultCleanupEvery {
		t.Fatalf("expected default check interval")
	}
	if maintainer.cfg.Limit != defaultCleanupLimit {
		t.Fatalf("expected default limit")
	}
	if maintainer.cfg.LockName == "" {
		t.Fatalf("expected lock name")
	}
}

func TestNewCleanupMaintainerValidation(t *testing.T) {
	db := &sql.DB{}
	if _, err := NewCleanupMaintainer(nil, CleanupMaintainerConfig{Table: "outbox", Retention: time.Hour}); err != ErrDBRequired {
		t.Fatalf("expected ErrDBRequired, got %v", err)
	}
	if _, err := NewCleanupMaintainer(db, CleanupMaintainerConfig{Table: "outbox", Retention: 0}); err != ErrCleanupRetentionInvalid {
		t.Fatalf("expected ErrCleanupRetentionInvalid, got %v", err)
	}
	if _, err := NewCleanupMaintainer(db, CleanupMaintainerConfig{Table: "outbox", Retention: time.Hour, Limit: -1}); err != ErrCleanupLimitInvalid {
		t.Fatalf("expected ErrCleanupLimitInvalid, got %v", err)
	}
}
