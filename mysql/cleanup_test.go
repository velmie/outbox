package mysql

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/velmie/outbox"
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

func TestCleanupConfirmedResults(t *testing.T) {
	cause := errors.New("delete result unavailable")
	for _, tc := range []struct {
		name    string
		results []cleanupExecResult
		want    CleanupResult
		wantErr error
		limits  []int
	}{
		{name: "processed execution failed", results: []cleanupExecResult{{execErr: cause}}, wantErr: cause, limits: []int{5}},
		{name: "processed affected count unknown", results: []cleanupExecResult{{affected: 99, rowsErr: cause}}, wantErr: cause, limits: []int{5}},
		{name: "dead execution failed", results: []cleanupExecResult{{affected: 2}, {execErr: cause}}, want: CleanupResult{Processed: 2}, wantErr: cause, limits: []int{5, 3}},
		{name: "dead affected count unknown", results: []cleanupExecResult{{affected: 2}, {affected: 99, rowsErr: cause}}, want: CleanupResult{Processed: 2}, wantErr: cause, limits: []int{5, 3}},
		{name: "shared limit", results: []cleanupExecResult{{affected: 2}, {affected: 3}}, want: CleanupResult{Processed: 2, Dead: 3}, limits: []int{5, 3}},
		{name: "processed exhausts limit", results: []cleanupExecResult{{affected: 5}}, want: CleanupResult{Processed: 5}, limits: []int{5}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := NewStore(&sql.DB{})
			require.NoError(t, err)
			exec := &cleanupExecutor{t: t, results: tc.results, limits: tc.limits}
			result, err := store.cleanup(context.Background(), exec, CleanupOptions{Before: time.Now(), Limit: 5, IncludeDead: true})
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.want, result)
			require.Equal(t, len(tc.results), exec.calls)
		})
	}
}

type cleanupExecResult struct {
	affected         int64
	execErr, rowsErr error
}

func (r cleanupExecResult) LastInsertId() (int64, error) { return 0, nil }
func (r cleanupExecResult) RowsAffected() (int64, error) { return r.affected, r.rowsErr }

type cleanupExecutor struct {
	t       *testing.T
	results []cleanupExecResult
	limits  []int
	calls   int
}

func (e *cleanupExecutor) ExecContext(_ context.Context, query string, args ...any) (sql.Result, error) {
	e.t.Helper()
	require.Less(e.t, e.calls, len(e.results), "unexpected DELETE")
	statuses := []outbox.Status{outbox.StatusProcessed, outbox.StatusDead}
	columns := []string{"processed_at", "updated_at"}
	require.Equal(e.t, statuses[e.calls], args[0])
	require.Contains(e.t, query, columns[e.calls])
	require.Equal(e.t, e.limits[e.calls], args[2])
	r := e.results[e.calls]
	e.calls++
	if r.execErr != nil {
		return nil, r.execErr
	}
	return r, nil
}
