package mysql_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/velmie/outbox/mysql"
)

func TestMaintenanceTerminalDiagnostics(t *testing.T) {
	operationErr := errors.New("operation cause")
	releaseErr := errors.New("release cause")
	for _, kind := range []string{"cleanup", "partitions"} {
		for _, tc := range []struct {
			name              string
			busy              bool
			opErr, releaseErr error
			outcome, stage    string
		}{
			{name: "success", outcome: "succeeded"},
			{name: "busy", busy: true, outcome: "skipped"},
			{name: "operation failure", opErr: operationErr, outcome: "failed", stage: map[string]string{"cleanup": "cleanup", "partitions": "inspect"}[kind]},
			{name: "release failure", releaseErr: releaseErr, outcome: "failed", stage: "release_lock"},
			{name: "joined failure", opErr: operationErr, releaseErr: releaseErr, outcome: "failed", stage: "operation_and_release"},
		} {
			t.Run(kind+"/"+tc.name, func(t *testing.T) {
				logger := &maintenanceLogger{}
				db := sql.OpenDB(maintenanceConnector{busy: tc.busy, opErr: tc.opErr, releaseErr: tc.releaseErr})
				t.Cleanup(func() { _ = db.Close() })
				var err error
				if kind == "cleanup" {
					m, e := mysql.NewCleanupMaintainer(db, mysql.CleanupMaintainerConfig{Retention: time.Hour, Logger: logger})
					require.NoError(t, e)
					_, err = m.Ensure(context.Background())
				} else {
					m, e := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{Table: "outbox.outbox", Period: mysql.PartitionDay, Logger: logger})
					require.NoError(t, e)
					err = m.Ensure(context.Background())
				}
				if tc.opErr != nil {
					require.ErrorIs(t, err, tc.opErr)
				}
				if tc.releaseErr != nil {
					require.ErrorIs(t, err, tc.releaseErr)
				}
				if tc.opErr == nil && tc.releaseErr == nil {
					require.NoError(t, err)
				}
				require.Len(t, logger.events, 1)
				event := logger.events[0]
				suffix := map[string]string{"succeeded": "completed", "skipped": "skipped", "failed": "failed"}[tc.outcome]
				require.Equal(t, kind+"."+suffix, event["event"])
				require.Equal(t, kind+".ensure", event["operation"])
				require.Equal(t, tc.outcome, event["outcome"])
				if tc.stage != "" {
					require.Equal(t, tc.stage, event["stage"])
					require.Same(t, err, event["err"])
				}
				if tc.busy {
					require.Equal(t, "lock_busy", event["reason"])
				}
			})
		}
	}
}

func TestPartitionExpansionDiagnosticsWithReleaseFailure(t *testing.T) {
	releaseErr := errors.New("release cause")
	ddlErr := errors.New("ddl cause")
	for _, failDDL := range []bool{false, true} {
		t.Run(map[bool]string{false: "ddl succeeded", true: "ddl failed"}[failDDL], func(t *testing.T) {
			connector := maintenanceConnector{expand: true, releaseErr: releaseErr}
			if failDDL {
				connector.ddlErr = ddlErr
			}
			db := sql.OpenDB(connector)
			defer db.Close()
			logger := &maintenanceLogger{}
			m, err := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{Table: "outbox.outbox", Period: mysql.PartitionDay, Logger: logger})
			require.NoError(t, err)
			err = m.Ensure(context.Background())
			require.ErrorIs(t, err, releaseErr)
			want := []string{"partitions.expansion_started", "partitions.expansion_completed", "partitions.failed"}
			stage := "release_lock"
			if failDDL {
				require.ErrorIs(t, err, ddlErr)
				want = []string{"partitions.expansion_started", "partitions.failed"}
				stage = "operation_and_release"
			}
			events := make([]string, 0, len(logger.events))
			for _, e := range logger.events {
				events = append(events, e["event"].(string))
			}
			require.Equal(t, want, events)
			last := logger.events[len(logger.events)-1]
			require.Equal(t, stage, last["stage"])
			require.Same(t, err, last["err"])
		})
	}
}

func TestMaintenancePanicDoesNotReportCompletion(t *testing.T) {
	for _, kind := range []string{"cleanup", "partitions"} {
		t.Run(kind, func(t *testing.T) {
			logger := &maintenanceLogger{}
			db := sql.OpenDB(maintenanceConnector{})
			defer db.Close()
			if kind == "cleanup" {
				m, err := mysql.NewCleanupMaintainer(db, mysql.CleanupMaintainerConfig{Retention: time.Hour, Clock: maintenancePanicClock{}, Logger: logger})
				require.NoError(t, err)
				require.Panics(t, func() { _, _ = m.Ensure(context.Background()) })
			} else {
				m, err := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{Table: "outbox.outbox", Period: mysql.PartitionDay, Clock: maintenancePanicClock{}, Logger: logger})
				require.NoError(t, err)
				require.Panics(t, func() { _ = m.Ensure(context.Background()) })
			}
			require.Empty(t, logger.events)
		})
	}
}

func TestMaintenanceRunDoesNotDuplicateFailure(t *testing.T) {
	for _, kind := range []string{"cleanup", "partitions"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			logger := &maintenanceLogger{afterRecord: cancel}
			db := sql.OpenDB(maintenanceConnector{opErr: errors.New("operation cause")})
			defer db.Close()
			var err error
			if kind == "cleanup" {
				m, e := mysql.NewCleanupMaintainer(db, mysql.CleanupMaintainerConfig{Retention: time.Hour, Logger: logger})
				require.NoError(t, e)
				err = m.Run(ctx)
			} else {
				m, e := mysql.NewPartitionMaintainer(db, mysql.PartitionMaintainerConfig{Table: "outbox.outbox", Period: mysql.PartitionDay, Logger: logger})
				require.NoError(t, e)
				err = m.Run(ctx)
			}
			require.ErrorIs(t, err, context.Canceled)
			require.Len(t, logger.events, 1)
			require.Equal(t, kind+".failed", logger.events[0]["event"])
		})
	}
}

// The consumer deliberately discards human messages and classifies only fields.
type maintenanceLogger struct {
	events      []map[string]any
	afterRecord func()
}

func (l *maintenanceLogger) Debug(_ string, args ...any) { l.record(args) }
func (l *maintenanceLogger) Info(_ string, args ...any)  { l.record(args) }
func (l *maintenanceLogger) Warn(_ string, args ...any)  { l.record(args) }
func (l *maintenanceLogger) Error(_ string, args ...any) { l.record(args) }
func (l *maintenanceLogger) record(args []any) {
	fields := map[string]any{}
	for i := 0; i+1 < len(args); i += 2 {
		fields[args[i].(string)] = args[i+1]
	}
	l.events = append(l.events, fields)
	if l.afterRecord != nil {
		l.afterRecord()
	}
}

type maintenanceConnector struct {
	expand            bool
	ddlErr            error
	busy              bool
	opErr, releaseErr error
}

func (c maintenanceConnector) Connect(context.Context) (driver.Conn, error) {
	return &maintenanceConn{c}, nil
}
func (c maintenanceConnector) Driver() driver.Driver { return maintenanceDriver{} }

type maintenanceDriver struct{}

func (maintenanceDriver) Open(string) (driver.Conn, error) {
	return nil, errors.New("connector required")
}

type maintenanceConn struct{ maintenanceConnector }

func (*maintenanceConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (*maintenanceConn) Close() error              { return nil }
func (*maintenanceConn) Begin() (driver.Tx, error) { return nil, errors.New("unexpected transaction") }
func (c *maintenanceConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	if c.ddlErr != nil {
		return nil, c.ddlErr
	}
	if c.opErr != nil {
		return nil, c.opErr
	}
	return driver.RowsAffected(0), nil
}
func (c *maintenanceConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	switch {
	case strings.Contains(q, "GET_LOCK"):
		if c.busy {
			return &maintenanceRows{values: [][]driver.Value{{int64(0)}}}, nil
		}
		return &maintenanceRows{values: [][]driver.Value{{int64(1)}}}, nil
	case strings.Contains(q, "RELEASE_LOCK"):
		if c.releaseErr != nil {
			return nil, c.releaseErr
		}
		return &maintenanceRows{values: [][]driver.Value{{int64(1)}}}, nil
	default:
		if c.opErr != nil {
			return nil, c.opErr
		}
		if c.expand {
			return &maintenanceRows{values: [][]driver.Value{{"InnoDB", "pmax", "MAXVALUE", "RANGE", "created_ts", nil}}, width: 6}, nil
		}
		return &maintenanceRows{values: [][]driver.Value{{"InnoDB", "p20990101", "4070995200", "RANGE", "created_ts", nil}, {"InnoDB", "pmax", "MAXVALUE", "RANGE", "created_ts", nil}}, width: 6}, nil
	}
}

type maintenanceRows struct {
	values [][]driver.Value
	width  int
}

func (r *maintenanceRows) Columns() []string {
	n := r.width
	if n == 0 {
		n = 1
	}
	return make([]string, n)
}
func (*maintenanceRows) Close() error { return nil }
func (r *maintenanceRows) Next(dest []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	copy(dest, r.values[0])
	r.values = r.values[1:]
	return nil
}

type maintenancePanicClock struct{}

func (maintenancePanicClock) Now() time.Time { panic("clock panic") }
