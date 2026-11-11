//go:build integration

package main

import (
	"context"
	"strings"
	"testing"

	"github.com/velmie/outbox"
	"github.com/velmie/outbox/cmd/internal/testutil"
	"github.com/velmie/outbox/mysql"
)

func TestRequeueCommand(t *testing.T) {
	ctx := context.Background()
	env := testutil.StartMySQLContainer(t, ctx)
	ddl, err := mysql.SchemaWithOptions("outbox", mysql.SchemaOptions{
		Retry: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = env.DB.ExecContext(ctx, ddl); err != nil {
		t.Fatal(err)
	}
	store, err := mysql.NewStore(env.DB)
	if err != nil {
		t.Fatal(err)
	}
	var ids []outbox.ID
	for i := 0; i < 2; i++ {
		id, insertErr := store.Enqueue(ctx, env.DB, outbox.Entry{
			AggregateType: "endpoint",
			EventType:     "delivery",
			Payload:       []byte(`{}`),
		})
		if insertErr != nil {
			t.Fatal(insertErr)
		}
		ids = append(ids, id)
	}
	_, err = env.DB.ExecContext(ctx, `UPDATE outbox SET status = ?, attempt_count = 4,
last_error = 'failure', next_attempt_at = DATE_ADD(UTC_TIMESTAMP(6), INTERVAL 1 DAY)`, outbox.StatusDead)
	if err != nil {
		t.Fatal(err)
	}
	bin := testutil.BuildBinary(t, ".")
	code, output := testutil.RunCLIContainer(t, ctx, env.Network.Name, bin, map[string]string{
		"OUTBOX_DSN": env.DSN,
	}, []string{
		"-id", ids[0].String(),
		"-id", ids[1].String(),
		"-aggregate-type", "endpoint",
		"-event-type", "delivery",
		"-limit", "1",
		"-retry-scheduling",
	})
	if strings.Contains(output, env.Password) {
		t.Fatal("command output contains the database password")
	}
	if code != 0 || strings.TrimSpace(output) != "1" {
		t.Fatalf("exit %d, output %q", code, output)
	}
	var pending, cleared int
	err = env.DB.QueryRowContext(ctx, `SELECT COUNT(*), SUM(next_attempt_at IS NULL)
FROM outbox WHERE status = ?`, outbox.StatusPending).Scan(&pending, &cleared)
	if err != nil || pending != 1 || cleared != 1 {
		t.Fatalf("pending %d, cleared %d, err %v", pending, cleared, err)
	}
	code, output = testutil.RunCLIContainer(t, ctx, env.Network.Name, bin, map[string]string{
		"OUTBOX_DSN": env.DSN,
	}, []string{"-id", "invalid"})
	if code != 2 || strings.Contains(output, env.Password) {
		t.Fatalf("invalid arguments: exit %d, output %q", code, output)
	}
}
