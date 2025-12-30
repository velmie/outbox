//go:build integration

package main

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/velmie/outbox/cmd/internal/testutil"
	"github.com/velmie/outbox/mysql"
)

func TestPartitionsCLIContainer(t *testing.T) {
	ctx := context.Background()
	env := testutil.StartMySQLContainer(t, ctx)

	schema, err := mysql.PartitionedSchema("outbox", []mysql.Partition{
		{Name: "pmax", LessThan: "MAXVALUE"},
	})
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if _, err := env.DB.ExecContext(ctx, schema); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	bin := testutil.BuildBinary(t, ".")
	args := []string{
		"-dsn", env.DSN,
		"-table", "outbox",
		"-period", "day",
		"-lookahead", "48h",
		"-once",
	}
	code, logs := testutil.RunCLIContainer(t, ctx, env.Network.Name, bin, args)
	if code != 0 {
		t.Fatalf("partitions exit code %d logs: %s", code, logs)
	}

	partitions, err := fetchPartitionNames(ctx, env.DB, "outbox")
	if err != nil {
		t.Fatalf("list partitions: %v", err)
	}

	if _, ok := partitions["pmax"]; !ok {
		t.Fatalf("expected pmax partition to remain")
	}

	lookahead := 48 * time.Hour
	now := time.Now().UTC()
	expected := expectedDayPartitions(now, lookahead)
	if !containsAll(partitions, expected) {
		fallback := expectedDayPartitions(now.Add(-24*time.Hour), lookahead)
		if !containsAll(partitions, fallback) {
			t.Fatalf("expected partitions missing, got %v", sortedKeys(partitions))
		}
	}
}

func fetchPartitionNames(ctx context.Context, db *sql.DB, table string) (map[string]struct{}, error) {
	rows, err := db.QueryContext(
		ctx,
		"SELECT PARTITION_NAME FROM information_schema.PARTITIONS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
		table,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	parts := make(map[string]struct{})
	for rows.Next() {
		var name sql.NullString
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if name.Valid && name.String != "" {
			parts[name.String] = struct{}{}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return parts, nil
}

func expectedDayPartitions(now time.Time, lookahead time.Duration) []string {
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	end := now.Add(lookahead)

	var names []string
	for {
		next := start.AddDate(0, 0, 1)
		names = append(names, dayPartitionName(start))
		if !next.Before(end) {
			break
		}
		start = next
	}

	return names
}

func dayPartitionName(t time.Time) string {
	return fmt.Sprintf("p%04d%02d%02d", t.Year(), int(t.Month()), t.Day())
}

func containsAll(set map[string]struct{}, names []string) bool {
	for _, name := range names {
		if _, ok := set[name]; !ok {
			return false
		}
	}

	return true
}

func sortedKeys(set map[string]struct{}) []string {
	keys := make([]string, 0, len(set))
	for key := range set {
		keys = append(keys, key)
	}
	for i := 0; i < len(keys)-1; i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[j] < keys[i] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	return keys
}
