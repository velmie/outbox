package mysql

import (
	"errors"
	"testing"
	"time"
)

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

func TestPlanPartitionChangesAddsDayPartitions(t *testing.T) {
	now := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)
	cfg := PartitionMaintainerConfig{
		Period:    PartitionDay,
		Lookahead: 24 * time.Hour,
		Clock:     fixedClock{now: now},
	}
	info := partitionInfo{
		maxName: "pmax",
		bounds:  map[int64]string{},
		names:   map[string]int64{},
	}

	plan, err := planPartitionChanges(cfg, info)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(plan.add) != 2 {
		t.Fatalf("expected 2 partitions, got %d", len(plan.add))
	}
	if plan.add[0].name != "p20250301" {
		t.Fatalf("unexpected partition name: %s", plan.add[0].name)
	}
	if plan.add[1].name != "p20250302" {
		t.Fatalf("unexpected partition name: %s", plan.add[1].name)
	}
	if plan.add[0].upperBound != time.Date(2025, 3, 2, 0, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("unexpected upper bound: %d", plan.add[0].upperBound)
	}
	if plan.add[1].upperBound != time.Date(2025, 3, 3, 0, 0, 0, 0, time.UTC).Unix() {
		t.Fatalf("unexpected upper bound: %d", plan.add[1].upperBound)
	}
}

func TestPlanPartitionChangesSkipsWhenAhead(t *testing.T) {
	now := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)
	cfg := PartitionMaintainerConfig{
		Period:    PartitionDay,
		Lookahead: 24 * time.Hour,
		Clock:     fixedClock{now: now},
	}
	info := partitionInfo{
		maxName:  "pmax",
		maxUpper: now.Add(48 * time.Hour).Unix(),
		bounds:   map[int64]string{},
		names:    map[string]int64{},
	}

	plan, err := planPartitionChanges(cfg, info)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(plan.add) != 0 {
		t.Fatalf("expected no additions, got %d", len(plan.add))
	}
}

func TestPlanPartitionChangesRetentionDrop(t *testing.T) {
	now := time.Date(2025, 3, 3, 0, 0, 0, 0, time.UTC)
	cfg := PartitionMaintainerConfig{
		Period:    PartitionDay,
		Lookahead: 24 * time.Hour,
		Retention: 24 * time.Hour,
		Clock:     fixedClock{now: now},
	}
	info := partitionInfo{
		maxName: "pmax",
		bounds: map[int64]string{
			now.Add(-48 * time.Hour).Unix(): "p20250301",
			now.Add(-24 * time.Hour).Unix(): "p20250302",
		},
		names: map[string]int64{},
	}

	plan, err := planPartitionChanges(cfg, info)
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	if len(plan.drop) != 2 {
		t.Fatalf("expected 2 drops, got %d", len(plan.drop))
	}
}

func TestPlanPartitionChangesNameConflict(t *testing.T) {
	now := time.Date(2025, 3, 1, 10, 0, 0, 0, time.UTC)
	cfg := PartitionMaintainerConfig{
		Period:    PartitionDay,
		Lookahead: 24 * time.Hour,
		Clock:     fixedClock{now: now},
	}
	info := partitionInfo{
		maxName: "pmax",
		bounds:  map[int64]string{},
		names: map[string]int64{
			"p20250301": time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC).Unix(),
		},
	}

	_, err := planPartitionChanges(cfg, info)
	if err == nil {
		t.Fatalf("expected error")
	}
	if !errors.Is(err, ErrPartitionNameConflict) {
		t.Fatalf("expected ErrPartitionNameConflict, got %v", err)
	}
}

func TestValidatePartitionLayout(t *testing.T) {
	tests := []struct {
		name           string
		engine         string
		method         string
		expression     string
		subpartitioned bool
		want           bool
	}{
		{
			name:       "canonical",
			engine:     "InnoDB",
			method:     "RANGE",
			expression: "`created_ts`",
			want:       true,
		},
		{
			name:       "wrong engine",
			engine:     "MyISAM",
			method:     "RANGE",
			expression: "`created_ts`",
		},
		{
			name:       "range columns",
			engine:     "InnoDB",
			method:     "RANGE COLUMNS",
			expression: "`created_ts`",
		},
		{
			name:       "wrong expression",
			engine:     "InnoDB",
			method:     "RANGE",
			expression: "`status`",
		},
		{
			name:           "subpartitioned",
			engine:         "InnoDB",
			method:         "RANGE",
			expression:     "`created_ts`",
			subpartitioned: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := validPartitionLayout(tt.engine, tt.method, tt.expression, tt.subpartitioned); got != tt.want {
				t.Fatalf("validPartitionLayout() = %v, want %v", got, tt.want)
			}
		})
	}
}
