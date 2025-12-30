package main

import (
	"testing"
	"time"
)

func TestValidateConsumeWindow(t *testing.T) {
	tests := []struct {
		name    string
		cfg     benchConfig
		wantErr bool
	}{
		{
			name:    "no window",
			cfg:     benchConfig{partitionWindow: 0, seedAge: 2160 * time.Hour, seedDays: 90},
			wantErr: false,
		},
		{
			name:    "seeded in past older than window",
			cfg:     benchConfig{partitionWindow: time.Hour, seedAge: 2160 * time.Hour, seedDays: 90},
			wantErr: true,
		},
		{
			name:    "seeded across days with fresh data",
			cfg:     benchConfig{partitionWindow: time.Hour, seedAge: 0, seedDays: 90},
			wantErr: false,
		},
		{
			name:    "single-day seed older than window",
			cfg:     benchConfig{partitionWindow: time.Hour, seedAge: 24 * time.Hour, seedDays: 1},
			wantErr: true,
		},
		{
			name:    "future skew still overlaps",
			cfg:     benchConfig{partitionWindow: time.Hour, seedAge: 48 * time.Hour, seedDays: 90},
			wantErr: false,
		},
		{
			name:    "negative seed age treated as zero",
			cfg:     benchConfig{partitionWindow: time.Hour, seedAge: -time.Hour, seedDays: 1},
			wantErr: false,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			err := validateConsumeWindow(test.cfg)
			if test.wantErr && err == nil {
				t.Fatal("expected error")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
