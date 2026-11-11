package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
)

func TestInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		{},
		{"-id", "invalid"},
		{"-event-type", "delivery", "-limit", "0"},
		{"-event-type", "delivery", "extra"},
	} {
		var out bytes.Buffer
		var diagnostics bytes.Buffer
		code := run(context.Background(), args, "user:password@tcp(localhost:3306)/db", &out, &diagnostics)
		if code != 2 || out.Len() != 0 {
			t.Fatalf("args %v: code %d, output %q", args, code, out.String())
		}
		if strings.Contains(diagnostics.String(), "password") {
			t.Fatal("diagnostics exposed a credential")
		}
	}
}

func TestInvalidDSNDoesNotPrintValue(t *testing.T) {
	var out bytes.Buffer
	var diagnostics bytes.Buffer
	code := run(context.Background(), []string{"-event-type", "delivery"}, "missing-slash-secret", &out, &diagnostics)
	if code != 2 || strings.Contains(diagnostics.String(), "missing-slash-secret") {
		t.Fatalf("code %d, diagnostics %q", code, diagnostics.String())
	}
}
