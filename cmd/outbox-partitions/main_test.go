package main

import "testing"

func TestResolveDSN(t *testing.T) {
	t.Setenv(dsnEnvName, "from-environment")

	if got := resolveDSN(""); got != "from-environment" {
		t.Fatalf("environment DSN = %q, want %q", got, "from-environment")
	}
	if got := resolveDSN("from-flag"); got != "from-flag" {
		t.Fatalf("flag DSN = %q, want %q", got, "from-flag")
	}
}

func TestResolveDSNMissing(t *testing.T) {
	t.Setenv(dsnEnvName, "")

	if got := resolveDSN(""); got != "" {
		t.Fatalf("DSN = %q, want empty", got)
	}
}
