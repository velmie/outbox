package main

import (
	"context"
	"errors"
	"testing"
)

func TestStopRelayJoinsAndPreservesFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	producerErr := errors.New("producer failed")
	relayErr := errors.New("relay failed")
	relayDone := make(chan error)
	result := make(chan error, 1)
	go func() {
		result <- stopRelay(cancel, relayDone, producerErr)
	}()
	<-ctx.Done()
	select {
	case err := <-result:
		t.Fatalf("returned before relay stopped: %v", err)
	default:
	}
	relayDone <- relayErr
	err := <-result
	if !errors.Is(err, producerErr) || !errors.Is(err, relayErr) {
		t.Fatalf("lost failure cause: %v", err)
	}
}

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
