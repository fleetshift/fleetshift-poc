package harness

import (
	"strings"
	"testing"
)

func TestFleetctlArgs(t *testing.T) {
	got := fleetctlArgs("/tmp/state")
	want := []string{
		"--config-dir", "/tmp/state",
		"--insecure-storage",
		"--server", GRPCTarget,
		"--output", "json",
	}
	if strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("got %v want %v", got, want)
	}
}
