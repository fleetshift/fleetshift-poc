package steps

import (
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

func TestClip(t *testing.T) {
	t.Parallel()
	if got := clip("ab", 4); got != "ab" {
		t.Fatalf("got %q", got)
	}
	if got := clip("abcd", 2); got != "ab…" {
		t.Fatalf("got %q", got)
	}
}

func TestFleetctlDetail(t *testing.T) {
	t.Parallel()
	got := fleetctlDetail(harness.FleetctlResult{Stderr: " boom ", Stdout: " {\"state\":\"STATE_CREATING\"} "})
	if !strings.Contains(got, "stderr=boom") || !strings.Contains(got, "stdout={\"state\":\"STATE_CREATING\"}") {
		t.Fatalf("got %q", got)
	}
}
