package steps

import (
	"errors"
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

func TestRPCNotFound(t *testing.T) {
	t.Parallel()
	exitErr := errors.New("exit status 1")
	tests := []struct {
		name string
		res  harness.FleetctlResult
		want bool
	}{
		{
			name: "grpc code",
			res: harness.FleetctlResult{
				Err:    exitErr,
				Stderr: "Error: rpc error: code = NotFound desc = deployment \"deployments/x\" not found\n",
			},
			want: true,
		},
		{
			name: "desc only",
			res:  harness.FleetctlResult{Err: exitErr, Stderr: "deployment \"x\" not found"},
			want: true,
		},
		{
			name: "code without desc phrase",
			res:  harness.FleetctlResult{Err: exitErr, Stderr: "Error: rpc error: code = NotFound desc = missing"},
			want: true,
		},
		{name: "success", res: harness.FleetctlResult{}},
		{
			name: "connection refused",
			res:  harness.FleetctlResult{Err: exitErr, Stderr: "connection refused"},
		},
		{
			name: "unauthenticated",
			res: harness.FleetctlResult{
				Err:    exitErr,
				Stderr: "Error: rpc error: code = Unauthenticated desc = unauthenticated\n",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := rpcNotFound(tt.res); got != tt.want {
				t.Fatalf("rpcNotFound = %v, want %v", got, tt.want)
			}
		})
	}
}
