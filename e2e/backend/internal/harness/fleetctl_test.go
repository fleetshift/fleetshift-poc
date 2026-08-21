package harness

import (
	"errors"
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

func TestUnauthenticatedRPC(t *testing.T) {
	t.Parallel()
	exitErr := errors.New("exit status 1")
	tests := []struct {
		name string
		res  FleetctlResult
		want bool
	}{
		{
			name: "stderr unauthenticated",
			res: FleetctlResult{
				Err:    exitErr,
				Stderr: "Error: rpc error: code = Unauthenticated desc = unauthenticated\n",
			},
			want: true,
		},
		{name: "success", res: FleetctlResult{}},
		{
			name: "other error",
			res:  FleetctlResult{Err: exitErr, Stderr: "connection refused"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := unauthenticatedRPC(tt.res); got != tt.want {
				t.Fatalf("unauthenticatedRPC = %v, want %v", got, tt.want)
			}
		})
	}
}
