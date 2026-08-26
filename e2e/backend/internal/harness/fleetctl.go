package harness

import (
	"bytes"
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// FleetctlResult is one fleetctl invocation. Stdout/stderr may contain secrets;
// never print credentials.json or AUTH_URL query strings.
type FleetctlResult struct {
	Args     []string
	ExitCode int
	Duration time.Duration
	Stdout   string
	Stderr   string
	Err      error
}

// Run runs host fleetctl with the suite --config-dir and JSON output.
func (f *Fixture) Run(ctx context.Context, args ...string) FleetctlResult {
	return f.RunWithConfigDir(ctx, f.configDir, args...)
}

// RunWithConfigDir runs host fleetctl with configDir as --config-dir and JSON output.
func (f *Fixture) RunWithConfigDir(ctx context.Context, configDir string, args ...string) FleetctlResult {
	full := append(fleetctlArgs(configDir), args...)
	start := time.Now()
	cmd := exec.CommandContext(ctx, f.fleetctl, full...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	res := FleetctlResult{
		Args:     full,
		Duration: time.Since(start),
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		Err:      err,
	}
	if cmd.ProcessState != nil {
		res.ExitCode = cmd.ProcessState.ExitCode()
	} else if err != nil {
		res.ExitCode = -1
	}
	return res
}

// RunUnauthenticated runs fleetctl with an empty temp --config-dir so the suite
// credentials.json cannot be used. Isolation checks use this, not a second
// --config-dir invented in steps. The Unauthenticated stderr this produces is
// expected; begin/end log lines mark it so a later failure dump is not mistaken
// for a real auth outage.
func (f *Fixture) RunUnauthenticated(t *testing.T, ctx context.Context, args ...string) FleetctlResult {
	t.Helper()
	const reason = "empty --config-dir (credential isolation; suite tokens must not apply)"
	f.logf("begin expected Unauthenticated: %s", reason)
	res := f.RunWithConfigDir(ctx, t.TempDir(), args...)
	if unauthenticatedRPC(res) {
		f.logf("end expected Unauthenticated: %s", reason)
	} else {
		f.logf("did not get expected Unauthenticated: %s (exit=%d)", reason, res.ExitCode)
	}
	return res
}

// fleetctlArgs is the suite-wide fleetctl flag prefix for configDir.
func fleetctlArgs(configDir string) []string {
	return []string{
		"--config-dir", configDir,
		"--insecure-storage",
		"--server", GRPCTarget,
		"--output", "json",
	}
}

// unauthenticatedRPC reports whether fleetctl failed with an Unauthenticated RPC.
func unauthenticatedRPC(res FleetctlResult) bool {
	if res.Err == nil {
		return false
	}
	combined := strings.ToLower(res.Stderr + " " + res.Err.Error())
	return strings.Contains(combined, "unauthenticated")
}
