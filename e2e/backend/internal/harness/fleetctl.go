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
	return f.runWithConfigDir(ctx, f.configDir, args...)
}

// RunUnauthenticated runs fleetctl with an empty temp --config-dir so the suite
// credentials.json cannot be used. Isolation checks use this, not a second
// --config-dir invented in steps.
func (f *Fixture) RunUnauthenticated(t *testing.T, ctx context.Context, args ...string) FleetctlResult {
	t.Helper()
	return f.runWithConfigDir(ctx, t.TempDir(), args...)
}

// runWithConfigDir is Run using an explicit --config-dir.
func (f *Fixture) runWithConfigDir(ctx context.Context, configDir string, args ...string) FleetctlResult {
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
	f.logf("fleetctl exit=%d dur=%s argv=%s", res.ExitCode, res.Duration.Truncate(time.Millisecond), strings.Join(full, " "))
	if err != nil && strings.TrimSpace(res.Stderr) != "" {
		f.logf("stderr=%s", res.Stderr)
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
