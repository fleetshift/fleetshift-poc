package test

import (
	"os"
	"path/filepath"
	"testing"
)

// findAIORoot walks from the test cwd to deploy/aio (s6/s6-rc.d and go.mod).
// Nx and `go test` use different working directories.
func findAIORoot(t *testing.T) string {
	t.Helper()
	dir := walkUp(t, func(dir string) bool {
		_, s6 := os.Stat(filepath.Join(dir, "s6", "s6-rc.d"))
		_, mod := os.Stat(filepath.Join(dir, "go.mod"))
		return s6 == nil && mod == nil
	})
	if dir == "" {
		t.Fatal("deploy/aio root not found (expected s6/s6-rc.d and go.mod)")
	}
	return dir
}

// findRepoRoot walks from the test cwd to the monorepo root
// (Dockerfile.fleetshift). That file is the AIO assembly Dockerfile, not
// under deploy/aio.
func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir := walkUp(t, func(dir string) bool {
		_, err := os.Stat(filepath.Join(dir, "Dockerfile.fleetshift"))
		return err == nil
	})
	if dir == "" {
		t.Fatal("repo root not found (expected Dockerfile.fleetshift)")
	}
	return dir
}

// walkUp returns the nearest ancestor of the cwd (inclusive) for which found
// is true, or "" if none.
func walkUp(t *testing.T, found func(dir string) bool) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	dir := wd
	for {
		if found(dir) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}
