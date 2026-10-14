package aioinit_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/deploy/aio/internal/aioinit"
)

const (
	testIP       = "127.0.0.1"
	testHost     = "fleetshift-sandbox.localhost"
	testMarker   = "fleetshift-aio"
	wantLine     = "127.0.0.1 fleetshift-sandbox.localhost # fleetshift-aio"
	originalBody = "127.0.0.1 localhost\n::1 localhost ip6-localhost\n"
)

func TestEnsureHostsAlias_AppendsMarkedLine(t *testing.T) {
	path := writeHosts(t, originalBody)
	if err := aioinit.EnsureHostsAlias(path, testIP, testHost, testMarker); err != nil {
		t.Fatal(err)
	}
	body := readHosts(t, path)
	if !strings.HasPrefix(body, originalBody) {
		t.Fatalf("existing content was not preserved:\n%s", body)
	}
	if !strings.Contains(body, wantLine+"\n") {
		t.Fatalf("missing alias line:\n%s", body)
	}
	if strings.Count(body, testHost) != 1 {
		t.Fatalf("hostname appeared more than once:\n%s", body)
	}
}

func TestEnsureHostsAlias_IdempotentExactLine(t *testing.T) {
	path := writeHosts(t, originalBody+wantLine+"\n")
	if err := aioinit.EnsureHostsAlias(path, testIP, testHost, testMarker); err != nil {
		t.Fatal(err)
	}
	body := readHosts(t, path)
	if strings.Count(body, wantLine) != 1 {
		t.Fatalf("exact line was duplicated:\n%s", body)
	}
}

func TestEnsureHostsAlias_IdempotentExistingMapping(t *testing.T) {
	path := writeHosts(t, "127.0.0.1 localhost fleetshift-sandbox.localhost\n")
	if err := aioinit.EnsureHostsAlias(path, testIP, testHost, testMarker); err != nil {
		t.Fatal(err)
	}
	body := readHosts(t, path)
	if strings.Contains(body, wantLine) {
		t.Fatalf("appended a second mapping for the same IP:\n%s", body)
	}
}

func TestEnsureHostsAlias_ConflictingMapping(t *testing.T) {
	path := writeHosts(t, "10.0.0.1 fleetshift-sandbox.localhost\n")
	err := aioinit.EnsureHostsAlias(path, testIP, testHost, testMarker)
	if err == nil || !strings.Contains(err.Error(), "conflicting hosts mapping") {
		t.Fatalf("EnsureHostsAlias() = %v, want conflicting hosts mapping", err)
	}
	if readHosts(t, path) != "10.0.0.1 fleetshift-sandbox.localhost\n" {
		t.Fatal("conflicting file was modified")
	}
}

func TestEnsureHostsAlias_MissingNewlineStillAppends(t *testing.T) {
	path := writeHosts(t, "127.0.0.1 localhost")
	if err := aioinit.EnsureHostsAlias(path, testIP, testHost, testMarker); err != nil {
		t.Fatal(err)
	}
	body := readHosts(t, path)
	if !strings.Contains(body, "127.0.0.1 localhost\n"+wantLine+"\n") {
		t.Fatalf("expected newline before append:\n%q", body)
	}
}

func TestEnsureHostsAlias_ReadOnly(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write a 0444 file")
	}
	path := writeHosts(t, originalBody)
	if err := os.Chmod(path, 0444); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(path, 0644) })
	err := aioinit.EnsureHostsAlias(path, testIP, testHost, testMarker)
	if err == nil {
		t.Fatal("expected read-only failure")
	}
}

func TestEnsureHostsAlias_MissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "no-such-hosts")
	err := aioinit.EnsureHostsAlias(path, testIP, testHost, testMarker)
	if err == nil || !strings.Contains(err.Error(), "read hosts file") {
		t.Fatalf("EnsureHostsAlias() = %v, want read hosts file", err)
	}
}

func writeHosts(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "hosts")
	if err := os.WriteFile(path, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	return path
}

func readHosts(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
