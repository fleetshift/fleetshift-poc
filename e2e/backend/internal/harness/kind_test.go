package harness

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestHostKindClusterName(t *testing.T) {
	t.Parallel()
	if got := HostKindClusterName("kind-e2e-abcd"); got != "fs--kind-e2e-abcd" {
		t.Fatalf("got %q", got)
	}
}

func TestIsSuiteKindCluster(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   string
		want bool
	}{
		{name: "suite encoded name", in: "fs--kind-e2e-abcd", want: true},
		{name: "foreign encoded name", in: "fs--other", want: false},
		{name: "bare fleetctl id", in: "kind-e2e-abcd", want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := isSuiteKindCluster(tt.in); got != tt.want {
				t.Fatalf("isSuiteKindCluster(%q) = %v, want %v", tt.in, got, tt.want)
			}
		})
	}
}

func TestResolveEngineSocket_PODMAN_SOCKET(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "podman.sock")
	mustListenUnix(t, sock)

	t.Setenv(engineSocketEnv, sock)
	t.Setenv("XDG_RUNTIME_DIR", "/no/such/xdg")
	got, err := resolveEngineSocket()
	if err != nil {
		t.Fatal(err)
	}
	if got != sock {
		t.Fatalf("got %q want %q", got, sock)
	}
}

func TestResolveEngineSocket_XDG(t *testing.T) {
	xdg := t.TempDir()
	if err := os.Mkdir(filepath.Join(xdg, "podman"), 0o700); err != nil {
		t.Fatal(err)
	}
	sock := filepath.Join(xdg, "podman", "podman.sock")
	mustListenUnix(t, sock)

	t.Setenv(engineSocketEnv, "")
	t.Setenv("XDG_RUNTIME_DIR", xdg)
	got, err := resolveEngineSocket()
	if err != nil {
		t.Fatal(err)
	}
	if got != sock {
		t.Fatalf("got %q want %q", got, sock)
	}
}

func TestResolveEngineSocket_Missing(t *testing.T) {
	t.Setenv(engineSocketEnv, "")
	t.Setenv("XDG_RUNTIME_DIR", "")
	_, err := resolveEngineSocket()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "systemctl --user") {
		t.Fatalf("error = %v, want systemctl hint", err)
	}
}

func TestResolveEngineSocket_NotASocket(t *testing.T) {
	p := filepath.Join(t.TempDir(), "not-a-sock")
	if err := os.WriteFile(p, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(engineSocketEnv, p)
	_, err := resolveEngineSocket()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "not a unix socket") {
		t.Fatalf("error = %v", err)
	}
}

func TestRequireLiveUnixSocket_Missing(t *testing.T) {
	err := requireLiveUnixSocket(filepath.Join(t.TempDir(), "gone.sock"))
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestRequireLiveUnixSocket_Stale(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "stale.sock")
	mustStaleUnixSocket(t, sock)
	err := requireLiveUnixSocket(sock)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "not a live unix socket") {
		t.Fatalf("error = %v, want not a live unix socket", err)
	}
}

func TestResolveEngineSocket_StalePODMAN_SOCKET(t *testing.T) {
	sock := filepath.Join(t.TempDir(), "stale.sock")
	mustStaleUnixSocket(t, sock)
	t.Setenv(engineSocketEnv, sock)
	_, err := resolveEngineSocket()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), engineSocketEnv) {
		t.Fatalf("error = %v, want %s", err, engineSocketEnv)
	}
	if !strings.Contains(err.Error(), "not a live unix socket") {
		t.Fatalf("error = %v, want not a live unix socket", err)
	}
}

func TestResolveEngineSocket_PODMAN_SOCKETMissing(t *testing.T) {
	p := filepath.Join(t.TempDir(), "gone.sock")
	t.Setenv(engineSocketEnv, p)
	_, err := resolveEngineSocket()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), engineSocketEnv) {
		t.Fatalf("error = %v, want %s", err, engineSocketEnv)
	}
	if !strings.Contains(err.Error(), p) {
		t.Fatalf("error = %v, want path %s", err, p)
	}
}

func TestResolveEngineSocket_XDGMissing(t *testing.T) {
	xdg := t.TempDir()
	t.Setenv(engineSocketEnv, "")
	t.Setenv("XDG_RUNTIME_DIR", xdg)
	_, err := resolveEngineSocket()
	if err == nil {
		t.Fatal("expected error")
	}
	wantPath := filepath.Join(xdg, "podman", "podman.sock")
	if !strings.Contains(err.Error(), wantPath) {
		t.Fatalf("error = %v, want path %s", err, wantPath)
	}
	if !strings.Contains(err.Error(), "systemctl --user") {
		t.Fatalf("error = %v, want systemctl hint", err)
	}
}

func TestLeftoverKindNodeID(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		line   string
		wantID string
		wantOK bool
	}{
		{name: "suite cluster", line: "abc123\tfs--kind-e2e-abcd", wantID: "abc123", wantOK: true},
		{name: "padded fields", line: "  abc123  \t  fs--kind-e2e-abcd  ", wantID: "abc123", wantOK: true},
		{name: "foreign encoded name", line: "abc123\tfs--other", wantOK: false},
		{name: "space not tab", line: "abc123 fs--kind-e2e-abcd", wantOK: false},
		{name: "empty", line: "", wantOK: false},
		{name: "blank", line: "   ", wantOK: false},
		{name: "missing name", line: "abc123\t", wantOK: false},
		{name: "missing id", line: "\tfs--kind-e2e-abcd", wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			gotID, ok := leftoverKindNodeID(tt.line)
			if ok != tt.wantOK || gotID != tt.wantID {
				t.Fatalf("leftoverKindNodeID(%q) = %q, %v, want %q, %v", tt.line, gotID, ok, tt.wantID, tt.wantOK)
			}
		})
	}
}

func mustListenUnix(t *testing.T, path string) {
	t.Helper()
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
}

// mustStaleUnixSocket creates a unix socket file with no listener.
// net.Listen then Close unlinks the path; bind+listen+close-fd leaves it.
func mustStaleUnixSocket(t *testing.T, path string) {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Bind(fd, &syscall.SockaddrUnix{Name: path}); err != nil {
		_ = syscall.Close(fd)
		t.Fatal(err)
	}
	if err := syscall.Listen(fd, 1); err != nil {
		_ = syscall.Close(fd)
		_ = os.Remove(path)
		t.Fatal(err)
	}
	if err := syscall.Close(fd); err != nil {
		_ = os.Remove(path)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(path) })
}
