package harness

import (
	"fmt"
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

func TestParsePodmanPort(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr string
	}{
		{name: "arrow loopback", in: "6443/tcp -> 127.0.0.1:41187", want: "https://127.0.0.1:41187"},
		{name: "unspecified ipv4", in: "6443/tcp -> 0.0.0.0:1980", want: "https://127.0.0.1:1980"},
		{name: "bare hostport", in: "0.0.0.0:1980", want: "https://127.0.0.1:1980"},
		{name: "ipv6 unspecified", in: "6443/tcp -> [::]:6443", want: "https://127.0.0.1:6443"},
		{
			name: "prefer ipv4 loopback",
			in:   "6443/tcp -> [::1]:6443\n6443/tcp -> 127.0.0.1:41187",
			want: "https://127.0.0.1:41187",
		},
		{name: "empty", in: "  \n", wantErr: "parse podman port"},
		{name: "garbage", in: "not a port", wantErr: "parse podman port"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parsePodmanPort(tt.in)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want %s", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
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
	sock := filepath.Join(shortUnixDir(t), "podman.sock")
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
	xdg := shortUnixDir(t)
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
	setLinuxEngineHost(t, true)
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

func TestResolveEngineSocket_DarwinMissing(t *testing.T) {
	setLinuxEngineHost(t, false)
	skipDockerCompatSocket(t)
	setLookupRemoteEngineSocket(t, func() (string, error) {
		return "", fmt.Errorf("machine stopped")
	})
	t.Setenv(engineSocketEnv, "")
	t.Setenv("XDG_RUNTIME_DIR", "")
	_, err := resolveEngineSocket()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "podman machine start") {
		t.Fatalf("error = %v, want podman machine start", err)
	}
}

func TestResolveEngineSocket_DarwinRemote(t *testing.T) {
	setLinuxEngineHost(t, false)
	skipDockerCompatSocket(t)
	sock := filepath.Join(shortUnixDir(t), "podman.sock")
	mustListenUnix(t, sock)
	setLookupRemoteEngineSocket(t, func() (string, error) {
		return sock, nil
	})
	t.Setenv(engineSocketEnv, "")
	t.Setenv("XDG_RUNTIME_DIR", "")
	got, err := resolveEngineSocket()
	if err != nil {
		t.Fatal(err)
	}
	if got != sock {
		t.Fatalf("got %q want %q", got, sock)
	}
}

func TestResolveEngineSocket_DarwinRemoteMissing(t *testing.T) {
	setLinuxEngineHost(t, false)
	skipDockerCompatSocket(t)
	want := filepath.Join(t.TempDir(), "gone.sock")
	setLookupRemoteEngineSocket(t, func() (string, error) {
		return want, nil
	})
	t.Setenv(engineSocketEnv, "")
	t.Setenv("XDG_RUNTIME_DIR", "")
	_, err := resolveEngineSocket()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want path %s", err, want)
	}
	if !strings.Contains(err.Error(), "podman machine start") {
		t.Fatalf("error = %v, want podman machine start", err)
	}
}

func TestResolveEngineSocket_DarwinDockerSock(t *testing.T) {
	setLinuxEngineHost(t, false)
	sock := filepath.Join(shortUnixDir(t), "docker.sock")
	mustListenUnix(t, sock)
	setDockerCompatSocketPath(t, sock)
	setLookupRemoteEngineSocket(t, func() (string, error) {
		t.Fatal("podman info should not run when docker.sock is live")
		return "", nil
	})
	t.Setenv(engineSocketEnv, "")
	t.Setenv("XDG_RUNTIME_DIR", "")
	got, err := resolveEngineSocket()
	if err != nil {
		t.Fatal(err)
	}
	if got != sock {
		t.Fatalf("got %q want %q", got, sock)
	}
}

func TestResolveEngineSocket_LinuxIgnoresDockerSock(t *testing.T) {
	setLinuxEngineHost(t, true)
	sock := filepath.Join(shortUnixDir(t), "docker.sock")
	mustListenUnix(t, sock)
	setDockerCompatSocketPath(t, sock)
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
	sock := filepath.Join(shortUnixDir(t), "stale.sock")
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
	sock := filepath.Join(shortUnixDir(t), "stale.sock")
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

func TestParseRemoteSocketPath(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		raw     string
		want    string
		wantErr string
	}{
		{name: "unix prefix", raw: "unix:///run/user/501/podman/podman.sock", want: "/run/user/501/podman/podman.sock"},
		{name: "bare path", raw: "/run/podman/podman.sock", want: "/run/podman/podman.sock"},
		{name: "whitespace", raw: "  unix:///tmp/p.sock\n", want: "/tmp/p.sock"},
		{name: "empty", raw: "  \n", wantErr: "empty remote socket path"},
		{name: "tcp", raw: "tcp://127.0.0.1:2375", wantErr: "not a unix path"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := parseRemoteSocketPath(tt.raw)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatal("expected error")
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want %s", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("got %q want %q", got, tt.want)
			}
		})
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

// shortUnixDir returns a temp directory short enough for Darwin sun_path.
// t.TempDir() under $TMPDIR (/var/folders/...) is often too long and listen
// fails with "bind: invalid argument".
func shortUnixDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "fs-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return dir
}

func setLinuxEngineHost(t *testing.T, v bool) {
	t.Helper()
	prev := linuxEngineHost
	linuxEngineHost = v
	t.Cleanup(func() { linuxEngineHost = prev })
}

func setLookupRemoteEngineSocket(t *testing.T, fn func() (string, error)) {
	t.Helper()
	prev := lookupRemoteEngineSocket
	lookupRemoteEngineSocket = fn
	t.Cleanup(func() { lookupRemoteEngineSocket = prev })
}

func setDockerCompatSocketPath(t *testing.T, path string) {
	t.Helper()
	prev := dockerCompatSocketPath
	dockerCompatSocketPath = path
	t.Cleanup(func() { dockerCompatSocketPath = prev })
}

func skipDockerCompatSocket(t *testing.T) {
	t.Helper()
	setDockerCompatSocketPath(t, filepath.Join(t.TempDir(), "gone.sock"))
}
