package aioinit

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrimaryIPv4_SkipsLoopback(t *testing.T) {
	ip, err := primaryIPv4()
	if err != nil {
		// Some CI sandboxes have no non-loopback IPv4; skip rather than fail.
		t.Skipf("no non-loopback IPv4: %v", err)
	}
	parsed := net.ParseIP(ip)
	if parsed == nil || parsed.To4() == nil || parsed.IsLoopback() {
		t.Fatalf("primaryIPv4 = %q", ip)
	}
}

func TestContainerEngineSocketPresent(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		t.Setenv("CONTAINER_HOST", "")
		if containerEngineSocketPresent() {
			t.Fatal("empty CONTAINER_HOST should be false")
		}
	})
	t.Run("tcp", func(t *testing.T) {
		t.Setenv("CONTAINER_HOST", "tcp://127.0.0.1:1234")
		if containerEngineSocketPresent() {
			t.Fatal("tcp CONTAINER_HOST should be false")
		}
	})
	t.Run("missing unix", func(t *testing.T) {
		t.Setenv("CONTAINER_HOST", "unix:///no/such/socket")
		if containerEngineSocketPresent() {
			t.Fatal("missing socket should be false")
		}
	})
	t.Run("present unix", func(t *testing.T) {
		dir := t.TempDir()
		sockPath := filepath.Join(dir, "engine.sock")
		ln, err := net.Listen("unix", sockPath)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		t.Setenv("CONTAINER_HOST", "unix://"+sockPath)
		if !containerEngineSocketPresent() {
			t.Fatal("existing unix socket should be true")
		}
	})
	t.Run("regular file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "not-a-socket")
		if err := os.WriteFile(path, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
		t.Setenv("CONTAINER_HOST", "unix://"+path)
		if containerEngineSocketPresent() {
			t.Fatal("regular file should be false")
		}
	})
}

func clearEnv(t *testing.T, key string) {
	t.Helper()
	prev, had := os.LookupEnv(key)
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv(key, prev)
			return
		}
		_ = os.Unsetenv(key)
	})
}

func withUnixSocket(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	sockPath := filepath.Join(dir, "engine.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	t.Setenv("CONTAINER_HOST", "unix://"+sockPath)
	return sockPath
}

func TestConfigureKindEnv(t *testing.T) {
	t.Run("no socket removes file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kind.env")
		if err := os.WriteFile(path, []byte("stale=1\n"), 0644); err != nil {
			t.Fatal(err)
		}
		t.Setenv("CONTAINER_HOST", "")
		clearEnv(t, kindExperimentalNetKey)
		if err := ConfigureKindEnv(path, true, ":5556"); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("kind.env still present: %v", err)
		}
	})

	t.Run("socket defaults experimental network", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kind.env")
		withUnixSocket(t)
		clearEnv(t, kindExperimentalNetKey)
		if err := ConfigureKindEnv(path, false, ":5556"); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		body := string(raw)
		if !strings.Contains(body, kindExperimentalNetKey+"="+kindExperimentalNetDefault+"\n") {
			t.Fatalf("kind.env missing network default: %q", body)
		}
		if strings.Contains(body, kindNodeRouteEnvKey+"=") {
			t.Fatalf("Dex-off should omit node route: %q", body)
		}
	})

	t.Run("preserves explicit network override", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kind.env")
		withUnixSocket(t)
		t.Setenv(kindExperimentalNetKey, "custom-net")
		if err := ConfigureKindEnv(path, false, ":5556"); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			raw, _ := os.ReadFile(path)
			t.Fatalf("expected no kind.env when only override is set, got %q err=%v", raw, err)
		}
	})

	t.Run("preserves empty network override", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kind.env")
		withUnixSocket(t)
		t.Setenv(kindExperimentalNetKey, "")
		if err := ConfigureKindEnv(path, false, ":5556"); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			raw, _ := os.ReadFile(path)
			t.Fatalf("expected no kind.env for empty override, got %q err=%v", raw, err)
		}
	})

	t.Run("dex-on adds node route when possible", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kind.env")
		withUnixSocket(t)
		clearEnv(t, kindExperimentalNetKey)
		if _, err := primaryIPv4(); err != nil {
			err := ConfigureKindEnv(path, true, ":5556")
			if err == nil {
				t.Fatal("expected error when no non-loopback IPv4")
			}
			if !strings.Contains(err.Error(), "kind node route backend") {
				t.Fatalf("error = %v, want kind node route backend", err)
			}
			return
		}
		if err := ConfigureKindEnv(path, true, ":5556"); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		body := string(raw)
		if !strings.Contains(body, kindExperimentalNetKey+"="+kindExperimentalNetDefault+"\n") {
			t.Fatalf("kind.env missing network default: %q", body)
		}
		if !strings.Contains(body, kindNodeRouteEnvKey+"=") || !strings.Contains(body, ":5556") {
			t.Fatalf("kind.env missing node route backend: %q", body)
		}
	})
}
