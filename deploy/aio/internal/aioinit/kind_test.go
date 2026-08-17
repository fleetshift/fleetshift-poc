package aioinit

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
		sockPath := listenUnixSocket(t)
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

// listenUnixSocket creates a listening unix socket under /tmp.
// macOS sun_path is ~104 bytes; t.TempDir() under $TMPDIR (/var/folders/...)
// is often too long and listen fails with "bind: invalid argument".
func listenUnixSocket(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "fs-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	sockPath := filepath.Join(dir, "engine.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return sockPath
}

func withUnixSocket(t *testing.T) string {
	t.Helper()
	sockPath := listenUnixSocket(t)
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
		if err := ConfigureKindEnv(path, true, ":8085"); err != nil {
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
		if err := ConfigureKindEnv(path, false, ":8085"); err != nil {
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
		if strings.Contains(body, loopbackForwardToEnvKey+"=") {
			t.Fatalf("Dex-off should omit loopback forward: %q", body)
		}
		if strings.Contains(body, loopbackIssuerHostEnvKey+"=") {
			t.Fatalf("Dex-off should omit issuer host: %q", body)
		}
	})

	t.Run("preserves explicit network override", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kind.env")
		withUnixSocket(t)
		t.Setenv(kindExperimentalNetKey, "custom-net")
		if err := ConfigureKindEnv(path, false, ":8085"); err != nil {
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
		if err := ConfigureKindEnv(path, false, ":8085"); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			raw, _ := os.ReadFile(path)
			t.Fatalf("expected no kind.env for empty override, got %q err=%v", raw, err)
		}
	})

	t.Run("dex-on adds fleetshift alias loopback forward", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kind.env")
		withUnixSocket(t)
		clearEnv(t, kindExperimentalNetKey)
		clearEnv(t, loopbackForwardToEnvKey)
		clearEnv(t, loopbackIssuerHostEnvKey)
		if err := ConfigureKindEnv(path, true, ":8085"); err != nil {
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
		want := loopbackForwardToEnvKey + "=fleetshift:8085\n"
		if !strings.Contains(body, want) {
			t.Fatalf("kind.env missing %q, got %q", want, body)
		}
		wantHost := loopbackIssuerHostEnvKey + "=" + PublicHost + "\n"
		if !strings.Contains(body, wantHost) {
			t.Fatalf("kind.env missing %q, got %q", wantHost, body)
		}
	})

	t.Run("preserves explicit loopback forward override", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kind.env")
		withUnixSocket(t)
		clearEnv(t, kindExperimentalNetKey)
		t.Setenv(loopbackForwardToEnvKey, "10.89.0.2:5556")
		if err := ConfigureKindEnv(path, true, ":8085"); err != nil {
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
		if strings.Contains(body, loopbackForwardToEnvKey+"=") {
			t.Fatalf("explicit loopback forward should not be rewritten: %q", body)
		}
	})

	t.Run("preserves empty loopback forward override", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kind.env")
		withUnixSocket(t)
		clearEnv(t, kindExperimentalNetKey)
		t.Setenv(loopbackForwardToEnvKey, "")
		if err := ConfigureKindEnv(path, true, ":8085"); err != nil {
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
		if strings.Contains(body, loopbackForwardToEnvKey+"=") {
			t.Fatalf("empty loopback forward override should not be rewritten: %q", body)
		}
	})

	t.Run("preserves explicit issuer host override", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kind.env")
		withUnixSocket(t)
		clearEnv(t, kindExperimentalNetKey)
		clearEnv(t, loopbackForwardToEnvKey)
		t.Setenv(loopbackIssuerHostEnvKey, "other.localhost")
		if err := ConfigureKindEnv(path, true, ":8085"); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		body := string(raw)
		if strings.Contains(body, loopbackIssuerHostEnvKey+"=") {
			t.Fatalf("explicit issuer host should not be rewritten: %q", body)
		}
	})

	t.Run("preserves empty issuer host override", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "kind.env")
		withUnixSocket(t)
		clearEnv(t, kindExperimentalNetKey)
		clearEnv(t, loopbackForwardToEnvKey)
		t.Setenv(loopbackIssuerHostEnvKey, "")
		if err := ConfigureKindEnv(path, true, ":8085"); err != nil {
			t.Fatal(err)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		body := string(raw)
		if strings.Contains(body, loopbackIssuerHostEnvKey+"=") {
			t.Fatalf("empty issuer host override should not be rewritten: %q", body)
		}
	})
}
