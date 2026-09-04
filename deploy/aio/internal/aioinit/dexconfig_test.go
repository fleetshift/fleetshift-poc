package aioinit_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/deploy/aio/internal/aioinit"
)

func TestInstallDexConfig(t *testing.T) {
	t.Parallel()
	body := installTestDexConfig(t, "error")
	for _, want := range []string{
		"issuer: https://fleetshift-sandbox.localhost:8085/idp",
		"http: 127.0.0.1:5556",
		"preferredUsername: \"dev-user\"",
		"id: fleetshift-ui",
		"id: fleetshift-cli",
		"id: fleetshift-signing",
		"https://fleetshift-sandbox.localhost:8085/app/auth/callback",
		"https://fleetshift-sandbox.localhost:8085/app/silent-renew.html",
		"level: \"error\"",
		"format: \"text\"",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("config missing %q\n%s", want, body)
		}
	}
	for _, bad := range []string{
		"tlsCert:",
		"tlsKey:",
		"allowedOrigins:",
		"fleetshift-ops",
		"fleetshift-dev",
	} {
		if strings.Contains(body, bad) {
			t.Fatalf("config unexpectedly contains %q\n%s", bad, body)
		}
	}
	if strings.Contains(body, "web:\n  https:") || strings.Contains(body, "web:\r\n  https:") {
		t.Fatalf("peer Dex must listen on HTTP, not HTTPS:\n%s", body)
	}
	if !strings.Contains(body, "$2") {
		t.Fatal("expected bcrypt hashes in config")
	}
}

func TestInstallDexConfig_LogLevel(t *testing.T) {
	t.Parallel()
	for _, level := range []string{"debug", "warn"} {
		t.Run(level, func(t *testing.T) {
			t.Parallel()
			body := installTestDexConfig(t, level)
			want := `level: "` + level + `"`
			if !strings.Contains(body, want) {
				t.Fatalf("missing %q\n%s", want, body)
			}
			if strings.Contains(body, `level: "error"`) {
				t.Fatalf("%s LOG_LEVEL must not leave the error logger default:\n%s", level, body)
			}
		})
	}
}

func TestInstallDexConfig_ConfigDirIsFile(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	notDir := filepath.Join(root, "not-a-dir")
	if err := os.WriteFile(notDir, []byte("x"), 0644); err != nil {
		t.Fatal(err)
	}
	err := aioinit.InstallDexConfig(aioinit.DexRenderInput{
		Issuer:    aioinit.PeerDexIssuer,
		Endpoints: aioinit.FixedEndpoints,
		LogLevel:  "error",
	}, aioinit.DexPaths{
		ConfigDir:  notDir,
		ConfigPath: filepath.Join(notDir, "config.yaml"),
		DBPath:     filepath.Join(notDir, "dex.db"),
	}, os.Getuid(), os.Getgid())
	if err == nil {
		t.Fatal("InstallDexConfig: expected error when ConfigDir is a file")
	}
}

func installTestDexConfig(t *testing.T, logLevel string) string {
	t.Helper()
	root := t.TempDir()
	paths := aioinit.DexPaths{
		ConfigDir:  filepath.Join(root, "dex"),
		ConfigPath: filepath.Join(root, "dex", "config.yaml"),
		DBPath:     filepath.Join(root, "dex", "dex.db"),
	}
	err := aioinit.InstallDexConfig(aioinit.DexRenderInput{
		Issuer:    aioinit.PeerDexIssuer,
		Endpoints: aioinit.FixedEndpoints,
		LogLevel:  logLevel,
	}, paths, os.Getuid(), os.Getgid())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(paths.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}
