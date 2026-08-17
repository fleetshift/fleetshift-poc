package aioinit_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/deploy/aio/internal/aioinit"
)

func TestInstallDexConfig(t *testing.T) {
	root := t.TempDir()
	paths := aioinit.DexPaths{
		ConfigDir:  filepath.Join(root, "dex"),
		ConfigPath: filepath.Join(root, "dex", "config.yaml"),
		DBPath:     filepath.Join(root, "dex", "dex.db"),
	}
	uid, gid := os.Getuid(), os.Getgid()
	err := aioinit.InstallDexConfig(aioinit.DexRenderInput{
		Issuer:    aioinit.PeerDexIssuer,
		Endpoints: aioinit.FixedEndpoints,
	}, paths, uid, gid)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(paths.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	for _, want := range []string{
		"issuer: https://fleetshift-sandbox.localhost:8085/dex",
		"http: 127.0.0.1:5556",
		"preferredUsername: \"dev-user\"",
		"id: fleetshift-ui",
		"id: fleetshift-cli",
		"id: fleetshift-signing",
		"https://fleetshift-sandbox.localhost:8085/auth/callback",
		"https://fleetshift-sandbox.localhost:8085/silent-renew.html",
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
