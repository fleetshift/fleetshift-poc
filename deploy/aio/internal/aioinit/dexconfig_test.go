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
		TLSCert:   "/data/sandbox/pki/server.crt",
		TLSKey:    "/data/sandbox/pki/server.key",
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
		"issuer: https://127.0.0.1:5556/dex",
		"preferredUsername: \"dev-user\"",
		"id: fleetshift-ui",
		"id: fleetshift-cli",
		"id: fleetshift-signing",
		"http://127.0.0.1:8085/auth/callback",
		"allowedOrigins:",
		"- http://127.0.0.1:8085",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("config missing %q\n%s", want, body)
		}
	}
	if strings.Contains(body, "fleetshift-ops") || strings.Contains(body, "fleetshift-dev") {
		t.Fatal("plaintext passwords must not appear in Dex config")
	}
	if !strings.Contains(body, "$2") {
		t.Fatal("expected bcrypt hashes in config")
	}
}
