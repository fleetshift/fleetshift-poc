package auth_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestSaveLoadConfig_ConfigDir(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	configDir := t.TempDir()

	cfg := auth.Config{
		IssuerURL:             "https://issuer.example/idp",
		ClientID:              "fleetshift-cli",
		Scopes:                []string{"openid"},
		AuthorizationEndpoint: "https://issuer.example/idp/auth",
		TokenEndpoint:         "https://issuer.example/idp/token",
	}
	if err := auth.SaveConfigTo(configDir, cfg); err != nil {
		t.Fatalf("SaveConfigTo: %v", err)
	}

	if _, err := os.Stat(filepath.Join(home, ".config")); !os.IsNotExist(err) {
		t.Fatalf("default config dir should be absent, stat err=%v", err)
	}

	loaded, err := auth.LoadConfigFrom(configDir)
	if err != nil {
		t.Fatalf("LoadConfigFrom: %v", err)
	}
	if loaded.IssuerURL != cfg.IssuerURL || loaded.ClientID != cfg.ClientID {
		t.Fatalf("LoadConfigFrom() = %#v, want %#v", loaded, cfg)
	}

	info, err := os.Stat(filepath.Join(configDir, "auth.json"))
	if err != nil {
		t.Fatalf("stat auth.json: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("auth.json mode = %o, want 0600", info.Mode().Perm())
	}
}
