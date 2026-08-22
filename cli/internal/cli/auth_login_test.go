package cli_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestAuthSetup_ConfigDirWritesConfigThereNotHome(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	stateDir := t.TempDir()
	issuer := startOIDCDiscovery(t)

	out := runCLI(t,
		"--config-dir", stateDir,
		"--insecure-storage",
		"--server", "127.0.0.1:1",
		"auth", "setup",
		"--issuer-url", issuer,
		"--client-id", "fleetshift-cli",
	)
	if !strings.Contains(out, "Local authentication configured") {
		t.Fatalf("stdout = %q", out)
	}

	if _, err := os.Stat(filepath.Join(home, ".config")); !os.IsNotExist(err) {
		t.Fatalf("home config should be absent, err=%v", err)
	}

	cfg, err := auth.LoadConfigFrom(stateDir)
	if err != nil {
		t.Fatalf("LoadConfigFrom: %v", err)
	}
	if cfg.IssuerURL != issuer || cfg.ClientID != "fleetshift-cli" {
		t.Fatalf("config = %#v", cfg)
	}
}

func TestAuthSetup_RelativeOIDCCAFilePersistsAbsolutePath(t *testing.T) {
	stateDir := t.TempDir()
	caDir := t.TempDir()
	caSrc := filepath.Join(caDir, "ca.crt")
	if err := os.WriteFile(caSrc, []byte("sandbox-ca\n"), 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	t.Chdir(caDir)
	issuer := startOIDCDiscovery(t)

	_ = runCLI(t,
		"--config-dir", stateDir,
		"auth", "setup",
		"--issuer-url", issuer,
		"--client-id", "fleetshift-cli",
		"--oidc-ca-file", "ca.crt",
	)

	cfg, err := auth.LoadConfigFrom(stateDir)
	if err != nil {
		t.Fatalf("LoadConfigFrom: %v", err)
	}
	want, err := filepath.Abs("ca.crt")
	if err != nil {
		t.Fatalf("Abs: %v", err)
	}
	if cfg.OIDCCAFile != want {
		t.Fatalf("OIDCCAFile = %q, want absolute path %q", cfg.OIDCCAFile, want)
	}
}

func TestAuthSetup_ConfigDirRecordsOriginalCAPath(t *testing.T) {
	stateDir := t.TempDir()
	caSrc := filepath.Join(t.TempDir(), "ca.crt")
	if err := os.WriteFile(caSrc, []byte("sandbox-ca\n"), 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	issuer := startOIDCDiscovery(t)

	_ = runCLI(t,
		"--config-dir", stateDir,
		"auth", "setup",
		"--issuer-url", issuer,
		"--client-id", "fleetshift-cli",
		"--oidc-ca-file", caSrc,
	)

	cfg, err := auth.LoadConfigFrom(stateDir)
	if err != nil {
		t.Fatalf("LoadConfigFrom: %v", err)
	}
	if cfg.OIDCCAFile != caSrc {
		t.Fatalf("OIDCCAFile = %q, want original path %q", cfg.OIDCCAFile, caSrc)
	}
	copied := filepath.Join(stateDir, "oidc-ca.crt")
	if _, err := os.Stat(copied); !os.IsNotExist(err) {
		t.Fatalf("oidc-ca.crt should not exist under config dir, err=%v", err)
	}
}

func TestAuthCLI_InsecureStorageRequiresConfigDir(t *testing.T) {
	_, err := runCLIErr(t, "--insecure-storage", "auth", "setup",
		"--issuer-url", "https://issuer.example/idp",
		"--client-id", "fleetshift-cli",
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "--insecure-storage requires --config-dir") {
		t.Fatalf("error = %v", err)
	}
}

func TestAuthCLI_CredentialStoreFlagRemoved(t *testing.T) {
	_, err := runCLIErr(t, "--credential-store", "file", "auth", "login")
	if err == nil {
		t.Fatal("expected unknown flag")
	}
	if !strings.Contains(err.Error(), "unknown flag: --credential-store") {
		t.Fatalf("error = %v, want unknown --credential-store", err)
	}
}

func TestAuthCLI_ConfigDirMustBeAbsolute(t *testing.T) {
	_, err := runCLIErr(t, "--config-dir", "relative", "auth", "setup",
		"--issuer-url", "https://issuer.example/idp",
		"--client-id", "fleetshift-cli",
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "--config-dir must be an absolute path") {
		t.Fatalf("error = %v", err)
	}
}

func TestAuthLogin_NoBrowserWithoutSetupFails(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	out, err := runCLIErr(t, "auth", "login", "--no-browser")
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "load auth config") {
		t.Fatalf("error = %v", err)
	}
	if strings.Contains(out, "AUTH_URL ") {
		t.Fatalf("stdout = %q, must not print AUTH_URL without setup", out)
	}
}

func TestAuthLogout_ClearsFileStoreTokens(t *testing.T) {
	stateDir := t.TempDir()
	store := auth.FileStore{Dir: stateDir}
	ctx := context.Background()
	if err := store.Save(ctx, auth.Tokens{
		AccessToken: "tok",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	const pem = "-----BEGIN EC PRIVATE KEY-----\ntest\n-----END EC PRIVATE KEY-----\n"
	if err := store.SaveSigningKey(pem); err != nil {
		t.Fatalf("SaveSigningKey: %v", err)
	}

	out := runCLI(t, "--config-dir", stateDir, "--insecure-storage", "auth", "logout")
	if !strings.Contains(out, "Logged out.") {
		t.Fatalf("stdout = %q, want Logged out.", out)
	}
	if _, err := store.Load(ctx); err == nil {
		t.Fatal("Load after logout: expected error")
	}
	got, err := store.LoadSigningKey()
	if err != nil {
		t.Fatalf("LoadSigningKey after logout: %v", err)
	}
	if got != pem {
		t.Fatalf("signing key after logout = %q, want preserved PEM", got)
	}
}
