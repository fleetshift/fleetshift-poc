package cli_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestAuthSetup_WritesLocalConfigFromDiscovery(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	issuer := startOIDCDiscovery(t)

	// Point --server at an unusable address; default local mode must not dial gRPC.
	out := runCLI(t,
		"--server", "127.0.0.1:1",
		"auth", "setup",
		"--issuer-url", issuer,
		"--client-id", "fleetshift-cli",
		"--scopes", "openid, profile, email",
		"--key-enrollment-client-id", "fleetshift-signing",
	)
	if out == "" {
		t.Fatal("expected success message on stdout")
	}
	assertAuthJSON(t, home, issuer)

	cfg, err := auth.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.KeyEnrollmentClientID != "fleetshift-signing" {
		t.Errorf("KeyEnrollmentClientID = %q, want fleetshift-signing", cfg.KeyEnrollmentClientID)
	}
	wantScopes := []string{"openid", "profile", "email"}
	if len(cfg.Scopes) != len(wantScopes) {
		t.Fatalf("Scopes = %#v, want %#v", cfg.Scopes, wantScopes)
	}
	for i := range wantScopes {
		if cfg.Scopes[i] != wantScopes[i] {
			t.Fatalf("Scopes = %#v, want %#v", cfg.Scopes, wantScopes)
		}
	}
}

func TestAuthSetup_LocalConfigFlag(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	issuer := startOIDCDiscovery(t)

	out := runCLI(t,
		"--server", "127.0.0.1:1",
		"auth", "setup",
		"--local-config",
		"--issuer-url", issuer,
		"--client-id", "fleetshift-cli",
	)
	if !strings.Contains(out, "Local authentication configured") {
		t.Fatalf("stdout = %q, want local success message", out)
	}
	assertAuthJSON(t, home, issuer)
}

func TestAuthSetup_EmptyScopes(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)

	issuer := startOIDCDiscovery(t)
	_, err := runCLIErr(t,
		"--server", "127.0.0.1:1",
		"auth", "setup",
		"--issuer-url", issuer,
		"--client-id", "fleetshift-cli",
		"--scopes", ", ,",
	)
	if err == nil {
		t.Fatal("expected empty scopes error")
	}
	if !strings.Contains(err.Error(), "--scopes must include at least one scope") {
		t.Fatalf("error = %v, want scopes validation failure", err)
	}
}

func TestAuthSetup_LocalConfigAndConfigureServerConflict(t *testing.T) {
	_, err := runCLIErr(t,
		"auth", "setup",
		"--local-config",
		"--configure-server",
		"--issuer-url", "https://issuer.example/dex",
		"--client-id", "fleetshift-cli",
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Fatalf("error = %v, want mutually exclusive", err)
	}
}

func TestAuthSetup_ServerOnlyFlagRequiresConfigureServer(t *testing.T) {
	_, err := runCLIErr(t,
		"auth", "setup",
		"--issuer-url", "https://issuer.example/dex",
		"--client-id", "fleetshift-cli",
		"--audience", "fleetshift",
	)
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "--audience requires --configure-server") {
		t.Fatalf("error = %v, want --audience requires --configure-server", err)
	}
}

func TestAuthSetup_ConfigureServerDials(t *testing.T) {
	_, err := runCLIErr(t,
		"--server", "127.0.0.1:1",
		"auth", "setup",
		"--configure-server",
		"--issuer-url", "https://issuer.example/dex",
		"--client-id", "fleetshift-cli",
		"--audience", "fleetshift",
	)
	if err == nil {
		t.Fatal("expected dial error")
	}
	// Unreachable server proves configure-server does not take the local path.
	if !strings.Contains(err.Error(), "connection refused") &&
		!strings.Contains(err.Error(), "connect") &&
		!strings.Contains(err.Error(), "dial") {
		t.Fatalf("error = %v, want dial/connect failure", err)
	}
}

func startOIDCDiscovery(t *testing.T) string {
	t.Helper()
	var issuer string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/dex/.well-known/openid-configuration" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{
			"issuer":                 issuer,
			"authorization_endpoint": issuer + "/auth",
			"token_endpoint":         issuer + "/token",
		})
	}))
	t.Cleanup(srv.Close)
	issuer = srv.URL + "/dex"
	return issuer
}

func assertAuthJSON(t *testing.T, home, issuer string) {
	t.Helper()
	cfgPath := filepath.Join(home, ".config", "fleetshift", "auth.json")
	data, err := os.ReadFile(cfgPath)
	if err != nil {
		t.Fatalf("read %s: %v", cfgPath, err)
	}

	var cfg auth.Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("parse auth.json: %v\n%s", err, data)
	}
	if cfg.IssuerURL != issuer {
		t.Errorf("IssuerURL = %q, want %q", cfg.IssuerURL, issuer)
	}
	if cfg.ClientID != "fleetshift-cli" {
		t.Errorf("ClientID = %q, want fleetshift-cli", cfg.ClientID)
	}
	if cfg.AuthorizationEndpoint != issuer+"/auth" {
		t.Errorf("AuthorizationEndpoint = %q, want %q", cfg.AuthorizationEndpoint, issuer+"/auth")
	}
	if cfg.TokenEndpoint != issuer+"/token" {
		t.Errorf("TokenEndpoint = %q, want %q", cfg.TokenEndpoint, issuer+"/token")
	}

	loaded, err := auth.LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if loaded.ClientID != cfg.ClientID || loaded.AuthorizationEndpoint != cfg.AuthorizationEndpoint {
		t.Fatalf("LoadConfig() = %#v, want match of written config %#v", loaded, cfg)
	}
}
