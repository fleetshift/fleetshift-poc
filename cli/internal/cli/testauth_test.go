package cli_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/zalando/go-keyring"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

// TestMain points HOME at a temp dir so tests in this directory never read
// or write the developer's ~/.config/fleetshift/auth.json.
func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "fleetctl-test-home-")
	if err != nil {
		fmt.Fprintf(os.Stderr, "isolate HOME: %v\n", err)
		os.Exit(1)
	}
	if err := os.Setenv("HOME", dir); err != nil {
		fmt.Fprintf(os.Stderr, "set HOME: %v\n", err)
		os.RemoveAll(dir)
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// seedTestAuth writes local Fleetctl OIDC config and a dummy access token so
// CLI commands can authenticate to testserver (stub verifier accepts any bearer).
func seedTestAuth(t *testing.T) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	keyring.MockInit()

	if err := auth.SaveConfig(auth.Config{
		IssuerURL:             "https://test-issuer.example",
		ClientID:              "fleetshift-cli",
		Scopes:                []string{"openid"},
		AuthorizationEndpoint: "https://test-issuer.example/auth",
		TokenEndpoint:         "https://test-issuer.example/token",
	}); err != nil {
		t.Fatalf("SaveConfig: %v", err)
	}

	store := auth.KeyringStore{}
	if err := store.Save(context.Background(), auth.Tokens{
		AccessToken: "test-access-token",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("Save tokens: %v", err)
	}
}
