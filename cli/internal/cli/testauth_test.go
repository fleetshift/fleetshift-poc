package cli_test

import (
	"context"
	"testing"
	"time"

	"github.com/zalando/go-keyring"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

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

	store := auth.KeyringTokenStore{}
	if err := store.Save(context.Background(), auth.Tokens{
		AccessToken: "test-access-token",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("Save tokens: %v", err)
	}
}
