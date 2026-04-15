//go:build e2e

package e2e

import (
	"strings"
	"testing"
)

func TestGenerateClusterName(t *testing.T) {
	name := generateClusterName()
	if !strings.HasPrefix(name, "fleetshift-e2etest-") {
		t.Errorf("cluster name %q missing expected prefix", name)
	}
	name2 := generateClusterName()
	if name == name2 {
		t.Error("two generated names should differ")
	}
}

func TestEnvOr(t *testing.T) {
	t.Setenv("TEST_E2E_VAR", "custom")
	if got := envOr("TEST_E2E_VAR", "default"); got != "custom" {
		t.Errorf("envOr = %q, want custom", got)
	}
	if got := envOr("TEST_E2E_MISSING", "default"); got != "default" {
		t.Errorf("envOr = %q, want default", got)
	}
}

func TestLoadConfig_MissingRequired(t *testing.T) {
	for _, key := range []string{
		"E2E_KEYCLOAK_ISSUER", "E2E_KEYCLOAK_CLIENT_ID",
		"E2E_ROLE_ARN", "E2E_RH_SSO_ISSUER",
	} {
		t.Setenv(key, "")
	}

	_, err := LoadConfig()
	if err == nil {
		t.Fatal("expected error for missing required vars")
	}
	if !strings.Contains(err.Error(), "E2E_KEYCLOAK_ISSUER") {
		t.Errorf("error should mention missing var, got: %v", err)
	}
}
