package aioinit_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/deploy/aio/internal/aioinit"
)

// TestServeArgs_PeerDexDefaultsForbidSandboxFlags checks Dex-on packaging
// argv (issuer, audience, UI client, registry, CA) and that serve is not
// given --sandbox or --bootstrap-auth-method.
func TestServeArgs_PeerDexDefaultsForbidSandboxFlags(t *testing.T) {
	in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{
		Endpoints: aioinit.FixedEndpoints,
		Issuer:    aioinit.PeerDexIssuer,
		CAFile:    "/data/sandbox/pki/ca.crt",
	})
	args := aioinit.ServeArgs(in)
	joined := strings.Join(args, "\x00")
	for _, want := range []string{
		"serve",
		"--http-addr\x00127.0.0.1:8086",
		"--oidc-issuer\x00https://fleetshift-sandbox.localhost:8085/idp",
		"--oidc-resource-audience\x00fleetshift",
		"--oidc-ui-client-id\x00fleetshift-ui",
		"--oidc-ui-scope\x00" + aioinit.DefaultPeerDexUIScope,
		"--oidc-registry-id\x00github.com",
		"--oidc-registry-subject-expression\x00claims.preferred_username",
		"--oidc-ca-file\x00/data/sandbox/pki/ca.crt",
	} {
		if !strings.Contains(joined, want) {
			t.Fatalf("args missing %q in %v", want, args)
		}
	}
	for _, bad := range []string{"--sandbox", "--bootstrap-auth-method"} {
		if strings.Contains(joined, bad) {
			t.Fatalf("forbidden flag %s present", bad)
		}
	}
}

func TestApplyServeDefaults_ExternalIssuerUsesPortableUIScope(t *testing.T) {
	in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{
		Endpoints: aioinit.FixedEndpoints,
		Issuer:    "https://keycloak.example/realms/fleetshift",
	})
	if in.UIScope != aioinit.DefaultExternalIssuerUIScope {
		t.Fatalf("UIScope = %q, want %q", in.UIScope, aioinit.DefaultExternalIssuerUIScope)
	}
	args := aioinit.ServeArgs(in)
	joined := strings.Join(args, "\x00")
	want := "--oidc-ui-scope\x00" + aioinit.DefaultExternalIssuerUIScope
	if !strings.Contains(joined, want) {
		t.Fatalf("args missing %q in %v", want, args)
	}
	if strings.Contains(joined, "audience:server:client_id:") {
		t.Fatalf("external issuer must not default Dex audience scope: %v", args)
	}
}

func TestApplyServeDefaults_ExplicitUIScopeWinsOnExternalIssuer(t *testing.T) {
	in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{
		Endpoints: aioinit.FixedEndpoints,
		Issuer:    "https://keycloak.example/realms/fleetshift",
		UIScope:   "openid profile email groups",
	})
	if in.UIScope != "openid profile email groups" {
		t.Fatalf("UIScope = %q, want explicit override", in.UIScope)
	}
}

func TestApplyServeDefaults_PublicKeySkipsRegistryDefaults(t *testing.T) {
	in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{
		Endpoints:     aioinit.FixedEndpoints,
		Issuer:        aioinit.PeerDexIssuer,
		PublicKeyExpr: "claims.spk",
	})
	if in.RegistryID != "" || in.RegistryExpr != "" {
		t.Fatalf("registry defaults = %q/%q, want empty when public-key claim set", in.RegistryID, in.RegistryExpr)
	}
	args := aioinit.ServeArgs(in)
	joined := strings.Join(args, "\x00")
	if !strings.Contains(joined, "--oidc-public-key-claim-expression\x00claims.spk") {
		t.Fatalf("args missing public-key claim: %v", args)
	}
	if strings.Contains(joined, "--oidc-registry-id") {
		t.Fatalf("args unexpectedly include registry: %v", args)
	}
}

func TestApplyServeDefaults_LogLevelOverride(t *testing.T) {
	in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{
		Endpoints: aioinit.FixedEndpoints,
		Issuer:    aioinit.PeerDexIssuer,
		LogLevel:  "info",
	})
	if in.LogLevel != "info" {
		t.Fatalf("LogLevel = %q, want info", in.LogLevel)
	}
	args := aioinit.ServeArgs(in)
	joined := strings.Join(args, "\x00")
	if !strings.Contains(joined, "--log-level\x00info") {
		t.Fatalf("args missing info log level: %v", args)
	}
}

func TestWriteServeExecScript(t *testing.T) {
	path := filepath.Join(t.TempDir(), "exec-serve")
	if err := aioinit.WriteServeExecScript(path, []string{
		"serve",
		"--oidc-ui-scope", "a b",
		"--oidc-issuer", "https://issuer.example/x&id",
		"--oidc-ui-client-id", "it's-me",
	}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	if !strings.Contains(body, "exec /usr/local/bin/fleetshift 'serve'") {
		t.Fatalf("unexpected script: %s", raw)
	}
	if !strings.Contains(body, " '--oidc-ui-scope' 'a b'") {
		t.Fatalf("scope not single-quoted in script: %s", raw)
	}
	if !strings.Contains(body, " '--oidc-issuer' 'https://issuer.example/x&id'") {
		t.Fatalf("issuer with & not single-quoted in script: %s", raw)
	}
	if !strings.Contains(body, ` '--oidc-ui-client-id' 'it'\''s-me'`) {
		t.Fatalf("embedded quote not escaped in script: %s", raw)
	}
}
