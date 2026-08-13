package aioinit_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/deploy/aio/internal/aioinit"
)

func TestApplyServeDefaultsAndArgs(t *testing.T) {
	in := aioinit.ApplyServeDefaults(aioinit.ServeConfig{
		Endpoints: aioinit.FixedEndpoints,
		Issuer:    aioinit.PeerDexIssuer,
		CAFile:    "/data/sandbox/pki/ca.crt",
	})
	args := aioinit.ServeArgs(in)
	joined := strings.Join(args, "\x00")
	for _, want := range []string{
		"serve",
		"--http-addr\x00:8085",
		"--oidc-issuer\x00https://127.0.0.1:5556/dex",
		"--oidc-resource-audience\x00fleetshift",
		"--oidc-ui-client-id\x00fleetshift-ui",
		"--oidc-ui-scope\x00openid profile email groups audience:server:client_id:fleetshift",
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
	if err := aioinit.WriteServeExecScript(path, []string{"serve", "--oidc-ui-scope", "a b"}); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	body := string(raw)
	if !strings.Contains(body, "exec /usr/local/bin/fleetshift serve") {
		t.Fatalf("unexpected script: %s", raw)
	}
	if !strings.Contains(body, " --oidc-ui-scope 'a b'\n") {
		t.Fatalf("scope not single-quoted in script: %s", raw)
	}
}
