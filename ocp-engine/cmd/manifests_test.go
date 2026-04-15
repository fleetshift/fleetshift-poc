package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestIsAuthenticationCR(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		want bool
	}{
		{
			name: "authentication CR",
			yaml: "apiVersion: config.openshift.io/v1\nkind: Authentication\nmetadata:\n  name: cluster\n",
			want: true,
		},
		{
			name: "other CR",
			yaml: "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: test\n",
			want: false,
		},
		{
			name: "invalid yaml",
			yaml: "not: [valid: yaml",
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isAuthenticationCR([]byte(tt.yaml)); got != tt.want {
				t.Errorf("isAuthenticationCR() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMergeAuthenticationCR(t *testing.T) {
	manifestsDir := t.TempDir()

	// Write ccoctl's Authentication CR (has serviceAccountIssuer)
	ccoctlCR := `apiVersion: config.openshift.io/v1
kind: Authentication
metadata:
  name: cluster
spec:
  serviceAccountIssuer: https://test-cluster-oidc.s3.us-west-2.amazonaws.com
`
	if err := os.WriteFile(filepath.Join(manifestsDir, "cluster-authentication-02-config.yaml"), []byte(ccoctlCR), 0600); err != nil {
		t.Fatal(err)
	}

	// OIDC extra manifest
	oidcCR := `apiVersion: config.openshift.io/v1
kind: Authentication
metadata:
  name: cluster
spec:
  type: OIDC
  webhookTokenAuthenticator: null
  oidcProviders:
  - name: fleetshift-oidc
    issuer:
      issuerURL: https://keycloak.example.com/realms/fleetshift
      audiences:
      - fleetshift-cli
    claimMappings:
      username:
        claim: email
        prefixPolicy: Prefix
        prefix:
          prefixString: 'oidc:'
    oidcClients:
    - clientID: fleetshift-cli
      componentName: cli
      componentNamespace: openshift-console
`

	if err := mergeAuthenticationCR(manifestsDir, []byte(oidcCR)); err != nil {
		t.Fatalf("mergeAuthenticationCR: %v", err)
	}

	// Read the merged result
	merged, err := os.ReadFile(filepath.Join(manifestsDir, "cluster-authentication-02-config.yaml"))
	if err != nil {
		t.Fatalf("read merged: %v", err)
	}

	var result map[string]any
	if err := yaml.Unmarshal(merged, &result); err != nil {
		t.Fatalf("parse merged: %v", err)
	}

	spec, _ := result["spec"].(map[string]any)

	// Should have ccoctl's serviceAccountIssuer
	if sai, _ := spec["serviceAccountIssuer"].(string); sai != "https://test-cluster-oidc.s3.us-west-2.amazonaws.com" {
		t.Errorf("serviceAccountIssuer = %q, want ccoctl's value", sai)
	}

	// Should have OIDC type
	if typ, _ := spec["type"].(string); typ != "OIDC" {
		t.Errorf("type = %q, want OIDC", typ)
	}

	// Should have oidcProviders
	providers, _ := spec["oidcProviders"].([]any)
	if len(providers) == 0 {
		t.Fatal("oidcProviders is empty")
	}

	// Should have webhookTokenAuthenticator set to null
	if _, exists := spec["webhookTokenAuthenticator"]; !exists {
		t.Error("webhookTokenAuthenticator should be present (set to null)")
	}

	// Verify the merged YAML is valid and doesn't have duplicate keys
	content := string(merged)
	if strings.Count(content, "serviceAccountIssuer") != 1 {
		t.Error("serviceAccountIssuer appears more than once")
	}
	if strings.Count(content, "kind: Authentication") != 1 {
		t.Error("kind: Authentication appears more than once")
	}
}

func TestMergeAuthenticationCR_NoExisting(t *testing.T) {
	manifestsDir := t.TempDir()

	oidcCR := `apiVersion: config.openshift.io/v1
kind: Authentication
metadata:
  name: cluster
spec:
  type: OIDC
`

	if err := mergeAuthenticationCR(manifestsDir, []byte(oidcCR)); err != nil {
		t.Fatalf("mergeAuthenticationCR: %v", err)
	}

	// Should write the OIDC manifest directly
	data, err := os.ReadFile(filepath.Join(manifestsDir, "cluster-authentication-oidc.yaml"))
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(data), "type: OIDC") {
		t.Error("missing type: OIDC")
	}
}
