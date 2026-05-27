package gcphcp_test

import (
	"encoding/json"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestClusterOutput_Target(t *testing.T) {
	output := gcphcp.ClusterOutput{
		TargetID:  "target-123",
		Name:      "test-cluster",
		APIServer: "https://1.2.3.4:6443",
		CACert:    []byte("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"),
		SATokenRef: "vault/secret/test-token",
		SAToken:    []byte("sa-token-value"),
		TrustBundles: []domain.TrustBundleEntry{
			{
				IssuerURL:                  "https://issuer.example.com",
				JWKSURI:                    "https://issuer.example.com/.well-known/jwks.json",
				EnrollmentAudience:         "fleetshift",
				PublicKeyClaimExpression:   ".pub",
				RegistrySubjectMapping:     &domain.RegistrySubjectMapping{
					RegistryID: "github.com",
					Expression: "claims.sub",
				},
			},
		},
	}

	target := output.Target()

	if target.ID != "target-123" {
		t.Errorf("expected ID=target-123, got %s", target.ID)
	}
	if target.Type != gcphcp.KubernetesTargetType {
		t.Errorf("expected Type=%s, got %s", gcphcp.KubernetesTargetType, target.Type)
	}
	if target.Name != "test-cluster" {
		t.Errorf("expected Name=test-cluster, got %s", target.Name)
	}

	if target.Properties["api_server"] != "https://1.2.3.4:6443" {
		t.Errorf("expected api_server=https://1.2.3.4:6443, got %s", target.Properties["api_server"])
	}
	if target.Properties["ca_cert"] != "-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----" {
		t.Errorf("unexpected ca_cert: %s", target.Properties["ca_cert"])
	}
	if target.Properties["service_account_token_ref"] != "vault/secret/test-token" {
		t.Errorf("expected service_account_token_ref=vault/secret/test-token, got %s", target.Properties["service_account_token_ref"])
	}

	// Verify trust_bundle is valid JSON
	trustBundleJSON := target.Properties["trust_bundle"]
	if trustBundleJSON == "" {
		t.Fatal("expected trust_bundle property to be set")
	}
	var bundles []domain.TrustBundleEntry
	if err := json.Unmarshal([]byte(trustBundleJSON), &bundles); err != nil {
		t.Fatalf("trust_bundle is not valid JSON: %v", err)
	}
	if len(bundles) != 1 {
		t.Fatalf("expected 1 trust bundle, got %d", len(bundles))
	}
	if bundles[0].IssuerURL != "https://issuer.example.com" {
		t.Errorf("expected IssuerURL=https://issuer.example.com, got %s", bundles[0].IssuerURL)
	}

	// Verify AcceptedResourceTypes
	if len(target.AcceptedResourceTypes) != 1 {
		t.Fatalf("expected 1 accepted resource type, got %d", len(target.AcceptedResourceTypes))
	}
	if target.AcceptedResourceTypes[0] != kubernetes.ManifestResourceType {
		t.Errorf("expected AcceptedResourceTypes[0]=%s, got %s", kubernetes.ManifestResourceType, target.AcceptedResourceTypes[0])
	}
}

func TestClusterOutput_Secrets(t *testing.T) {
	output := gcphcp.ClusterOutput{
		TargetID:   "target-123",
		Name:       "test-cluster",
		APIServer:  "https://1.2.3.4:6443",
		SATokenRef: "vault/secret/test-token",
		SAToken:    []byte("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..."),
	}

	secrets := output.Secrets()

	if len(secrets) != 1 {
		t.Fatalf("expected 1 secret, got %d", len(secrets))
	}

	if secrets[0].Ref != "vault/secret/test-token" {
		t.Errorf("expected Ref=vault/secret/test-token, got %s", secrets[0].Ref)
	}
	if string(secrets[0].Value) != "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9..." {
		t.Errorf("unexpected Value: %s", string(secrets[0].Value))
	}
}

func TestClusterOutput_NoSA(t *testing.T) {
	output := gcphcp.ClusterOutput{
		TargetID:  "target-123",
		Name:      "test-cluster",
		APIServer: "https://1.2.3.4:6443",
		CACert:    []byte("-----BEGIN CERTIFICATE-----\ntest\n-----END CERTIFICATE-----"),
		// No SATokenRef or SAToken
	}

	secrets := output.Secrets()

	if secrets != nil {
		t.Errorf("expected nil secrets when no SA token ref, got %v", secrets)
	}

	// Verify target still works
	target := output.Target()
	if target.Properties["service_account_token_ref"] != "" {
		t.Errorf("expected empty service_account_token_ref, got %s", target.Properties["service_account_token_ref"])
	}
}
