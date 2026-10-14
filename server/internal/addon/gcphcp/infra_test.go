package gcphcp_test

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
)

func TestGenerateClusterKeypair(t *testing.T) {
	t.Run("generates valid 4096-bit RSA keypair", func(t *testing.T) {
		kp, err := gcphcp.GenerateClusterKeypair()
		if err != nil {
			t.Fatalf("GenerateClusterKeypair() error = %v", err)
		}

		// Verify PEM base64 is non-empty and valid
		if kp.PrivateKeyPEMBase64 == "" {
			t.Error("PrivateKeyPEMBase64 is empty")
		}
		pemBytes, err := base64.StdEncoding.DecodeString(kp.PrivateKeyPEMBase64)
		if err != nil {
			t.Errorf("PrivateKeyPEMBase64 is not valid base64: %v", err)
		}
		if !strings.Contains(string(pemBytes), "BEGIN RSA PRIVATE KEY") {
			t.Error("decoded PEM does not contain RSA PRIVATE KEY header")
		}

		// Verify JWKS JSON is valid and has correct structure
		if len(kp.JWKSJSON) == 0 {
			t.Error("JWKSJSON is empty")
		}
		var jwks struct {
			Keys []map[string]any `json:"keys"`
		}
		if err := json.Unmarshal(kp.JWKSJSON, &jwks); err != nil {
			t.Fatalf("JWKSJSON is not valid JSON: %v", err)
		}
		if len(jwks.Keys) != 1 {
			t.Fatalf("expected 1 key in JWKS, got %d", len(jwks.Keys))
		}

		key := jwks.Keys[0]
		// Check required fields
		requiredFields := []string{"kty", "use", "alg", "kid", "n", "e"}
		for _, field := range requiredFields {
			if _, ok := key[field]; !ok {
				t.Errorf("JWKS key missing required field: %s", field)
			}
		}

		// Verify specific values
		if key["kty"] != "RSA" {
			t.Errorf("expected kty=RSA, got %v", key["kty"])
		}
		if key["use"] != "sig" {
			t.Errorf("expected use=sig, got %v", key["use"])
		}
		if key["alg"] != "RS256" {
			t.Errorf("expected alg=RS256, got %v", key["alg"])
		}
	})

	t.Run("generates unique keypairs", func(t *testing.T) {
		kp1, err := gcphcp.GenerateClusterKeypair()
		if err != nil {
			t.Fatalf("GenerateClusterKeypair() error = %v", err)
		}
		kp2, err := gcphcp.GenerateClusterKeypair()
		if err != nil {
			t.Fatalf("GenerateClusterKeypair() error = %v", err)
		}

		// Keys should be different
		if kp1.PrivateKeyPEMBase64 == kp2.PrivateKeyPEMBase64 {
			t.Error("generated identical PEM keys")
		}
		if string(kp1.JWKSJSON) == string(kp2.JWKSJSON) {
			t.Error("generated identical JWKS")
		}
	})
}

func TestIAMConfigToWIFSpec(t *testing.T) {
	t.Run("correctly maps service accounts", func(t *testing.T) {
		iamConfig := map[string]any{
			"workloadIdentityPool": map[string]any{
				"poolId":     "test-pool",
				"providerId": "test-provider",
			},
			"serviceAccounts": map[string]any{
				"ctrlplane-op":     "ctrlplane@project.iam.gserviceaccount.com",
				"nodepool-mgmt":    "nodepool@project.iam.gserviceaccount.com",
				"cloud-controller": "controller@project.iam.gserviceaccount.com",
				"gcp-pd-csi":       "storage@project.iam.gserviceaccount.com",
				"image-registry":   "registry@project.iam.gserviceaccount.com",
				"cloud-network":    "network@project.iam.gserviceaccount.com",
			},
			"projectNumber": "123456789012",
		}

		wif, err := gcphcp.IAMConfigToWIFSpec(iamConfig)
		if err != nil {
			t.Fatalf("IAMConfigToWIFSpec() error = %v", err)
		}

		// Check project number
		if wif["projectNumber"] != "123456789012" {
			t.Errorf("expected projectNumber=123456789012, got %v", wif["projectNumber"])
		}

		// Check pool and provider IDs
		if wif["poolID"] != "test-pool" {
			t.Errorf("expected poolID=test-pool, got %v", wif["poolID"])
		}
		if wif["providerID"] != "test-provider" {
			t.Errorf("expected providerID=test-provider, got %v", wif["providerID"])
		}

		// Check service accounts mapping
		saRef, ok := wif["serviceAccountsRef"].(map[string]string)
		if !ok {
			t.Fatalf("serviceAccountsRef is not map[string]string, got %T", wif["serviceAccountsRef"])
		}

		expectedMappings := map[string]string{
			"controlPlaneEmail":    "ctrlplane@project.iam.gserviceaccount.com",
			"nodePoolEmail":        "nodepool@project.iam.gserviceaccount.com",
			"cloudControllerEmail": "controller@project.iam.gserviceaccount.com",
			"storageEmail":         "storage@project.iam.gserviceaccount.com",
			"imageRegistryEmail":   "registry@project.iam.gserviceaccount.com",
			"networkEmail":         "network@project.iam.gserviceaccount.com",
		}

		for key, expected := range expectedMappings {
			if saRef[key] != expected {
				t.Errorf("expected %s=%s, got %s", key, expected, saRef[key])
			}
		}
	})

	t.Run("returns error for missing workloadIdentityPool", func(t *testing.T) {
		iamConfig := map[string]any{
			"serviceAccounts": map[string]any{},
		}

		_, err := gcphcp.IAMConfigToWIFSpec(iamConfig)
		if err == nil {
			t.Error("expected error for missing workloadIdentityPool, got nil")
		}
	})

	t.Run("returns error for missing poolId", func(t *testing.T) {
		iamConfig := map[string]any{
			"workloadIdentityPool": map[string]any{
				"providerId": "test-provider",
			},
		}

		_, err := gcphcp.IAMConfigToWIFSpec(iamConfig)
		if err == nil {
			t.Error("expected error for missing poolId, got nil")
		}
	})

	t.Run("returns error for missing serviceAccounts", func(t *testing.T) {
		iamConfig := map[string]any{
			"workloadIdentityPool": map[string]any{
				"poolId":     "test-pool",
				"providerId": "test-provider",
			},
		}

		_, err := gcphcp.IAMConfigToWIFSpec(iamConfig)
		if err == nil {
			t.Error("expected error for missing serviceAccounts, got nil")
		}
	})

	t.Run("returns error for missing service account", func(t *testing.T) {
		iamConfig := map[string]any{
			"workloadIdentityPool": map[string]any{
				"poolId":     "test-pool",
				"providerId": "test-provider",
			},
			"serviceAccounts": map[string]any{
				"ctrlplane-op": "ctrlplane@project.iam.gserviceaccount.com",
				// missing other required service accounts
			},
		}

		_, err := gcphcp.IAMConfigToWIFSpec(iamConfig)
		if err == nil {
			t.Error("expected error for missing service account, got nil")
		}
	})
}
