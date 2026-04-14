package ocp

import (
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestGenerateOIDCManifests(t *testing.T) {
	cfg := OIDCProviderConfig{
		IssuerURL: "https://keycloak.example.com/realms/openshift",
		Audiences: []string{"openshift", "kubernetes"},
		CABundle: []byte(`-----BEGIN CERTIFICATE-----
MIICxjCCAa4CCQDZlxQZFjHKGjANBgkqhkiG9w0BAQsFADAlMQswCQYDVQQGEwJV
UzEWMBQGA1UEAwwNZXhhbXBsZS5sb2NhbDAeFw0yNDA0MTMwMDAwMDBaFw0yNTA0
MTMwMDAwMDBaMCUxCzAJBgNVBAYTAlVTMRYwFAYDVQQDDA1leGFtcGxlLmxvY2Fs
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA0Z8Q==
-----END CERTIFICATE-----`),
		ClientSecret: "super-secret-value",
		CLIClientID:  "oc-cli",
	}

	manifests, err := GenerateOIDCManifests(cfg)
	if err != nil {
		t.Fatalf("GenerateOIDCManifests failed: %v", err)
	}

	// Should produce 3 manifests
	if len(manifests) != 3 {
		t.Fatalf("expected 3 manifests, got %d", len(manifests))
	}

	t.Run("AuthenticationCR", func(t *testing.T) {
		authYAML, ok := manifests["cluster-authentication-oidc.yaml"]
		if !ok {
			t.Fatal("missing cluster-authentication-oidc.yaml")
		}

		var auth map[string]any
		if err := yaml.Unmarshal(authYAML, &auth); err != nil {
			t.Fatalf("invalid YAML in auth manifest: %v", err)
		}

		// Check apiVersion and kind
		if auth["apiVersion"] != "config.openshift.io/v1" {
			t.Errorf("unexpected apiVersion: %v", auth["apiVersion"])
		}
		if auth["kind"] != "Authentication" {
			t.Errorf("unexpected kind: %v", auth["kind"])
		}

		// Check metadata
		metadata, ok := auth["metadata"].(map[string]any)
		if !ok {
			t.Fatal("missing or invalid metadata")
		}
		if metadata["name"] != "cluster" {
			t.Errorf("unexpected metadata.name: %v", metadata["name"])
		}

		// Check spec.type
		spec, ok := auth["spec"].(map[string]any)
		if !ok {
			t.Fatal("missing or invalid spec")
		}
		if spec["type"] != "OIDC" {
			t.Errorf("expected spec.type=OIDC, got %v", spec["type"])
		}

		// Check oidcProviders
		oidcProviders, ok := spec["oidcProviders"].([]any)
		if !ok || len(oidcProviders) != 1 {
			t.Fatal("missing or invalid spec.oidcProviders")
		}

		provider := oidcProviders[0].(map[string]any)
		if provider["name"] != "fleetshift-oidc" {
			t.Errorf("unexpected provider name: %v", provider["name"])
		}

		// Check issuer
		issuer, ok := provider["issuer"].(map[string]any)
		if !ok {
			t.Fatal("missing or invalid provider.issuer")
		}
		if issuer["issuerURL"] != cfg.IssuerURL {
			t.Errorf("expected issuerURL=%s, got %v", cfg.IssuerURL, issuer["issuerURL"])
		}

		// Check audiences
		audiences, ok := issuer["audiences"].([]any)
		if !ok || len(audiences) != 2 {
			t.Fatalf("expected 2 audiences, got %v", audiences)
		}
		if audiences[0] != "openshift" || audiences[1] != "kubernetes" {
			t.Errorf("unexpected audiences: %v", audiences)
		}

		// Check issuerCertificateAuthority
		issuerCA, ok := issuer["issuerCertificateAuthority"].(map[string]any)
		if !ok {
			t.Fatal("missing or invalid issuerCertificateAuthority")
		}
		if issuerCA["name"] != "fleetshift-oidc-ca" {
			t.Errorf("unexpected CA name: %v", issuerCA["name"])
		}

		// Check claimMappings
		claimMappings, ok := provider["claimMappings"].(map[string]any)
		if !ok {
			t.Fatal("missing or invalid claimMappings")
		}

		username, ok := claimMappings["username"].(map[string]any)
		if !ok {
			t.Fatal("missing or invalid claimMappings.username")
		}
		if username["claim"] != "email" {
			t.Errorf("unexpected username claim: %v", username["claim"])
		}

		groups, ok := claimMappings["groups"].(map[string]any)
		if !ok {
			t.Fatal("missing or invalid claimMappings.groups")
		}
		if groups["claim"] != "groups" {
			t.Errorf("unexpected groups claim: %v", groups["claim"])
		}
		if groups["prefix"] != "oidc:" {
			t.Errorf("unexpected groups prefix: %v", groups["prefix"])
		}

		// Check oidcClients
		oidcClients, ok := provider["oidcClients"].([]any)
		if !ok || len(oidcClients) != 1 {
			t.Fatal("missing or invalid oidcClients")
		}
		client := oidcClients[0].(map[string]any)
		if client["clientID"] != "oc-cli" {
			t.Errorf("unexpected clientID: %v", client["clientID"])
		}
		if client["componentName"] != "cli" {
			t.Errorf("unexpected componentName: %v", client["componentName"])
		}
	})

	t.Run("CAConfigMap", func(t *testing.T) {
		caYAML, ok := manifests["cluster-oidc-ca-configmap.yaml"]
		if !ok {
			t.Fatal("missing cluster-oidc-ca-configmap.yaml")
		}

		var caConfigMap map[string]any
		if err := yaml.Unmarshal(caYAML, &caConfigMap); err != nil {
			t.Fatalf("invalid YAML in CA configmap: %v", err)
		}

		if caConfigMap["apiVersion"] != "v1" {
			t.Errorf("unexpected apiVersion: %v", caConfigMap["apiVersion"])
		}
		if caConfigMap["kind"] != "ConfigMap" {
			t.Errorf("unexpected kind: %v", caConfigMap["kind"])
		}

		caMetadata, ok := caConfigMap["metadata"].(map[string]any)
		if !ok {
			t.Fatal("missing or invalid metadata")
		}
		if caMetadata["name"] != "fleetshift-oidc-ca" {
			t.Errorf("unexpected CA configmap name: %v", caMetadata["name"])
		}
		if caMetadata["namespace"] != "openshift-config" {
			t.Errorf("unexpected CA configmap namespace: %v", caMetadata["namespace"])
		}

		data, ok := caConfigMap["data"].(map[string]any)
		if !ok {
			t.Fatal("missing or invalid data")
		}
		caBundleStr, ok := data["ca-bundle.crt"].(string)
		if !ok {
			t.Fatal("missing or invalid ca-bundle.crt")
		}
		if !strings.Contains(caBundleStr, "BEGIN CERTIFICATE") {
			t.Error("ca-bundle.crt does not contain PEM certificate")
		}
	})

	t.Run("ClientSecret", func(t *testing.T) {
		secretYAML, ok := manifests["cluster-oidc-client-secret.yaml"]
		if !ok {
			t.Fatal("missing cluster-oidc-client-secret.yaml")
		}

		var secret map[string]any
		if err := yaml.Unmarshal(secretYAML, &secret); err != nil {
			t.Fatalf("invalid YAML in secret: %v", err)
		}

		if secret["apiVersion"] != "v1" {
			t.Errorf("unexpected apiVersion: %v", secret["apiVersion"])
		}
		if secret["kind"] != "Secret" {
			t.Errorf("unexpected kind: %v", secret["kind"])
		}

		secretMetadata, ok := secret["metadata"].(map[string]any)
		if !ok {
			t.Fatal("missing or invalid metadata")
		}
		if secretMetadata["name"] != "fleetshift-oidc-secret" {
			t.Errorf("unexpected secret name: %v", secretMetadata["name"])
		}
		if secretMetadata["namespace"] != "openshift-config" {
			t.Errorf("unexpected secret namespace: %v", secretMetadata["namespace"])
		}

		stringData, ok := secret["stringData"].(map[string]any)
		if !ok {
			t.Fatal("missing or invalid stringData")
		}
		if stringData["clientSecret"] != "super-secret-value" {
			t.Errorf("unexpected clientSecret: %v", stringData["clientSecret"])
		}
	})
}

func TestGenerateOIDCManifests_NoCABundle(t *testing.T) {
	cfg := OIDCProviderConfig{
		IssuerURL: "https://keycloak.example.com/realms/openshift",
		Audiences: []string{"openshift"},
		CABundle:  nil, // No CA bundle
	}

	manifests, err := GenerateOIDCManifests(cfg)
	if err != nil {
		t.Fatalf("GenerateOIDCManifests failed: %v", err)
	}

	// Should produce only 1 manifest (auth CR)
	if len(manifests) != 1 {
		t.Fatalf("expected 1 manifest, got %d", len(manifests))
	}

	// Should have auth manifest
	authYAML, ok := manifests["cluster-authentication-oidc.yaml"]
	if !ok {
		t.Fatal("missing cluster-authentication-oidc.yaml")
	}

	var auth map[string]any
	if err := yaml.Unmarshal(authYAML, &auth); err != nil {
		t.Fatalf("invalid YAML in auth manifest: %v", err)
	}

	spec, ok := auth["spec"].(map[string]any)
	if !ok {
		t.Fatal("missing or invalid spec")
	}

	oidcProviders, ok := spec["oidcProviders"].([]any)
	if !ok || len(oidcProviders) != 1 {
		t.Fatal("missing or invalid spec.oidcProviders")
	}

	provider := oidcProviders[0].(map[string]any)
	issuer, ok := provider["issuer"].(map[string]any)
	if !ok {
		t.Fatal("missing or invalid provider.issuer")
	}

	// issuerCertificateAuthority should NOT be present
	if _, exists := issuer["issuerCertificateAuthority"]; exists {
		t.Error("issuerCertificateAuthority should not be present when CABundle is nil")
	}

	// Should NOT have CA configmap
	if _, ok := manifests["cluster-oidc-ca-configmap.yaml"]; ok {
		t.Error("cluster-oidc-ca-configmap.yaml should not be generated when CABundle is nil")
	}
}

func TestGenerateOIDCManifests_WithCLIClient(t *testing.T) {
	cfg := OIDCProviderConfig{
		IssuerURL:   "https://keycloak.example.com/realms/openshift",
		Audiences:   []string{"openshift"},
		CLIClientID: "oc-cli-client",
	}

	manifests, err := GenerateOIDCManifests(cfg)
	if err != nil {
		t.Fatalf("GenerateOIDCManifests failed: %v", err)
	}

	authYAML := manifests["cluster-authentication-oidc.yaml"]
	var auth map[string]any
	if err := yaml.Unmarshal(authYAML, &auth); err != nil {
		t.Fatalf("invalid YAML in auth manifest: %v", err)
	}

	spec := auth["spec"].(map[string]any)
	oidcProviders := spec["oidcProviders"].([]any)
	provider := oidcProviders[0].(map[string]any)

	// Check oidcClients is present
	oidcClients, ok := provider["oidcClients"].([]any)
	if !ok || len(oidcClients) != 1 {
		t.Fatal("oidcClients should be present when CLIClientID is set")
	}

	client := oidcClients[0].(map[string]any)
	if client["clientID"] != "oc-cli-client" {
		t.Errorf("unexpected clientID: %v", client["clientID"])
	}
	if client["componentName"] != "cli" {
		t.Errorf("unexpected componentName: %v", client["componentName"])
	}
	if client["componentNamespace"] != "openshift-console" {
		t.Errorf("unexpected componentNamespace: %v", client["componentNamespace"])
	}
}

func TestGenerateOIDCManifests_NoCLIClient(t *testing.T) {
	cfg := OIDCProviderConfig{
		IssuerURL:   "https://keycloak.example.com/realms/openshift",
		Audiences:   []string{"openshift"},
		CLIClientID: "", // No CLI client
	}

	manifests, err := GenerateOIDCManifests(cfg)
	if err != nil {
		t.Fatalf("GenerateOIDCManifests failed: %v", err)
	}

	authYAML := manifests["cluster-authentication-oidc.yaml"]
	var auth map[string]any
	if err := yaml.Unmarshal(authYAML, &auth); err != nil {
		t.Fatalf("invalid YAML in auth manifest: %v", err)
	}

	spec := auth["spec"].(map[string]any)
	oidcProviders := spec["oidcProviders"].([]any)
	provider := oidcProviders[0].(map[string]any)

	// oidcClients should NOT be present
	if _, exists := provider["oidcClients"]; exists {
		t.Error("oidcClients should not be present when CLIClientID is empty")
	}
}

func TestGenerateOIDCManifests_ClientSecretSpecialChars(t *testing.T) {
	tests := []struct {
		name   string
		secret string
	}{
		{"double quotes", `secret-with-"quotes"`},
		{"backslash", `secret\with\backslashes`},
		{"colon", `secret: with colon`},
		{"newline", "secret\nwith\nnewlines"},
		{"yaml special", `{not: yaml}`},
		{"hash", `secret # not a comment`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := OIDCProviderConfig{
				IssuerURL:    "https://keycloak.example.com/realms/openshift",
				Audiences:    []string{"openshift"},
				ClientSecret: tt.secret,
			}

			manifests, err := GenerateOIDCManifests(cfg)
			if err != nil {
				t.Fatalf("GenerateOIDCManifests failed: %v", err)
			}

			secretYAML, ok := manifests["cluster-oidc-client-secret.yaml"]
			if !ok {
				t.Fatal("missing cluster-oidc-client-secret.yaml")
			}

			// The generated YAML must parse cleanly
			var secret map[string]any
			if err := yaml.Unmarshal(secretYAML, &secret); err != nil {
				t.Fatalf("generated YAML is invalid for secret %q: %v", tt.secret, err)
			}

			// The round-tripped value must match the original
			stringData := secret["stringData"].(map[string]any)
			if stringData["clientSecret"] != tt.secret {
				t.Errorf("round-trip mismatch: got %q, want %q", stringData["clientSecret"], tt.secret)
			}
		})
	}
}

func TestGenerateOIDCManifests_NoClientSecret(t *testing.T) {
	cfg := OIDCProviderConfig{
		IssuerURL:    "https://keycloak.example.com/realms/openshift",
		Audiences:    []string{"openshift"},
		ClientSecret: "", // No client secret
	}

	manifests, err := GenerateOIDCManifests(cfg)
	if err != nil {
		t.Fatalf("GenerateOIDCManifests failed: %v", err)
	}

	// Should only have auth manifest
	if len(manifests) != 1 {
		t.Fatalf("expected 1 manifest, got %d", len(manifests))
	}

	// Should NOT have client secret manifest
	if _, ok := manifests["cluster-oidc-client-secret.yaml"]; ok {
		t.Error("cluster-oidc-client-secret.yaml should not be generated when ClientSecret is empty")
	}
}
