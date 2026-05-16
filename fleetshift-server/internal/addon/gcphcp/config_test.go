package gcphcp_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/gcphcp"
)

func TestParseConfig_Valid(t *testing.T) {
	configYAML := `gateway:
  url: "https://hcp-backend-gateway.example.invalid"
  audience: "test-client-id.apps.googleusercontent.com"
targets:
  - id: "gcphcp-example-region-staging"
    gcp_project: "example-hcp-target-project"
    region: "us-central1"
    workforce_pool: "example-workforce-pool"
    workforce_provider: "example-oidc-provider"
    broker_sa_email: "hcp-idtoken-broker@example.iam.gserviceaccount.com"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	cfg, err := gcphcp.ParseConfig(configPath)
	if err != nil {
		t.Fatalf("ParseConfig failed: %v", err)
	}

	// Verify gateway
	if cfg.Gateway.URL != "https://hcp-backend-gateway.example.invalid" {
		t.Errorf("unexpected gateway URL: %s", cfg.Gateway.URL)
	}
	if cfg.Gateway.Audience != "test-client-id.apps.googleusercontent.com" {
		t.Errorf("unexpected gateway audience: %s", cfg.Gateway.Audience)
	}

	// Verify targets
	if len(cfg.Targets) != 1 {
		t.Fatalf("expected 1 target, got %d", len(cfg.Targets))
	}

	target := cfg.Targets[0]
	if target.ID != "gcphcp-example-region-staging" {
		t.Errorf("unexpected target ID: %s", target.ID)
	}
	if target.GCPProject != "example-hcp-target-project" {
		t.Errorf("unexpected target GCP project: %s", target.GCPProject)
	}
	if target.Region != "us-central1" {
		t.Errorf("unexpected target region: %s", target.Region)
	}
	if target.WorkforcePool != "example-workforce-pool" {
		t.Errorf("unexpected target workforce pool: %s", target.WorkforcePool)
	}
	if target.WorkforceProvider != "example-oidc-provider" {
		t.Errorf("unexpected target workforce provider: %s", target.WorkforceProvider)
	}
	if target.BrokerSAEmail != "hcp-idtoken-broker@example.iam.gserviceaccount.com" {
		t.Errorf("unexpected target broker SA email: %s", target.BrokerSAEmail)
	}

	// Verify TargetProperties
	props := target.TargetProperties()
	expectedProps := map[string]string{
		"id":                 "gcphcp-example-region-staging",
		"gcp_project":        "example-hcp-target-project",
		"region":             "us-central1",
		"workforce_pool":     "example-workforce-pool",
		"workforce_provider": "example-oidc-provider",
		"broker_sa_email":    "hcp-idtoken-broker@example.iam.gserviceaccount.com",
	}
	for key, expected := range expectedProps {
		if props[key] != expected {
			t.Errorf("TargetProperties[%s] = %s, want %s", key, props[key], expected)
		}
	}
}

func TestParseConfig_RejectsMultipleTargets(t *testing.T) {
	configYAML := `gateway:
  url: "https://hcp-backend-gateway.example.invalid"
  audience: "test-client-id.apps.googleusercontent.com"
targets:
  - id: "gcphcp-example-region-staging"
    gcp_project: "example-hcp-target-project"
    region: "us-central1"
    workforce_pool: "example-workforce-pool"
    workforce_provider: "example-oidc-provider"
    broker_sa_email: "hcp-idtoken-broker@example.iam.gserviceaccount.com"
  - id: "gcphcp-another-region-prod"
    gcp_project: "another-hcp-target-project"
    region: "us-east1"
    workforce_pool: "another-workforce-pool"
    workforce_provider: "another-oidc-provider"
    broker_sa_email: "hcp-broker@another.iam.gserviceaccount.com"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	_, err := gcphcp.ParseConfig(configPath)
	if err == nil {
		t.Fatal("expected error for multiple targets, got nil")
	}
}

func TestParseConfig_MissingFile(t *testing.T) {
	_, err := gcphcp.ParseConfig("/nonexistent/path/config.yaml")
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

func TestParseConfig_RejectsUnknownFields(t *testing.T) {
	configYAML := `gateway:
  url: "https://hcp-backend-gateway.example.invalid"
  audience: "test-client-id.apps.googleusercontent.com"
targets:
  - id: "gcphcp-example-region-staging"
    gcp_project: "example-hcp-target-project"
    region: "us-central1"
    workforce_pool: "example-workforce-pool"
    workforce_provier: "example-oidc-provider"
    broker_sa_email: "hcp-idtoken-broker@example.iam.gserviceaccount.com"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	_, err := gcphcp.ParseConfig(configPath)
	if err == nil {
		t.Fatal("expected error for unknown field, got nil")
	}
	if !strings.Contains(err.Error(), "field workforce_provier not found") {
		t.Fatalf("expected unknown field error, got %v", err)
	}
}

func TestParseConfig_MissingGatewayURL(t *testing.T) {
	configYAML := `gateway:
  audience: "test-client-id.apps.googleusercontent.com"
targets:
  - id: "gcphcp-example-region-staging"
    gcp_project: "example-hcp-target-project"
    region: "us-central1"
    workforce_pool: "example-workforce-pool"
    workforce_provider: "example-oidc-provider"
    broker_sa_email: "hcp-idtoken-broker@example.iam.gserviceaccount.com"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	_, err := gcphcp.ParseConfig(configPath)
	if err == nil {
		t.Fatal("expected error for missing gateway URL, got nil")
	}
}

func TestParseConfig_MissingGatewayAudience(t *testing.T) {
	configYAML := `gateway:
  url: "https://hcp-backend-gateway.example.invalid"
targets:
  - id: "gcphcp-example-region-staging"
    gcp_project: "example-hcp-target-project"
    region: "us-central1"
    workforce_pool: "example-workforce-pool"
    workforce_provider: "example-oidc-provider"
    broker_sa_email: "hcp-idtoken-broker@example.iam.gserviceaccount.com"
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	_, err := gcphcp.ParseConfig(configPath)
	if err == nil {
		t.Fatal("expected error for missing gateway audience, got nil")
	}
}

func TestParseConfig_EmptyTargets(t *testing.T) {
	configYAML := `gateway:
  url: "https://hcp-backend-gateway.example.invalid"
  audience: "test-client-id.apps.googleusercontent.com"
targets: []
`

	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.yaml")
	if err := os.WriteFile(configPath, []byte(configYAML), 0644); err != nil {
		t.Fatalf("failed to write test config: %v", err)
	}

	_, err := gcphcp.ParseConfig(configPath)
	if err == nil {
		t.Fatal("expected error for empty targets, got nil")
	}
}

func TestParseConfig_IncompleteTargetFields(t *testing.T) {
	tests := []struct {
		name       string
		configYAML string
	}{
		{
			name: "missing ID",
			configYAML: `gateway:
  url: "https://hcp-backend-gateway.example.invalid"
  audience: "test-client-id.apps.googleusercontent.com"
targets:
  - gcp_project: "example-hcp-target-project"
    region: "us-central1"
    workforce_pool: "example-workforce-pool"
    workforce_provider: "example-oidc-provider"
    broker_sa_email: "hcp-idtoken-broker@example.iam.gserviceaccount.com"
`,
		},
		{
			name: "missing GCP project",
			configYAML: `gateway:
  url: "https://hcp-backend-gateway.example.invalid"
  audience: "test-client-id.apps.googleusercontent.com"
targets:
  - id: "gcphcp-example-region-staging"
    region: "us-central1"
    workforce_pool: "example-workforce-pool"
    workforce_provider: "example-oidc-provider"
    broker_sa_email: "hcp-idtoken-broker@example.iam.gserviceaccount.com"
`,
		},
		{
			name: "missing region",
			configYAML: `gateway:
  url: "https://hcp-backend-gateway.example.invalid"
  audience: "test-client-id.apps.googleusercontent.com"
targets:
  - id: "gcphcp-example-region-staging"
    gcp_project: "example-hcp-target-project"
    workforce_pool: "example-workforce-pool"
    workforce_provider: "example-oidc-provider"
    broker_sa_email: "hcp-idtoken-broker@example.iam.gserviceaccount.com"
`,
		},
		{
			name: "missing workforce pool",
			configYAML: `gateway:
  url: "https://hcp-backend-gateway.example.invalid"
  audience: "test-client-id.apps.googleusercontent.com"
targets:
  - id: "gcphcp-example-region-staging"
    gcp_project: "example-hcp-target-project"
    region: "us-central1"
    workforce_provider: "example-oidc-provider"
    broker_sa_email: "hcp-idtoken-broker@example.iam.gserviceaccount.com"
`,
		},
		{
			name: "missing workforce provider",
			configYAML: `gateway:
  url: "https://hcp-backend-gateway.example.invalid"
  audience: "test-client-id.apps.googleusercontent.com"
targets:
  - id: "gcphcp-example-region-staging"
    gcp_project: "example-hcp-target-project"
    region: "us-central1"
    workforce_pool: "example-workforce-pool"
    broker_sa_email: "hcp-idtoken-broker@example.iam.gserviceaccount.com"
`,
		},
		{
			name: "missing broker SA email",
			configYAML: `gateway:
  url: "https://hcp-backend-gateway.example.invalid"
  audience: "test-client-id.apps.googleusercontent.com"
targets:
  - id: "gcphcp-example-region-staging"
    gcp_project: "example-hcp-target-project"
    region: "us-central1"
    workforce_pool: "example-workforce-pool"
    workforce_provider: "example-oidc-provider"
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpDir := t.TempDir()
			configPath := filepath.Join(tmpDir, "config.yaml")
			if err := os.WriteFile(configPath, []byte(tt.configYAML), 0644); err != nil {
				t.Fatalf("failed to write test config: %v", err)
			}

			_, err := gcphcp.ParseConfig(configPath)
			if err == nil {
				t.Fatalf("expected error for %s, got nil", tt.name)
			}
		})
	}
}
