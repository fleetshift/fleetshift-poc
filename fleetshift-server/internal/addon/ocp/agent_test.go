package ocp

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"gopkg.in/yaml.v3"
)

func TestPrepareWorkDir(t *testing.T) {
	spec := &ClusterSpec{
		Name:         "test-cluster",
		BaseDomain:   "example.com",
		Region:       "us-east-1",
		RoleARN:      "arn:aws:iam::123456789012:role/test",
		ReleaseImage: "quay.io/ocp-release:4.21.0",
	}
	pullSecret := []byte(`{"auths":{}}`)
	sshPubKey := []byte("ssh-ed25519 AAAAC3test")

	configPath, workDir, err := prepareWorkDir("test-cluster", spec, "us-east-1", pullSecret, sshPubKey, domain.DeliveryAuth{})
	if err != nil {
		t.Fatalf("prepareWorkDir: %v", err)
	}
	defer os.RemoveAll(workDir)

	// Verify pull secret was written
	ps, err := os.ReadFile(workDir + "/pull-secret.json")
	if err != nil {
		t.Fatalf("read pull-secret.json: %v", err)
	}
	if string(ps) != string(pullSecret) {
		t.Errorf("pull secret content mismatch")
	}

	// Verify cluster.yaml was written and is valid YAML
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read cluster.yaml: %v", err)
	}

	var cfg map[string]any
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		t.Fatalf("cluster.yaml is not valid YAML: %v", err)
	}

	if cfg["baseDomain"] != "example.com" {
		t.Errorf("baseDomain = %v, want example.com", cfg["baseDomain"])
	}
	if cfg["credentialsMode"] != "Manual" {
		t.Errorf("credentialsMode = %v, want Manual", cfg["credentialsMode"])
	}
}

func TestNewAgent_DefaultCredentials(t *testing.T) {
	t.Setenv("AWS_ACCESS_KEY_ID", "test-key")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("OCP_PULL_SECRET_FILE", "/nonexistent/pull-secret.json")

	a := NewAgent()

	// Agent should still be created despite bad pull secret path
	if a.credentials == nil {
		t.Fatal("credentials should not be nil")
	}
	if a.tokenSigner == nil {
		t.Fatal("tokenSigner should not be nil")
	}
}

func TestEffectiveProvisionTimeout(t *testing.T) {
	a := NewAgent()
	got := a.effectiveProvisionTimeout()
	if got != defaultProvisionSTSDuration {
		t.Errorf("got %v, want %v", got, defaultProvisionSTSDuration)
	}
}

func TestWriteDestroyMetadata(t *testing.T) {
	workDir := t.TempDir()
	err := writeDestroyMetadata(workDir, "infra-abc", "cluster-uuid", "us-west-2")
	if err != nil {
		t.Fatalf("writeDestroyMetadata: %v", err)
	}

	data, err := os.ReadFile(workDir + "/metadata.json")
	if err != nil {
		t.Fatalf("read metadata.json: %v", err)
	}

	var meta map[string]any
	if err := json.Unmarshal(data, &meta); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if meta["infraID"] != "infra-abc" {
		t.Errorf("infraID = %v, want infra-abc", meta["infraID"])
	}
	if meta["clusterID"] != "cluster-uuid" {
		t.Errorf("clusterID = %v, want cluster-uuid", meta["clusterID"])
	}

	aws, ok := meta["aws"].(map[string]any)
	if !ok {
		t.Fatal("missing aws section")
	}
	if aws["region"] != "us-west-2" {
		t.Errorf("region = %v, want us-west-2", aws["region"])
	}
}

func TestValidateCredentialModeCoupling(t *testing.T) {
	tests := []struct {
		name      string
		token     string
		stsMode   bool
		wantError bool
	}{
		{"STS creds + mint mode = rejected", "sts-session-token", false, true},
		{"STS creds + STS mode = allowed", "sts-session-token", true, false},
		{"long-lived keys + mint mode = allowed", "", false, false},
		{"long-lived keys + STS mode = allowed", "", true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			creds := &AWSCredentials{
				AccessKeyID:     "AKIA",
				SecretAccessKey: "secret",
				SessionToken:    tt.token,
			}
			err := validateCredentialModeCoupling(creds, tt.stsMode)
			if (err != nil) != tt.wantError {
				t.Errorf("validateCredentialModeCoupling() error = %v, wantError = %v", err, tt.wantError)
			}
		})
	}
}

func TestProvisionWorkDirPath(t *testing.T) {
	got := provisionWorkDirPath("my-cluster")
	expected := filepath.Join(os.TempDir(), "ocp-provision-my-cluster")
	if got != expected {
		t.Errorf("provisionWorkDirPath(%q) = %q, want %q", "my-cluster", got, expected)
	}
}

func TestPrepareWorkDir_DeterministicPath(t *testing.T) {
	spec := &ClusterSpec{
		Name:       "det-test",
		BaseDomain: "example.com",
		Region:     "us-east-1",
		RoleARN:    "arn:aws:iam::123:role/test",
	}

	_, workDir, err := prepareWorkDir("det-test", spec, "us-east-1", []byte(`{"auths":{}}`), []byte("ssh-ed25519 KEY"), domain.DeliveryAuth{})
	if err != nil {
		t.Fatalf("prepareWorkDir: %v", err)
	}
	defer os.RemoveAll(workDir)

	expected := filepath.Join(os.TempDir(), "ocp-provision-det-test")
	if workDir != expected {
		t.Errorf("workDir = %q, want %q", workDir, expected)
	}
}

func TestPrepareWorkDir_OIDCManifests(t *testing.T) {
	spec := &ClusterSpec{
		Name:       "oidc-test",
		BaseDomain: "example.com",
		Region:     "us-east-1",
		RoleARN:    "arn:aws:iam::123:role/test",
	}

	auth := domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "user-123",
				Issuer:  "https://keycloak.example.com/realms/fleetshift",
			},
			Extra: map[string][]string{
				"email": {"user@example.com"},
			},
		},
		Audience: []domain.Audience{"fleetshift-cli"},
	}

	_, workDir, err := prepareWorkDir("oidc-test", spec, "us-east-1", []byte(`{"auths":{}}`), []byte("ssh-ed25519 KEY"), auth)
	if err != nil {
		t.Fatalf("prepareWorkDir: %v", err)
	}
	defer os.RemoveAll(workDir)

	// Verify extra-manifests directory was created
	extraDir := filepath.Join(workDir, "extra-manifests")
	if _, err := os.Stat(extraDir); err != nil {
		t.Fatalf("extra-manifests dir not found: %v", err)
	}

	// Verify Authentication CR was written
	authFile := filepath.Join(extraDir, "cluster-authentication-oidc.yaml")
	data, err := os.ReadFile(authFile)
	if err != nil {
		t.Fatalf("read auth manifest: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "type: OIDC") {
		t.Error("auth manifest missing 'type: OIDC'")
	}
	if !strings.Contains(content, "https://keycloak.example.com/realms/fleetshift") {
		t.Error("auth manifest missing issuer URL")
	}
	if !strings.Contains(content, "fleetshift-cli") {
		t.Error("auth manifest missing audience/client ID")
	}
}

func TestPrepareWorkDir_NoOIDCWithoutCaller(t *testing.T) {
	spec := &ClusterSpec{
		Name:       "no-oidc-test",
		BaseDomain: "example.com",
		Region:     "us-east-1",
		RoleARN:    "arn:aws:iam::123:role/test",
	}

	// Empty auth — no caller, no OIDC manifests should be generated
	_, workDir, err := prepareWorkDir("no-oidc-test", spec, "us-east-1", []byte(`{"auths":{}}`), []byte("ssh-ed25519 KEY"), domain.DeliveryAuth{})
	if err != nil {
		t.Fatalf("prepareWorkDir: %v", err)
	}
	defer os.RemoveAll(workDir)

	extraDir := filepath.Join(workDir, "extra-manifests")
	if _, err := os.Stat(extraDir); !os.IsNotExist(err) {
		t.Error("extra-manifests dir should not exist when no caller is present")
	}
}
