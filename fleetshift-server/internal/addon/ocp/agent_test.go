package ocp

import (
	"encoding/json"
	"os"
	"testing"

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

	configPath, workDir, err := prepareWorkDir("test-cluster", spec, "us-east-1", pullSecret, sshPubKey)
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

func TestValidateCredentialModeCoupling_RejectSTSWithMintMode(t *testing.T) {
	creds := &AWSCredentials{
		AccessKeyID:     "ASIA",
		SecretAccessKey: "secret",
		SessionToken:    "sts-session-token",
	}
	err := validateCredentialModeCoupling(creds, false)
	if err == nil {
		t.Fatal("expected error: STS creds with mint mode should be rejected")
	}
}

func TestValidateCredentialModeCoupling_AllowSTSWithSTSMode(t *testing.T) {
	creds := &AWSCredentials{
		AccessKeyID:     "ASIA",
		SecretAccessKey: "secret",
		SessionToken:    "sts-session-token",
	}
	if err := validateCredentialModeCoupling(creds, true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateCredentialModeCoupling_AllowLongLivedWithMintMode(t *testing.T) {
	creds := &AWSCredentials{
		AccessKeyID:     "AKIA",
		SecretAccessKey: "secret",
	}
	if err := validateCredentialModeCoupling(creds, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
