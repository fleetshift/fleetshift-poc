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

func TestPrepareWorkDir_InvalidSpec(t *testing.T) {
	// nil spec causes a panic in BuildClusterYAML (nil pointer dereference)
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for nil spec")
		}
	}()
	prepareWorkDir("test", nil, "us-east-1", []byte("{}"), []byte("key"))
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
