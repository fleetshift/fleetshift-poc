package config

import (
	"os"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestGenerateInstallConfig_Minimal(t *testing.T) {
	// Create temp pull secret file
	pullSecretFile, err := os.CreateTemp("", "pull-secret-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(pullSecretFile.Name())

	if _, err := pullSecretFile.Write([]byte(`{"auths":{}}`)); err != nil {
		t.Fatal(err)
	}
	pullSecretFile.Close()

	// Load minimal config
	path := getTestdataPath("cluster-minimal.yaml")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Override PullSecretFile to temp path
	cfg.PullSecretFile = pullSecretFile.Name()

	// Call GenerateInstallConfig
	result, err := GenerateInstallConfig(cfg)
	if err != nil {
		t.Fatalf("GenerateInstallConfig() error = %v", err)
	}

	// Unmarshal result as YAML map
	var installConfig map[string]interface{}
	if err := yaml.Unmarshal(result, &installConfig); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	// Verify baseDomain
	if installConfig["baseDomain"] != "example.com" {
		t.Errorf("expected baseDomain=example.com, got %v", installConfig["baseDomain"])
	}

	// Verify metadata.name
	metadata, ok := installConfig["metadata"].(map[string]interface{})
	if !ok {
		t.Fatal("metadata is not a map")
	}
	if metadata["name"] != "test-cluster" {
		t.Errorf("expected metadata.name=test-cluster, got %v", metadata["name"])
	}

	// Verify platform.aws.region
	platform, ok := installConfig["platform"].(map[string]interface{})
	if !ok {
		t.Fatal("platform is not a map")
	}
	aws, ok := platform["aws"].(map[string]interface{})
	if !ok {
		t.Fatal("platform.aws is not a map")
	}
	if aws["region"] != "us-east-1" {
		t.Errorf("expected platform.aws.region=us-east-1, got %v", aws["region"])
	}
}

func TestGenerateInstallConfig_WithSSHKey(t *testing.T) {
	// Create temp pull secret file
	pullSecretFile, err := os.CreateTemp("", "pull-secret-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(pullSecretFile.Name())

	if _, err := pullSecretFile.Write([]byte(`{"auths":{}}`)); err != nil {
		t.Fatal(err)
	}
	pullSecretFile.Close()

	// Create temp SSH key file
	sshKeyFile, err := os.CreateTemp("", "id_rsa-*.pub")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(sshKeyFile.Name())

	sshKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC test@example.com"
	if _, err := sshKeyFile.Write([]byte(sshKey)); err != nil {
		t.Fatal(err)
	}
	sshKeyFile.Close()

	// Load minimal config
	path := getTestdataPath("cluster-minimal.yaml")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Override file paths
	cfg.PullSecretFile = pullSecretFile.Name()
	cfg.SSHPublicKeyFile = sshKeyFile.Name()

	// Call GenerateInstallConfig
	result, err := GenerateInstallConfig(cfg)
	if err != nil {
		t.Fatalf("GenerateInstallConfig() error = %v", err)
	}

	// Unmarshal result as YAML map
	var installConfig map[string]interface{}
	if err := yaml.Unmarshal(result, &installConfig); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	// Verify sshKey field in output
	if installConfig["sshKey"] != sshKey {
		t.Errorf("expected sshKey=%s, got %v", sshKey, installConfig["sshKey"])
	}
}

func TestGenerateInstallConfig_NetworkingDefaults(t *testing.T) {
	// Create temp pull secret file
	pullSecretFile, err := os.CreateTemp("", "pull-secret-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(pullSecretFile.Name())

	if _, err := pullSecretFile.Write([]byte(`{"auths":{}}`)); err != nil {
		t.Fatal(err)
	}
	pullSecretFile.Close()

	// Load minimal config
	path := getTestdataPath("cluster-minimal.yaml")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Override PullSecretFile to temp path
	cfg.PullSecretFile = pullSecretFile.Name()

	// Call GenerateInstallConfig
	result, err := GenerateInstallConfig(cfg)
	if err != nil {
		t.Fatalf("GenerateInstallConfig() error = %v", err)
	}

	// Unmarshal result as YAML map
	var installConfig map[string]interface{}
	if err := yaml.Unmarshal(result, &installConfig); err != nil {
		t.Fatalf("failed to unmarshal result: %v", err)
	}

	// Verify networking
	networking, ok := installConfig["networking"].(map[string]interface{})
	if !ok {
		t.Fatal("networking is not a map")
	}

	// Verify clusterNetwork[0].cidr
	clusterNetwork, ok := networking["clusterNetwork"].([]interface{})
	if !ok {
		t.Fatal("networking.clusterNetwork is not an array")
	}
	if len(clusterNetwork) == 0 {
		t.Fatal("networking.clusterNetwork is empty")
	}

	firstClusterNetwork, ok := clusterNetwork[0].(map[string]interface{})
	if !ok {
		t.Fatal("networking.clusterNetwork[0] is not a map")
	}

	if firstClusterNetwork["cidr"] != "10.128.0.0/14" {
		t.Errorf("expected networking.clusterNetwork[0].cidr=10.128.0.0/14, got %v", firstClusterNetwork["cidr"])
	}
}
