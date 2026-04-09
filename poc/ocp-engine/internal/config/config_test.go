package config

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func getTestdataPath(filename string) string {
	_, thisFile, _, _ := runtime.Caller(0)
	repoRoot := filepath.Join(filepath.Dir(thisFile), "..", "..")
	return filepath.Join(repoRoot, "testdata", filename)
}

func TestLoadConfig_Minimal(t *testing.T) {
	path := getTestdataPath("cluster-minimal.yaml")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Check engine fields
	if cfg.Engine.PullSecretFile != "/tmp/pull-secret.json" {
		t.Errorf("pull_secret_file = %q, want /tmp/pull-secret.json", cfg.Engine.PullSecretFile)
	}
	if cfg.Engine.Credentials.AccessKeyID != "AKIAIOSFODNN7EXAMPLE" {
		t.Errorf("access_key_id = %q, want AKIAIOSFODNN7EXAMPLE", cfg.Engine.Credentials.AccessKeyID)
	}

	// Check install-config pass-through
	if cfg.InstallConfig["baseDomain"] != "example.com" {
		t.Errorf("baseDomain = %v, want example.com", cfg.InstallConfig["baseDomain"])
	}
	metadata := cfg.InstallConfig["metadata"].(map[string]any)
	if metadata["name"] != "test-cluster" {
		t.Errorf("metadata.name = %v, want test-cluster", metadata["name"])
	}
	platform := cfg.InstallConfig["platform"].(map[string]any)
	aws := platform["aws"].(map[string]any)
	if aws["region"] != "us-east-1" {
		t.Errorf("platform.aws.region = %v, want us-east-1", aws["region"])
	}
}

func TestLoadConfig_Full(t *testing.T) {
	path := getTestdataPath("cluster-full.yaml")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Check engine fields
	if cfg.Engine.SSHPublicKeyFile != "/tmp/id_rsa.pub" {
		t.Errorf("ssh_public_key_file = %q, want /tmp/id_rsa.pub", cfg.Engine.SSHPublicKeyFile)
	}
	if cfg.Engine.ReleaseImage != "quay.io/openshift-release-dev/ocp-release:4.20.0-x86_64" {
		t.Errorf("release_image = %q", cfg.Engine.ReleaseImage)
	}

	// Check install-config pass-through
	if cfg.InstallConfig["baseDomain"] != "prod.example.com" {
		t.Errorf("baseDomain = %v, want prod.example.com", cfg.InstallConfig["baseDomain"])
	}
	if cfg.InstallConfig["publish"] != "External" {
		t.Errorf("publish = %v, want External", cfg.InstallConfig["publish"])
	}
	if cfg.InstallConfig["fips"] != false {
		t.Errorf("fips = %v, want false", cfg.InstallConfig["fips"])
	}

	// ocp_engine should not leak into install-config
	if _, ok := cfg.InstallConfig["ocp_engine"]; ok {
		t.Error("ocp_engine should be stripped from install-config pass-through")
	}
}

func TestLoadConfig_OcpEngineStripped(t *testing.T) {
	path := getTestdataPath("cluster-minimal.yaml")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}
	if _, ok := cfg.InstallConfig["ocp_engine"]; ok {
		t.Error("ocp_engine key should be removed from InstallConfig")
	}
}

func TestLoadConfig_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		errContains string
	}{
		{
			name: "missing pull_secret_file",
			yaml: `ocp_engine:
  credentials:
    access_key_id: "test"
    secret_access_key: "test"
baseDomain: example.com
metadata:
  name: test
platform:
  aws:
    region: us-east-1
`,
			errContains: "pull_secret_file",
		},
		{
			name: "missing credentials",
			yaml: `ocp_engine:
  pull_secret_file: /tmp/ps.json
baseDomain: example.com
metadata:
  name: test
platform:
  aws:
    region: us-east-1
`,
			errContains: "credentials",
		},
		{
			name: "missing baseDomain",
			yaml: `ocp_engine:
  pull_secret_file: /tmp/ps.json
  credentials:
    access_key_id: "test"
    secret_access_key: "test"
metadata:
  name: test
platform:
  aws:
    region: us-east-1
`,
			errContains: "baseDomain",
		},
		{
			name: "missing metadata.name",
			yaml: `ocp_engine:
  pull_secret_file: /tmp/ps.json
  credentials:
    access_key_id: "test"
    secret_access_key: "test"
baseDomain: example.com
platform:
  aws:
    region: us-east-1
`,
			errContains: "metadata.name",
		},
		{
			name: "missing platform.aws.region",
			yaml: `ocp_engine:
  pull_secret_file: /tmp/ps.json
  credentials:
    access_key_id: "test"
    secret_access_key: "test"
baseDomain: example.com
metadata:
  name: test
platform:
  aws: {}
`,
			errContains: "platform.aws.region",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseConfig([]byte(tt.yaml))
			if err == nil {
				t.Errorf("expected error containing %q, got nil", tt.errContains)
			} else if !strings.Contains(err.Error(), tt.errContains) {
				t.Errorf("expected error containing %q, got %v", tt.errContains, err)
			}
		})
	}
}

func TestLoadConfig_CredentialModes(t *testing.T) {
	base := `baseDomain: example.com
metadata:
  name: test
platform:
  aws:
    region: us-east-1
`
	tests := []struct {
		name   string
		engine string
	}{
		{
			name: "inline",
			engine: `ocp_engine:
  pull_secret_file: /tmp/ps.json
  credentials:
    access_key_id: "AKIA"
    secret_access_key: "secret"
`,
		},
		{
			name: "file",
			engine: `ocp_engine:
  pull_secret_file: /tmp/ps.json
  credentials:
    credentials_file: /home/user/.aws/credentials
`,
		},
		{
			name: "profile",
			engine: `ocp_engine:
  pull_secret_file: /tmp/ps.json
  credentials:
    profile: default
`,
		},
		{
			name: "role_arn",
			engine: `ocp_engine:
  pull_secret_file: /tmp/ps.json
  credentials:
    role_arn: "arn:aws:iam::123456789012:role/OCPRole"
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := ParseConfig([]byte(tt.engine + base))
			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if cfg == nil {
				t.Error("expected config, got nil")
			}
		})
	}
}

func TestExpandTilde(t *testing.T) {
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("cannot determine home directory")
	}

	result := expandTilde("~/some/path")
	expected := filepath.Join(home, "some/path")
	if result != expected {
		t.Errorf("expandTilde(~/some/path) = %q, want %q", result, expected)
	}
}

func TestExpandTilde_NoTilde(t *testing.T) {
	result := expandTilde("/absolute/path")
	if result != "/absolute/path" {
		t.Errorf("expandTilde should not modify absolute path, got %q", result)
	}
}

func TestPassThroughFieldsPreserved(t *testing.T) {
	yaml := `ocp_engine:
  pull_secret_file: /tmp/ps.json
  credentials:
    access_key_id: "test"
    secret_access_key: "test"
baseDomain: example.com
metadata:
  name: test
platform:
  aws:
    region: us-east-1
    subnets:
      - subnet-abc123
proxy:
  httpProxy: http://proxy:8080
credentialsMode: Mint
`
	cfg, err := ParseConfig([]byte(yaml))
	if err != nil {
		t.Fatalf("ParseConfig: %v", err)
	}

	// Fields that would have been "Not Yet Exposed" before should pass through
	if cfg.InstallConfig["credentialsMode"] != "Mint" {
		t.Errorf("credentialsMode not preserved: %v", cfg.InstallConfig["credentialsMode"])
	}
	proxy, ok := cfg.InstallConfig["proxy"].(map[string]any)
	if !ok {
		t.Fatal("proxy section not preserved")
	}
	if proxy["httpProxy"] != "http://proxy:8080" {
		t.Errorf("proxy.httpProxy = %v", proxy["httpProxy"])
	}
}

// --- GenerateInstallConfig tests ---

func loadMinimalWithPullSecret(t *testing.T) *ClusterConfig {
	t.Helper()

	pullSecretFile, err := os.CreateTemp("", "pull-secret-*.json")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(pullSecretFile.Name()) })
	pullSecretFile.Write([]byte(`{"auths":{}}`))
	pullSecretFile.Close()

	path := getTestdataPath("cluster-minimal.yaml")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	cfg.Engine.PullSecretFile = pullSecretFile.Name()
	return cfg
}

func TestGenerateInstallConfig_InlinesPullSecret(t *testing.T) {
	cfg := loadMinimalWithPullSecret(t)

	result, err := GenerateInstallConfig(cfg)
	if err != nil {
		t.Fatalf("GenerateInstallConfig: %v", err)
	}

	var ic map[string]any
	if err := yaml.Unmarshal(result, &ic); err != nil {
		t.Fatalf("invalid YAML: %v", err)
	}
	if ic["pullSecret"] != `{"auths":{}}` {
		t.Errorf("pullSecret = %v, want {\"auths\":{}}", ic["pullSecret"])
	}
}

func TestGenerateInstallConfig_SetsApiVersion(t *testing.T) {
	cfg := loadMinimalWithPullSecret(t)
	delete(cfg.InstallConfig, "apiVersion")

	result, err := GenerateInstallConfig(cfg)
	if err != nil {
		t.Fatalf("GenerateInstallConfig: %v", err)
	}

	var ic map[string]any
	yaml.Unmarshal(result, &ic)
	if ic["apiVersion"] != "v1" {
		t.Errorf("apiVersion = %v, want v1", ic["apiVersion"])
	}
}

func TestGenerateInstallConfig_InlinesSSHKey(t *testing.T) {
	cfg := loadMinimalWithPullSecret(t)

	sshKeyFile, err := os.CreateTemp("", "id_rsa-*.pub")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(sshKeyFile.Name())
	sshKey := "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQC test@example.com"
	sshKeyFile.Write([]byte(sshKey))
	sshKeyFile.Close()

	cfg.Engine.SSHPublicKeyFile = sshKeyFile.Name()

	result, err := GenerateInstallConfig(cfg)
	if err != nil {
		t.Fatalf("GenerateInstallConfig: %v", err)
	}

	var ic map[string]any
	yaml.Unmarshal(result, &ic)
	if ic["sshKey"] != sshKey {
		t.Errorf("sshKey = %v, want %s", ic["sshKey"], sshKey)
	}
}

func TestGenerateInstallConfig_PreservesPassThrough(t *testing.T) {
	cfg := loadMinimalWithPullSecret(t)

	result, err := GenerateInstallConfig(cfg)
	if err != nil {
		t.Fatalf("GenerateInstallConfig: %v", err)
	}

	var ic map[string]any
	yaml.Unmarshal(result, &ic)

	if ic["baseDomain"] != "example.com" {
		t.Errorf("baseDomain = %v, want example.com", ic["baseDomain"])
	}
	metadata := ic["metadata"].(map[string]any)
	if metadata["name"] != "test-cluster" {
		t.Errorf("metadata.name = %v, want test-cluster", metadata["name"])
	}
}

func TestGenerateInstallConfig_NoOcpEngineInOutput(t *testing.T) {
	cfg := loadMinimalWithPullSecret(t)

	result, err := GenerateInstallConfig(cfg)
	if err != nil {
		t.Fatalf("GenerateInstallConfig: %v", err)
	}

	var ic map[string]any
	yaml.Unmarshal(result, &ic)
	if _, ok := ic["ocp_engine"]; ok {
		t.Error("ocp_engine should not appear in generated install-config")
	}
}
