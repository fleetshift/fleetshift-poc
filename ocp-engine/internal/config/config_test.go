package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
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

	// Check required fields
	if cfg.Cluster.Name != "test-cluster" {
		t.Errorf("expected name=test-cluster, got %s", cfg.Cluster.Name)
	}
	if cfg.Cluster.BaseDomain != "example.com" {
		t.Errorf("expected base_domain=example.com, got %s", cfg.Cluster.BaseDomain)
	}
	if cfg.Cluster.Version != "4.20" {
		t.Errorf("expected version=4.20, got %s", cfg.Cluster.Version)
	}
	if cfg.Platform.AWS.Region != "us-east-1" {
		t.Errorf("expected region=us-east-1, got %s", cfg.Platform.AWS.Region)
	}
	if cfg.Platform.AWS.Credentials.AccessKeyID != "AKIAIOSFODNN7EXAMPLE" {
		t.Errorf("expected access_key_id=AKIAIOSFODNN7EXAMPLE, got %s", cfg.Platform.AWS.Credentials.AccessKeyID)
	}
	if cfg.Platform.AWS.Credentials.SecretAccessKey != "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" {
		t.Errorf("expected secret_access_key to match, got %s", cfg.Platform.AWS.Credentials.SecretAccessKey)
	}
	if cfg.PullSecretFile != "/tmp/pull-secret.json" {
		t.Errorf("expected pull_secret_file=/tmp/pull-secret.json, got %s", cfg.PullSecretFile)
	}
}

func TestLoadConfig_AppliesDefaults(t *testing.T) {
	path := getTestdataPath("cluster-minimal.yaml")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Check defaults for control plane
	if cfg.ControlPlane.Replicas != 3 {
		t.Errorf("expected control_plane.replicas=3, got %d", cfg.ControlPlane.Replicas)
	}
	if cfg.ControlPlane.InstanceType != "m6a.xlarge" {
		t.Errorf("expected control_plane.instance_type=m6a.xlarge, got %s", cfg.ControlPlane.InstanceType)
	}
	if cfg.ControlPlane.RootVolume.SizeGB != 120 {
		t.Errorf("expected control_plane.root_volume.size_gb=120, got %d", cfg.ControlPlane.RootVolume.SizeGB)
	}
	if cfg.ControlPlane.RootVolume.Type != "gp3" {
		t.Errorf("expected control_plane.root_volume.type=gp3, got %s", cfg.ControlPlane.RootVolume.Type)
	}

	// Check defaults for compute
	if cfg.Compute.Replicas != 3 {
		t.Errorf("expected compute.replicas=3, got %d", cfg.Compute.Replicas)
	}
	if cfg.Compute.InstanceType != "m6a.xlarge" {
		t.Errorf("expected compute.instance_type=m6a.xlarge, got %s", cfg.Compute.InstanceType)
	}
	if cfg.Compute.RootVolume.SizeGB != 120 {
		t.Errorf("expected compute.root_volume.size_gb=120, got %d", cfg.Compute.RootVolume.SizeGB)
	}
	if cfg.Compute.RootVolume.Type != "gp3" {
		t.Errorf("expected compute.root_volume.type=gp3, got %s", cfg.Compute.RootVolume.Type)
	}

	// Check defaults for networking
	if cfg.Networking.ClusterNetwork != "10.128.0.0/14" {
		t.Errorf("expected networking.cluster_network=10.128.0.0/14, got %s", cfg.Networking.ClusterNetwork)
	}
	if cfg.Networking.ServiceNetwork != "172.30.0.0/16" {
		t.Errorf("expected networking.service_network=172.30.0.0/16, got %s", cfg.Networking.ServiceNetwork)
	}
	if cfg.Networking.MachineNetwork != "10.0.0.0/16" {
		t.Errorf("expected networking.machine_network=10.0.0.0/16, got %s", cfg.Networking.MachineNetwork)
	}
	if cfg.Networking.HostPrefix != 23 {
		t.Errorf("expected networking.host_prefix=23, got %d", cfg.Networking.HostPrefix)
	}

	// Check default publish
	if cfg.Publish != "External" {
		t.Errorf("expected publish=External, got %s", cfg.Publish)
	}
}

func TestLoadConfig_Full(t *testing.T) {
	path := getTestdataPath("cluster-full.yaml")
	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() error = %v", err)
	}

	// Check cluster info
	if cfg.Cluster.Name != "full-cluster" {
		t.Errorf("expected name=full-cluster, got %s", cfg.Cluster.Name)
	}
	if cfg.Cluster.BaseDomain != "prod.example.com" {
		t.Errorf("expected base_domain=prod.example.com, got %s", cfg.Cluster.BaseDomain)
	}

	// Check platform
	if cfg.Platform.AWS.Region != "eu-west-1" {
		t.Errorf("expected region=eu-west-1, got %s", cfg.Platform.AWS.Region)
	}
	if cfg.Platform.AWS.Tags["environment"] != "staging" {
		t.Errorf("expected tags.environment=staging, got %s", cfg.Platform.AWS.Tags["environment"])
	}
	if cfg.Platform.AWS.Tags["team"] != "platform" {
		t.Errorf("expected tags.team=platform, got %s", cfg.Platform.AWS.Tags["team"])
	}

	// Check overridden values
	if cfg.ControlPlane.InstanceType != "m6a.2xlarge" {
		t.Errorf("expected control_plane.instance_type=m6a.2xlarge, got %s", cfg.ControlPlane.InstanceType)
	}
	if cfg.ControlPlane.RootVolume.SizeGB != 200 {
		t.Errorf("expected control_plane.root_volume.size_gb=200, got %d", cfg.ControlPlane.RootVolume.SizeGB)
	}

	if cfg.Compute.Replicas != 5 {
		t.Errorf("expected compute.replicas=5, got %d", cfg.Compute.Replicas)
	}
	if cfg.Compute.InstanceType != "m5.xlarge" {
		t.Errorf("expected compute.instance_type=m5.xlarge, got %s", cfg.Compute.InstanceType)
	}
	if cfg.Compute.RootVolume.SizeGB != 300 {
		t.Errorf("expected compute.root_volume.size_gb=300, got %d", cfg.Compute.RootVolume.SizeGB)
	}

	if cfg.SSHPublicKeyFile != "/tmp/id_rsa.pub" {
		t.Errorf("expected ssh_public_key_file=/tmp/id_rsa.pub, got %s", cfg.SSHPublicKeyFile)
	}
	if cfg.ReleaseImage != "quay.io/openshift-release-dev/ocp-release:4.20.0-x86_64" {
		t.Errorf("expected release_image to match, got %s", cfg.ReleaseImage)
	}
	if cfg.FIPS != false {
		t.Errorf("expected fips=false, got %v", cfg.FIPS)
	}
	if cfg.Publish != "External" {
		t.Errorf("expected publish=External, got %s", cfg.Publish)
	}
}

func TestLoadConfig_MissingRequiredFields(t *testing.T) {
	tests := []struct {
		name        string
		yaml        string
		errContains string
	}{
		{
			name: "missing cluster name",
			yaml: `cluster:
  base_domain: example.com
  version: "4.20"
platform:
  aws:
    region: us-east-1
    credentials:
      access_key_id: "test"
      secret_access_key: "test"
pull_secret_file: /tmp/pull-secret.json
`,
			errContains: "cluster.name",
		},
		{
			name: "missing base domain",
			yaml: `cluster:
  name: test
  version: "4.20"
platform:
  aws:
    region: us-east-1
    credentials:
      access_key_id: "test"
      secret_access_key: "test"
pull_secret_file: /tmp/pull-secret.json
`,
			errContains: "cluster.base_domain",
		},
		{
			name: "missing region",
			yaml: `cluster:
  name: test
  base_domain: example.com
  version: "4.20"
platform:
  aws:
    credentials:
      access_key_id: "test"
      secret_access_key: "test"
pull_secret_file: /tmp/pull-secret.json
`,
			errContains: "platform.aws.region",
		},
		{
			name: "missing credentials",
			yaml: `cluster:
  name: test
  base_domain: example.com
  version: "4.20"
platform:
  aws:
    region: us-east-1
pull_secret_file: /tmp/pull-secret.json
`,
			errContains: "credentials",
		},
		{
			name: "missing pull_secret_file",
			yaml: `cluster:
  name: test
  base_domain: example.com
  version: "4.20"
platform:
  aws:
    region: us-east-1
    credentials:
      access_key_id: "test"
      secret_access_key: "test"
`,
			errContains: "pull_secret_file",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpfile, err := os.CreateTemp("", "test-*.yaml")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpfile.Name())

			if _, err := tmpfile.Write([]byte(tt.yaml)); err != nil {
				t.Fatal(err)
			}
			tmpfile.Close()

			_, err = LoadConfig(tmpfile.Name())
			if err == nil {
				t.Errorf("expected error containing %q, got nil", tt.errContains)
			} else if !contains(err.Error(), tt.errContains) {
				t.Errorf("expected error containing %q, got %v", tt.errContains, err)
			}
		})
	}
}

func TestLoadConfig_CredentialModes(t *testing.T) {
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "inline credentials",
			yaml: `cluster:
  name: test
  base_domain: example.com
  version: "4.20"
platform:
  aws:
    region: us-east-1
    credentials:
      access_key_id: "AKIAIOSFODNN7EXAMPLE"
      secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
pull_secret_file: /tmp/pull-secret.json
`,
		},
		{
			name: "file credentials",
			yaml: `cluster:
  name: test
  base_domain: example.com
  version: "4.20"
platform:
  aws:
    region: us-east-1
    credentials:
      credentials_file: /home/user/.aws/credentials
pull_secret_file: /tmp/pull-secret.json
`,
		},
		{
			name: "profile credentials",
			yaml: `cluster:
  name: test
  base_domain: example.com
  version: "4.20"
platform:
  aws:
    region: us-east-1
    credentials:
      profile: default
pull_secret_file: /tmp/pull-secret.json
`,
		},
		{
			name: "STS credentials",
			yaml: `cluster:
  name: test
  base_domain: example.com
  version: "4.20"
platform:
  aws:
    region: us-east-1
    credentials:
      sts_role_arn: "arn:aws:iam::123456789012:role/OCPRole"
pull_secret_file: /tmp/pull-secret.json
`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tmpfile, err := os.CreateTemp("", "test-*.yaml")
			if err != nil {
				t.Fatal(err)
			}
			defer os.Remove(tmpfile.Name())

			if _, err := tmpfile.Write([]byte(tt.yaml)); err != nil {
				t.Fatal(err)
			}
			tmpfile.Close()

			cfg, err := LoadConfig(tmpfile.Name())
			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
			if cfg == nil {
				t.Error("expected config, got nil")
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 ||
		(len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
