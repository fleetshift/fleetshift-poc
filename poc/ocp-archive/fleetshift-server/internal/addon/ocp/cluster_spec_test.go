package ocp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"gopkg.in/yaml.v3"
)

func TestParseClusterSpec(t *testing.T) {
	manifests := []domain.Manifest{
		{
			ResourceType: "other.resource",
			Raw:          json.RawMessage(`{"foo": "bar"}`),
		},
		{
			ResourceType: ClusterResourceType,
			Raw: json.RawMessage(`{
				"name": "test-cluster",
				"base_domain": "example.com",
				"region": "us-east-1",
				"role_arn": "arn:aws:iam::123456789012:role/test",
				"release_image": "quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64"
			}`),
		},
	}

	spec, err := ParseClusterSpec(manifests)
	if err != nil {
		t.Fatalf("ParseClusterSpec failed: %v", err)
	}

	if spec.Name != "test-cluster" {
		t.Errorf("expected Name=test-cluster, got %s", spec.Name)
	}
	if spec.BaseDomain != "example.com" {
		t.Errorf("expected BaseDomain=example.com, got %s", spec.BaseDomain)
	}
	if spec.Region != "us-east-1" {
		t.Errorf("expected Region=us-east-1, got %s", spec.Region)
	}
	if spec.RoleARN != "arn:aws:iam::123456789012:role/test" {
		t.Errorf("expected RoleARN=arn:aws:iam::123456789012:role/test, got %s", spec.RoleARN)
	}
	if spec.ReleaseImage != "quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64" {
		t.Errorf("unexpected ReleaseImage: %s", spec.ReleaseImage)
	}
}

func TestParseClusterSpec_Errors(t *testing.T) {
	tests := []struct {
		name string
		raw  string
	}{
		{"no OCP manifest", ""},
		{"missing name", `{"base_domain":"d","region":"us-east-1","role_arn":"arn:aws:iam::123:role/r"}`},
		{"missing base_domain", `{"name":"c","region":"us-east-1","role_arn":"arn:aws:iam::123:role/r"}`},
		{"missing region", `{"name":"c","base_domain":"d","role_arn":"arn:aws:iam::123:role/r"}`},
		{"missing role_arn", `{"name":"c","base_domain":"d","region":"us-east-1"}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var manifests []domain.Manifest
			if tt.raw == "" {
				manifests = []domain.Manifest{{ResourceType: "other.resource", Raw: json.RawMessage(`{}`)}}
			} else {
				manifests = []domain.Manifest{{ResourceType: ClusterResourceType, Raw: json.RawMessage(tt.raw)}}
			}
			_, err := ParseClusterSpec(manifests)
			if err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestBuildClusterYAML(t *testing.T) {
	spec := &ClusterSpec{
		Name:         "test-cluster",
		BaseDomain:   "example.com",
		Region:       "us-east-1",
		RoleARN:      "arn:aws:iam::123:role/r",
		ReleaseImage: "quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64",
	}

	yamlBytes, err := BuildClusterYAML(spec, "us-east-1", "/path/to/pull-secret.json", "ssh-ed25519 AAAAC3...")
	if err != nil {
		t.Fatalf("BuildClusterYAML failed: %v", err)
	}

	// Parse the generated YAML to verify structure
	var result map[string]any
	if err := yaml.Unmarshal(yamlBytes, &result); err != nil {
		t.Fatalf("generated YAML is invalid: %v", err)
	}

	// Verify ocp_engine section
	ocpEngine, ok := result["ocp_engine"].(map[string]any)
	if !ok {
		t.Fatal("missing or invalid ocp_engine section")
	}
	if ocpEngine["pull_secret_file"] != "/path/to/pull-secret.json" {
		t.Errorf("unexpected pull_secret_file: %v", ocpEngine["pull_secret_file"])
	}
	if ocpEngine["release_image"] != "quay.io/openshift-release-dev/ocp-release:4.14.0-x86_64" {
		t.Errorf("unexpected release_image: %v", ocpEngine["release_image"])
	}

	// Verify baseDomain
	if result["baseDomain"] != "example.com" {
		t.Errorf("unexpected baseDomain: %v", result["baseDomain"])
	}

	// Verify credentialsMode
	if result["credentialsMode"] != "Manual" {
		t.Errorf("unexpected credentialsMode: %v", result["credentialsMode"])
	}

	// Verify metadata
	metadata, ok := result["metadata"].(map[string]any)
	if !ok {
		t.Fatal("missing or invalid metadata section")
	}
	if metadata["name"] != "test-cluster" {
		t.Errorf("unexpected metadata.name: %v", metadata["name"])
	}

	// Verify platform.aws.region
	platform, ok := result["platform"].(map[string]any)
	if !ok {
		t.Fatal("missing or invalid platform section")
	}
	aws, ok := platform["aws"].(map[string]any)
	if !ok {
		t.Fatal("missing or invalid platform.aws section")
	}
	if aws["region"] != "us-east-1" {
		t.Errorf("unexpected platform.aws.region: %v", aws["region"])
	}

	// Verify sshKey
	if result["sshKey"] != "ssh-ed25519 AAAAC3..." {
		t.Errorf("unexpected sshKey: %v", result["sshKey"])
	}
}

func TestBuildClusterYAML_WithInstallConfig(t *testing.T) {
	spec := &ClusterSpec{
		Name:       "test-cluster",
		BaseDomain: "example.com",
		Region:     "us-east-1",
		RoleARN:    "arn:aws:iam::123:role/r",
		InstallConfig: map[string]any{
			"controlPlane": map[string]any{
				"architecture": "amd64",
				"replicas":     3,
			},
			"compute": []any{
				map[string]any{
					"architecture": "amd64",
					"replicas":     3,
				},
			},
			"networking": map[string]any{
				"networkType": "OVNKubernetes",
			},
		},
	}

	yamlBytes, err := BuildClusterYAML(spec, "us-west-2", "/pull-secret.json", "ssh-ed25519 KEY")
	if err != nil {
		t.Fatalf("BuildClusterYAML failed: %v", err)
	}

	// Parse the generated YAML
	var result map[string]any
	if err := yaml.Unmarshal(yamlBytes, &result); err != nil {
		t.Fatalf("generated YAML is invalid: %v", err)
	}

	// Verify pass-through fields are preserved
	controlPlane, ok := result["controlPlane"].(map[string]any)
	if !ok {
		t.Fatal("missing or invalid controlPlane section")
	}
	if controlPlane["architecture"] != "amd64" {
		t.Errorf("unexpected controlPlane.architecture: %v", controlPlane["architecture"])
	}
	if controlPlane["replicas"] != 3 {
		t.Errorf("unexpected controlPlane.replicas: %v", controlPlane["replicas"])
	}

	// Verify compute
	compute, ok := result["compute"].([]any)
	if !ok {
		t.Fatal("missing or invalid compute section")
	}
	if len(compute) != 1 {
		t.Fatalf("expected 1 compute entry, got %d", len(compute))
	}

	// Verify networking
	networking, ok := result["networking"].(map[string]any)
	if !ok {
		t.Fatal("missing or invalid networking section")
	}
	if networking["networkType"] != "OVNKubernetes" {
		t.Errorf("unexpected networking.networkType: %v", networking["networkType"])
	}

	// Verify that our required fields are still present
	if result["baseDomain"] != "example.com" {
		t.Errorf("baseDomain was overwritten")
	}
	if result["credentialsMode"] != "Manual" {
		t.Errorf("credentialsMode was overwritten")
	}
}

func TestBuildClusterYAML_BaseKeysCannotBeOverridden(t *testing.T) {
	spec := &ClusterSpec{
		Name:       "real-cluster",
		BaseDomain: "real.example.com",
		Region:     "us-east-1",
		RoleARN:    "arn:aws:iam::123:role/r",
		InstallConfig: map[string]any{
			"baseDomain":      "evil.example.com",
			"credentialsMode": "Passthrough",
			"metadata":        map[string]any{"name": "evil-cluster"},
			"ocp_engine":      map[string]any{"pull_secret_file": "/etc/shadow"},
			"sshKey":          "ssh-rsa EVIL",
		},
	}

	yamlBytes, err := BuildClusterYAML(spec, "us-east-1", "/safe/pull-secret.json", "ssh-ed25519 SAFE")
	if err != nil {
		t.Fatalf("BuildClusterYAML failed: %v", err)
	}

	var result map[string]any
	if err := yaml.Unmarshal(yamlBytes, &result); err != nil {
		t.Fatalf("generated YAML is invalid: %v", err)
	}

	if result["baseDomain"] != "real.example.com" {
		t.Errorf("baseDomain was overridden: got %v", result["baseDomain"])
	}
	if result["credentialsMode"] != "Manual" {
		t.Errorf("credentialsMode was overridden: got %v", result["credentialsMode"])
	}
	if result["sshKey"] != "ssh-ed25519 SAFE" {
		t.Errorf("sshKey was overridden: got %v", result["sshKey"])
	}

	metadata := result["metadata"].(map[string]any)
	if metadata["name"] != "real-cluster" {
		t.Errorf("metadata.name was overridden: got %v", metadata["name"])
	}

	ocpEngine := result["ocp_engine"].(map[string]any)
	if ocpEngine["pull_secret_file"] != "/safe/pull-secret.json" {
		t.Errorf("ocp_engine.pull_secret_file was overridden: got %v", ocpEngine["pull_secret_file"])
	}
}

func TestBuildClusterYAML_PlatformDeepMerge(t *testing.T) {
	spec := &ClusterSpec{
		Name:       "test-cluster",
		BaseDomain: "example.com",
		Region:     "us-east-1",
		RoleARN:    "arn:aws:iam::123:role/r",
		InstallConfig: map[string]any{
			"platform": map[string]any{
				"aws": map[string]any{
					"region": "should-be-ignored",
					"subnets": []any{
						"subnet-aaa",
						"subnet-bbb",
					},
				},
			},
		},
	}

	yamlBytes, err := BuildClusterYAML(spec, "us-west-2", "/pull-secret.json", "ssh-ed25519 KEY")
	if err != nil {
		t.Fatalf("BuildClusterYAML failed: %v", err)
	}

	var result map[string]any
	if err := yaml.Unmarshal(yamlBytes, &result); err != nil {
		t.Fatalf("generated YAML is invalid: %v", err)
	}

	platform := result["platform"].(map[string]any)
	aws := platform["aws"].(map[string]any)

	// region must be the authoritative value, not the user-provided one
	if aws["region"] != "us-west-2" {
		t.Errorf("platform.aws.region was overridden: got %v", aws["region"])
	}

	// user-provided subnets should be preserved
	subnets, ok := aws["subnets"].([]any)
	if !ok || len(subnets) != 2 {
		t.Fatalf("expected 2 subnets, got %v", aws["subnets"])
	}
	if subnets[0] != "subnet-aaa" || subnets[1] != "subnet-bbb" {
		t.Errorf("unexpected subnets: %v", subnets)
	}
}

func TestParseClusterSpec_CCOSTSModeExplicitFalse(t *testing.T) {
	manifests := []domain.Manifest{
		{
			ResourceType: ClusterResourceType,
			Raw: json.RawMessage(`{
				"name": "test-cluster",
				"base_domain": "example.com",
				"region": "us-east-1",
				"role_arn": "arn:aws:iam::123456789012:role/test",
				"cco_sts_mode": false
			}`),
		},
	}

	spec, err := ParseClusterSpec(manifests)
	if err != nil {
		t.Fatalf("ParseClusterSpec failed: %v", err)
	}

	if spec.EffectiveCCOSTSMode() != false {
		t.Errorf("expected EffectiveCCOSTSMode()=false, got true")
	}
}

func TestParseClusterSpec_CCOSTSModeDefaultTrue(t *testing.T) {
	manifests := []domain.Manifest{
		{
			ResourceType: ClusterResourceType,
			Raw: json.RawMessage(`{
				"name": "test-cluster",
				"base_domain": "example.com",
				"region": "us-east-1",
				"role_arn": "arn:aws:iam::123456789012:role/test"
			}`),
		},
	}

	spec, err := ParseClusterSpec(manifests)
	if err != nil {
		t.Fatalf("ParseClusterSpec failed: %v", err)
	}

	if spec.EffectiveCCOSTSMode() != true {
		t.Errorf("expected EffectiveCCOSTSMode()=true (default), got false")
	}
}

func TestBuildClusterYAML_STSMode(t *testing.T) {
	stsTrue := true
	spec := &ClusterSpec{
		Name:       "test-cluster",
		BaseDomain: "example.com",
		Region:     "us-east-1",
		RoleARN:    "arn:aws:iam::123:role/r",
		CCOSTSMode: &stsTrue,
	}

	yamlBytes, err := BuildClusterYAML(spec, "us-east-1", "/path/to/pull-secret.json", "ssh-ed25519 AAAAC3...")
	if err != nil {
		t.Fatalf("BuildClusterYAML failed: %v", err)
	}

	yamlStr := string(yamlBytes)

	// Verify output contains cco_sts_mode: true
	if !strings.Contains(yamlStr, "cco_sts_mode: true") {
		t.Errorf("expected YAML to contain 'cco_sts_mode: true', got:\n%s", yamlStr)
	}

	// Verify output contains credentialsMode: Manual
	if !strings.Contains(yamlStr, "credentialsMode: Manual") {
		t.Errorf("expected YAML to contain 'credentialsMode: Manual', got:\n%s", yamlStr)
	}
}

func TestBuildClusterYAML_MintMode(t *testing.T) {
	stsFalse := false
	spec := &ClusterSpec{
		Name:       "test-cluster",
		BaseDomain: "example.com",
		Region:     "us-east-1",
		RoleARN:    "arn:aws:iam::123:role/r",
		CCOSTSMode: &stsFalse,
	}

	yamlBytes, err := BuildClusterYAML(spec, "us-east-1", "/path/to/pull-secret.json", "ssh-ed25519 AAAAC3...")
	if err != nil {
		t.Fatalf("BuildClusterYAML failed: %v", err)
	}

	yamlStr := string(yamlBytes)

	// Verify output does NOT contain cco_sts_mode
	if strings.Contains(yamlStr, "cco_sts_mode") {
		t.Errorf("expected YAML to NOT contain 'cco_sts_mode', got:\n%s", yamlStr)
	}

	// Verify output does NOT contain credentialsMode
	if strings.Contains(yamlStr, "credentialsMode") {
		t.Errorf("expected YAML to NOT contain 'credentialsMode', got:\n%s", yamlStr)
	}
}
