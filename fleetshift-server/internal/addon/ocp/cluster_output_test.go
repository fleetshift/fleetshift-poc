package ocp

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kubernetes"
)

func TestClusterOutput_Target(t *testing.T) {
	output := ClusterOutput{
		TargetID:      "target-123",
		Name:          "test-cluster",
		APIServer:     "https://api.test.example.com:6443",
		CACert:        []byte("-----BEGIN CERTIFICATE-----\ntest-ca\n-----END CERTIFICATE-----"),
		SATokenRef:    "vault/sa-token-ref",
		InfraID:       "test-infra-id",
		ClusterID:     "test-cluster-uuid",
		Region:        "us-east-1",
	}

	target := output.Target()

	if target.ID != "target-123" {
		t.Errorf("target.ID = %q; want %q", target.ID, "target-123")
	}
	if target.Type != KubernetesTargetType {
		t.Errorf("target.Type = %q; want %q", target.Type, KubernetesTargetType)
	}
	if target.Name != "test-cluster" {
		t.Errorf("target.Name = %q; want %q", target.Name, "test-cluster")
	}

	// Verify properties
	if target.Properties["api_server"] != "https://api.test.example.com:6443" {
		t.Errorf("properties[api_server] = %q; want %q",
			target.Properties["api_server"], "https://api.test.example.com:6443")
	}
	if target.Properties["ca_cert"] != string(output.CACert) {
		t.Errorf("properties[ca_cert] missing or incorrect")
	}
	if target.Properties["service_account_token_ref"] != "vault/sa-token-ref" {
		t.Errorf("properties[service_account_token_ref] = %q; want %q",
			target.Properties["service_account_token_ref"], "vault/sa-token-ref")
	}
	if target.Properties["infra_id"] != "test-infra-id" {
		t.Errorf("properties[infra_id] = %q; want %q",
			target.Properties["infra_id"], "test-infra-id")
	}
	if target.Properties["cluster_id"] != "test-cluster-uuid" {
		t.Errorf("properties[cluster_id] = %q; want %q",
			target.Properties["cluster_id"], "test-cluster-uuid")
	}
	if target.Properties["region"] != "us-east-1" {
		t.Errorf("properties[region] = %q; want %q",
			target.Properties["region"], "us-east-1")
	}

	// Verify AcceptedResourceTypes
	if len(target.AcceptedResourceTypes) != 1 {
		t.Fatalf("len(AcceptedResourceTypes) = %d; want 1", len(target.AcceptedResourceTypes))
	}
	if target.AcceptedResourceTypes[0] != kubernetes.ManifestResourceType {
		t.Errorf("AcceptedResourceTypes[0] = %q; want %q",
			target.AcceptedResourceTypes[0], kubernetes.ManifestResourceType)
	}
}

func TestClusterOutput_Secrets(t *testing.T) {
	output := ClusterOutput{
		SATokenRef:    "vault/sa-token",
		SAToken:       []byte("sa-token-value"),
		KubeconfigRef: "vault/kubeconfig",
		Kubeconfig:    []byte("kubeconfig-yaml"),
		SSHKeyRef:     "vault/ssh-key",
		SSHPrivateKey: []byte("-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----"),
	}

	secrets := output.Secrets()

	if len(secrets) != 3 {
		t.Fatalf("len(secrets) = %d; want 3", len(secrets))
	}

	// Verify SA token secret
	if secrets[0].Ref != "vault/sa-token" {
		t.Errorf("secrets[0].Ref = %q; want %q", secrets[0].Ref, "vault/sa-token")
	}
	if string(secrets[0].Value) != "sa-token-value" {
		t.Errorf("secrets[0].Value = %q; want %q", string(secrets[0].Value), "sa-token-value")
	}

	// Verify kubeconfig secret
	if secrets[1].Ref != "vault/kubeconfig" {
		t.Errorf("secrets[1].Ref = %q; want %q", secrets[1].Ref, "vault/kubeconfig")
	}
	if string(secrets[1].Value) != "kubeconfig-yaml" {
		t.Errorf("secrets[1].Value = %q; want %q", string(secrets[1].Value), "kubeconfig-yaml")
	}

	// Verify SSH key secret
	if secrets[2].Ref != "vault/ssh-key" {
		t.Errorf("secrets[2].Ref = %q; want %q", secrets[2].Ref, "vault/ssh-key")
	}
	if string(secrets[2].Value) != "-----BEGIN RSA PRIVATE KEY-----\ntest\n-----END RSA PRIVATE KEY-----" {
		t.Errorf("secrets[2].Value incorrect")
	}
}

func TestClusterOutput_Secrets_Empty(t *testing.T) {
	tests := []struct {
		name   string
		output ClusterOutput
	}{
		{
			name:   "no refs set",
			output: ClusterOutput{},
		},
		{
			name: "only values set without refs",
			output: ClusterOutput{
				SAToken:       []byte("token"),
				Kubeconfig:    []byte("config"),
				SSHPrivateKey: []byte("key"),
			},
		},
		{
			name: "partial - only SA token",
			output: ClusterOutput{
				SATokenRef: "vault/sa-token",
				SAToken:    []byte("token"),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			secrets := tt.output.Secrets()
			if tt.name == "partial - only SA token" {
				if len(secrets) != 1 {
					t.Errorf("len(secrets) = %d; want 1", len(secrets))
				}
				return
			}
			if secrets != nil {
				t.Errorf("secrets = %v; want nil", secrets)
			}
		})
	}
}
