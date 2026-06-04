package ocp

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestSSOProvider_ResolveAWS_MissingToken(t *testing.T) {
	provider := &SSOCredentialProvider{}
	req := AWSCredentialRequest{
		Region:  "us-east-1",
		RoleARN: "arn:aws:iam::123456789012:role/test-role",
		Auth:    domain.DeliveryAuth{Token: ""},
	}
	_, err := provider.ResolveAWS(context.Background(), req)
	if err == nil {
		t.Fatal("expected error when token is empty, got nil")
	}
	if err.Error() != "auth token is required" {
		t.Errorf("error = %q, want %q", err.Error(), "auth token is required")
	}
}

func TestSSOProvider_ResolveAWS_MissingRoleARN(t *testing.T) {
	provider := &SSOCredentialProvider{}
	req := AWSCredentialRequest{
		Region:  "us-east-1",
		RoleARN: "",
		Auth:    domain.DeliveryAuth{Token: "test-token"},
	}
	_, err := provider.ResolveAWS(context.Background(), req)
	if err == nil {
		t.Fatal("expected error when role ARN is empty, got nil")
	}
	if err.Error() != "role ARN is required" {
		t.Errorf("error = %q, want %q", err.Error(), "role ARN is required")
	}
}

func TestSSOProvider_ResolvePullSecret(t *testing.T) {
	pullSecret := []byte(`{"auths":{"registry.redhat.io":{"auth":"dGVzdA=="}}}`)
	provider := &SSOCredentialProvider{PullSecret: pullSecret}
	got, err := provider.ResolvePullSecret(context.Background(), PullSecretRequest{
		Auth: domain.DeliveryAuth{Token: "any-token"},
	})
	if err != nil {
		t.Fatalf("ResolvePullSecret: %v", err)
	}
	if string(got) != string(pullSecret) {
		t.Errorf("got %q, want %q", got, pullSecret)
	}
}

func TestSSOProvider_ResolvePullSecret_NotConfigured(t *testing.T) {
	provider := &SSOCredentialProvider{}
	_, err := provider.ResolvePullSecret(context.Background(), PullSecretRequest{
		Auth: domain.DeliveryAuth{Token: "token"},
	})
	if err == nil {
		t.Fatal("expected error when pull secret not configured")
	}
	if err.Error() != "pull secret not configured" {
		t.Errorf("error = %q, want %q", err.Error(), "pull secret not configured")
	}
}
