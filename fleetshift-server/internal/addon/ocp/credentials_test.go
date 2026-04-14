package ocp

import (
	"context"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestPassthroughProvider_ResolveAWS(t *testing.T) {
	provider := &PassthroughCredentialProvider{
		AWSAccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		AWSSecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		AWSSessionToken:    "FwoGZXIvYXdzEBEaDHExampleSessionToken",
	}

	req := AWSCredentialRequest{
		Region:  "us-east-1",
		RoleARN: "arn:aws:iam::123456789012:role/OCPInstaller",
		Auth: domain.DeliveryAuth{
			Token: "fake-token",
		},
	}

	creds, err := provider.ResolveAWS(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolveAWS failed: %v", err)
	}

	if creds.AccessKeyID != provider.AWSAccessKeyID {
		t.Errorf("AccessKeyID = %q, want %q", creds.AccessKeyID, provider.AWSAccessKeyID)
	}
	if creds.SecretAccessKey != provider.AWSSecretAccessKey {
		t.Errorf("SecretAccessKey = %q, want %q", creds.SecretAccessKey, provider.AWSSecretAccessKey)
	}
	if creds.SessionToken != provider.AWSSessionToken {
		t.Errorf("SessionToken = %q, want %q", creds.SessionToken, provider.AWSSessionToken)
	}
}

func TestAWSCredentials_Env(t *testing.T) {
	creds := &AWSCredentials{
		AccessKeyID:     "AKIAIOSFODNN7EXAMPLE",
		SecretAccessKey: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		SessionToken:    "FwoGZXIvYXdzEBEaDHExampleSessionToken",
	}

	env := creds.Env()

	want := []string{
		"AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE",
		"AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
		"AWS_SESSION_TOKEN=FwoGZXIvYXdzEBEaDHExampleSessionToken",
	}

	if len(env) != len(want) {
		t.Fatalf("Env() returned %d vars, want %d", len(env), len(want))
	}

	for i, wantVar := range want {
		if env[i] != wantVar {
			t.Errorf("Env()[%d] = %q, want %q", i, env[i], wantVar)
		}
	}
}

func TestPassthroughProvider_ResolvePullSecret(t *testing.T) {
	pullSecret := []byte(`{"auths":{"cloud.openshift.com":{"auth":"secret"}}}`)
	provider := &PassthroughCredentialProvider{
		PullSecret: pullSecret,
	}

	req := PullSecretRequest{
		Auth: domain.DeliveryAuth{
			Token: "fake-token",
		},
	}

	secret, err := provider.ResolvePullSecret(context.Background(), req)
	if err != nil {
		t.Fatalf("ResolvePullSecret failed: %v", err)
	}

	if string(secret) != string(pullSecret) {
		t.Errorf("ResolvePullSecret = %q, want %q", secret, pullSecret)
	}
}

func TestPassthroughProvider_ResolvePullSecret_Missing(t *testing.T) {
	provider := &PassthroughCredentialProvider{
		// No PullSecret set
	}

	req := PullSecretRequest{
		Auth: domain.DeliveryAuth{
			Token: "fake-token",
		},
	}

	_, err := provider.ResolvePullSecret(context.Background(), req)
	if err == nil {
		t.Fatal("ResolvePullSecret should fail when pull secret is missing")
	}

	if !strings.Contains(err.Error(), "pull secret") {
		t.Errorf("error message should mention pull secret, got: %v", err)
	}
}

func TestGenerateSSHKey(t *testing.T) {
	publicKey, privateKey, err := GenerateSSHKey()
	if err != nil {
		t.Fatalf("GenerateSSHKey failed: %v", err)
	}

	if len(publicKey) == 0 {
		t.Error("public key is empty")
	}
	if len(privateKey) == 0 {
		t.Error("private key is empty")
	}

	// Public key should be in OpenSSH authorized_keys format
	pubKeyStr := string(publicKey)
	if !strings.HasPrefix(pubKeyStr, "ssh-ed25519 ") {
		t.Errorf("public key should start with 'ssh-ed25519 ', got: %s", pubKeyStr[:min(20, len(pubKeyStr))])
	}

	// Private key should be in PEM format
	privKeyStr := string(privateKey)
	if !strings.Contains(privKeyStr, "-----BEGIN OPENSSH PRIVATE KEY-----") {
		t.Error("private key should be in PEM/OpenSSH format")
	}
	if !strings.Contains(privKeyStr, "-----END OPENSSH PRIVATE KEY-----") {
		t.Error("private key should be in PEM/OpenSSH format")
	}
}

