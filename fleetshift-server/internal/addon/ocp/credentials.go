package ocp

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"golang.org/x/crypto/ssh"
)

// AWSCredentials holds AWS credentials for cluster provisioning.
type AWSCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

// Env returns credentials as environment variable strings suitable for
// subprocess execution.
func (c *AWSCredentials) Env() []string {
	env := []string{
		"AWS_ACCESS_KEY_ID=" + c.AccessKeyID,
		"AWS_SECRET_ACCESS_KEY=" + c.SecretAccessKey,
	}
	if c.SessionToken != "" {
		env = append(env, "AWS_SESSION_TOKEN="+c.SessionToken)
	}
	return env
}

// AWSCredentialRequest requests AWS credentials for a specific region and role.
type AWSCredentialRequest struct {
	Region  string
	RoleARN string
	Auth    domain.DeliveryAuth
}

// PullSecretRequest requests an OpenShift pull secret.
type PullSecretRequest struct {
	Auth domain.DeliveryAuth
}

// CredentialProvider resolves credentials needed for OCP provisioning.
// Implementations may fetch credentials from external sources (SSO, vault)
// or return caller-provided credentials directly.
type CredentialProvider interface {
	// ResolveAWS returns AWS credentials for cluster provisioning.
	ResolveAWS(ctx context.Context, req AWSCredentialRequest) (*AWSCredentials, error)

	// ResolvePullSecret returns the OpenShift pull secret JSON.
	ResolvePullSecret(ctx context.Context, req PullSecretRequest) ([]byte, error)
}

// PassthroughCredentialProvider returns caller-provided credentials directly
// without any transformation or external lookups. Used for testing and
// environments where credentials are pre-configured.
type PassthroughCredentialProvider struct {
	AWSAccessKeyID     string
	AWSSecretAccessKey string
	AWSSessionToken    string
	PullSecret         []byte
}

// ResolveAWS returns the pre-configured AWS credentials.
func (p *PassthroughCredentialProvider) ResolveAWS(ctx context.Context, req AWSCredentialRequest) (*AWSCredentials, error) {
	return &AWSCredentials{
		AccessKeyID:     p.AWSAccessKeyID,
		SecretAccessKey: p.AWSSecretAccessKey,
		SessionToken:    p.AWSSessionToken,
	}, nil
}

// ResolvePullSecret returns the pre-configured pull secret.
func (p *PassthroughCredentialProvider) ResolvePullSecret(ctx context.Context, req PullSecretRequest) ([]byte, error) {
	if len(p.PullSecret) == 0 {
		return nil, fmt.Errorf("pull secret not configured in passthrough provider")
	}
	return p.PullSecret, nil
}

// GenerateSSHKey generates an ED25519 SSH key pair for node access.
// Returns the public key in OpenSSH authorized_keys format and the private
// key in PEM format.
func GenerateSSHKey() (publicKey []byte, privateKey []byte, error error) {
	// Generate ED25519 key pair
	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate ed25519 key: %w", err)
	}

	// Convert public key to SSH authorized_keys format
	sshPubKey, err := ssh.NewPublicKey(pubKey)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert public key: %w", err)
	}
	publicKeyBytes := ssh.MarshalAuthorizedKey(sshPubKey)

	// Convert private key to PEM format
	pemBlock, err := ssh.MarshalPrivateKey(privKey, "")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal private key: %w", err)
	}

	return publicKeyBytes, pem.EncodeToMemory(pemBlock), nil
}
