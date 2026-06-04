package ocp

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const defaultProvisionSTSDuration = 2 * time.Hour

// SSOCredentialProvider exchanges caller OIDC tokens for temporary AWS
// credentials via AssumeRoleWithWebIdentity. The pull secret is provided
// externally (by the UI, CLI, or test harness that performed the Red Hat
// SSO authentication) — the agent never contacts Red Hat directly.
type SSOCredentialProvider struct {
	STSDuration time.Duration // override default STS session duration (0 = use default)
	PullSecret  []byte        // OpenShift pull secret JSON, provided by caller
}

// ResolveAWS exchanges the caller's OIDC token for temporary AWS credentials
// via STS AssumeRoleWithWebIdentity.
func (p *SSOCredentialProvider) ResolveAWS(ctx context.Context, req AWSCredentialRequest) (*AWSCredentials, error) {
	if req.Auth.Token == "" {
		return nil, fmt.Errorf("auth token is required")
	}
	if req.RoleARN == "" {
		return nil, fmt.Errorf("role ARN is required")
	}

	duration := p.STSDuration
	if duration == 0 {
		duration = defaultProvisionSTSDuration
	}

	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(req.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	stsClient := sts.NewFromConfig(cfg)

	result, err := stsClient.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityInput{
		RoleArn:          aws.String(req.RoleARN),
		RoleSessionName:  aws.String(fmt.Sprintf("fleetshift-provision-%d", time.Now().Unix())),
		WebIdentityToken: aws.String(string(req.Auth.Token)),
		DurationSeconds:  aws.Int32(int32(duration.Seconds())),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to assume role with web identity: %w", err)
	}

	if result.Credentials == nil {
		return nil, fmt.Errorf("STS returned nil credentials")
	}

	return &AWSCredentials{
		AccessKeyID:     aws.ToString(result.Credentials.AccessKeyId),
		SecretAccessKey: aws.ToString(result.Credentials.SecretAccessKey),
		SessionToken:    aws.ToString(result.Credentials.SessionToken),
	}, nil
}

// ResolvePullSecret returns the pre-provided pull secret. The Red Hat SSO
// authentication to acquire the pull secret happens outside the agent —
// in the UI, CLI, or test harness.
func (p *SSOCredentialProvider) ResolvePullSecret(ctx context.Context, req PullSecretRequest) ([]byte, error) {
	if len(p.PullSecret) == 0 {
		return nil, fmt.Errorf("pull secret not configured")
	}
	return p.PullSecret, nil
}
