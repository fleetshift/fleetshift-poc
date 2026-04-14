package ocp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

const (
	defaultProvisionSTSDuration = 2 * time.Hour
	defaultDestroySTSDuration   = 1 * time.Hour
	rhPullSecretEndpoint        = "https://api.openshift.com/api/accounts_mgmt/v1/access_token"
)

// SSOCredentialProvider exchanges caller OIDC tokens for temporary AWS
// credentials via AssumeRoleWithWebIdentity and acquires pull secrets via
// Red Hat SSO.
type SSOCredentialProvider struct {
	STSDuration time.Duration // override default STS session duration (0 = use default)
	HTTPClient  *http.Client  // override for testing
}

// httpClient returns the configured HTTP client, or a default with a 30s timeout.
func (p *SSOCredentialProvider) httpClient() *http.Client {
	if p.HTTPClient != nil {
		return p.HTTPClient
	}
	return &http.Client{Timeout: 30 * time.Second}
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

	// Determine session duration
	duration := p.STSDuration
	if duration == 0 {
		duration = defaultProvisionSTSDuration
	}

	// Load AWS config for the specified region
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(req.Region))
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	// Create STS client
	stsClient := sts.NewFromConfig(cfg)

	// Assume role with web identity
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

// ResolvePullSecret acquires an OpenShift pull secret from Red Hat API
// using a bearer token.
func (p *SSOCredentialProvider) ResolvePullSecret(ctx context.Context, req PullSecretRequest) ([]byte, error) {
	if req.Auth.Token == "" {
		return nil, fmt.Errorf("auth token is required")
	}

	client := p.httpClient()

	// Create POST request with empty JSON body
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, rhPullSecretEndpoint, strings.NewReader("{}"))
	if err != nil {
		return nil, fmt.Errorf("failed to create pull secret request: %w", err)
	}

	httpReq.Header.Set("Authorization", fmt.Sprintf("Bearer %s", req.Auth.Token))
	httpReq.Header.Set("Content-Type", "application/json")

	// Execute request
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch pull secret: %w", err)
	}
	defer resp.Body.Close()

	// Read response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read pull secret response: %w", err)
	}

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("pull secret request failed with status %d: %s", resp.StatusCode, string(body))
	}

	// Validate response has auths field
	var pullSecret map[string]interface{}
	if err := json.Unmarshal(body, &pullSecret); err != nil {
		return nil, fmt.Errorf("failed to parse pull secret response: %w", err)
	}

	if _, ok := pullSecret["auths"]; !ok {
		return nil, fmt.Errorf("pull secret response missing 'auths' field")
	}

	return body, nil
}
