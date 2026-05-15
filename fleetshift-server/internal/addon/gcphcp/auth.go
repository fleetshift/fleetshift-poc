package gcphcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// BrokerAuthConfig holds the configuration for workforce identity federation
// and broker service account token generation.
type BrokerAuthConfig struct {
	// WorkforcePool is the GCP Workforce Identity Pool ID.
	WorkforcePool string

	// WorkforceProvider is the GCP Workforce Identity Provider ID.
	WorkforceProvider string

	// GCPProject is the GCP project containing the broker service account.
	GCPProject string

	// BrokerSAEmail is the email of the broker service account.
	BrokerSAEmail string

	// GatewayAudience is the audience for the broker ID token (CLS gateway client ID).
	GatewayAudience string

	// STSEndpoint is the Google STS token exchange endpoint.
	// Defaults to "https://sts.googleapis.com/v1/token".
	STSEndpoint string

	// IAMEndpoint is the Google IAM credentials endpoint.
	// Defaults to "https://iamcredentials.googleapis.com".
	IAMEndpoint string

	// HTTPClient is the HTTP client to use for requests.
	// Defaults to http.DefaultClient.
	HTTPClient *http.Client
}

// BrokerAuthResult contains the results of a successful auth exchange.
type BrokerAuthResult struct {
	// BrokerToken is the Google-signed ID token for the CLS gateway.
	BrokerToken string

	// BrokerEmail is the broker service account email (for X-User-Email header).
	BrokerEmail string

	// WorkforceToken is the Workforce access token (for hypershift credential files).
	WorkforceToken string
}

// BrokerAuth performs workforce identity federation and broker token generation.
type BrokerAuth struct {
	cfg BrokerAuthConfig
}

// NewBrokerAuth creates a new BrokerAuth instance with the given configuration.
func NewBrokerAuth(cfg BrokerAuthConfig) *BrokerAuth {
	// Set defaults
	if cfg.STSEndpoint == "" {
		cfg.STSEndpoint = "https://sts.googleapis.com/v1/token"
	}
	if cfg.IAMEndpoint == "" {
		cfg.IAMEndpoint = "https://iamcredentials.googleapis.com"
	}
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}

	return &BrokerAuth{cfg: cfg}
}

// Exchange performs a two-step auth flow:
// 1. Exchange the caller's OIDC token for a Workforce access token via STS.
// 2. Use the Workforce token to mint a broker ID token via IAM generateIdToken.
func (a *BrokerAuth) Exchange(ctx context.Context, callerToken string) (BrokerAuthResult, error) {
	// Step 1: STS token exchange
	workforceToken, err := a.exchangeSTS(ctx, callerToken)
	if err != nil {
		return BrokerAuthResult{}, fmt.Errorf("STS token exchange failed: %w", err)
	}

	// Step 2: Generate broker ID token
	brokerToken, err := a.generateIDToken(ctx, workforceToken)
	if err != nil {
		return BrokerAuthResult{}, fmt.Errorf("broker ID token generation failed: %w", err)
	}

	return BrokerAuthResult{
		BrokerToken:    brokerToken,
		BrokerEmail:    a.cfg.BrokerSAEmail,
		WorkforceToken: workforceToken,
	}, nil
}

// exchangeSTS exchanges the caller's OIDC token for a Workforce access token.
func (a *BrokerAuth) exchangeSTS(ctx context.Context, callerToken string) (string, error) {
	audience := fmt.Sprintf("//iam.googleapis.com/locations/global/workforcePools/%s/providers/%s",
		a.cfg.WorkforcePool, a.cfg.WorkforceProvider)

	formData := url.Values{
		"grant_type":           {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"audience":             {audience},
		"requested_token_type": {"urn:ietf:params:oauth:token-type:access_token"},
		"scope":                {"https://www.googleapis.com/auth/cloud-platform"},
		"subject_token_type":   {"urn:ietf:params:oauth:token-type:jwt"},
		"subject_token":        {callerToken},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.cfg.STSEndpoint, bytes.NewBufferString(formData.Encode()))
	if err != nil {
		return "", fmt.Errorf("failed to create STS request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.cfg.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("STS request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read STS response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("STS returned status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("failed to parse STS response: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return "", fmt.Errorf("STS response missing access_token")
	}

	return tokenResp.AccessToken, nil
}

// generateIDToken uses the Workforce token to generate a broker ID token.
func (a *BrokerAuth) generateIDToken(ctx context.Context, workforceToken string) (string, error) {
	endpoint := fmt.Sprintf("%s/v1/projects/-/serviceAccounts/%s:generateIdToken",
		a.cfg.IAMEndpoint, a.cfg.BrokerSAEmail)

	requestBody := map[string]interface{}{
		"audience":     a.cfg.GatewayAudience,
		"includeEmail": true,
	}
	requestJSON, err := json.Marshal(requestBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal IAM request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBuffer(requestJSON))
	if err != nil {
		return "", fmt.Errorf("failed to create IAM request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+workforceToken)
	req.Header.Set("x-goog-user-project", a.cfg.GCPProject)

	resp, err := a.cfg.HTTPClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("IAM request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read IAM response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("IAM returned status %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		Token string `json:"token"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", fmt.Errorf("failed to parse IAM response: %w", err)
	}

	if tokenResp.Token == "" {
		return "", fmt.Errorf("IAM response missing token")
	}

	return tokenResp.Token, nil
}

// WorkforceCredentialConfig generates an external_account credential configuration
// for use by hypershift. This configuration enables workload identity federation
// with the broker service account.
func WorkforceCredentialConfig(cfg TargetConfig, subjectTokenFile string) []byte {
	audience := fmt.Sprintf("//iam.googleapis.com/locations/global/workforcePools/%s/providers/%s",
		cfg.WorkforcePool, cfg.WorkforceProvider)

	impersonationURL := fmt.Sprintf("https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
		cfg.BrokerSAEmail)

	credConfig := map[string]interface{}{
		"type":                      "external_account",
		"audience":                  audience,
		"subject_token_type":        "urn:ietf:params:oauth:token-type:jwt",
		"token_url":                 "https://sts.googleapis.com/v1/token",
		"credential_source": map[string]interface{}{
			"file": subjectTokenFile,
		},
		"service_account_impersonation_url": impersonationURL,
	}

	jsonData, _ := json.Marshal(credConfig)
	return jsonData
}
