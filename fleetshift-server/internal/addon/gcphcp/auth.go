package gcphcp

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const defaultBrokerHTTPTimeout = 30 * time.Second

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

	// WorkforceTokenExpiry is when the Workforce access token expires.
	WorkforceTokenExpiry time.Time
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
		cfg.HTTPClient = &http.Client{Timeout: defaultBrokerHTTPTimeout}
	}

	return &BrokerAuth{cfg: cfg}
}

// Exchange performs a two-step auth flow:
// 1. Exchange the caller's OIDC token for a Workforce access token via STS.
// 2. Use the Workforce token to mint a broker ID token via IAM generateIdToken.
func (a *BrokerAuth) Exchange(ctx context.Context, callerToken string) (BrokerAuthResult, error) {
	if callerToken == "" {
		return BrokerAuthResult{}, newAuthExpiredError(fmt.Errorf("caller token is empty"))
	}

	// Step 1: STS token exchange
	workforceToken, workforceTokenExpiry, err := a.exchangeSTS(ctx, callerToken)
	if err != nil {
		return BrokerAuthResult{}, fmt.Errorf("STS token exchange failed: %w", err)
	}

	// Step 2: Generate broker ID token
	brokerToken, err := a.generateIDToken(ctx, workforceToken)
	if err != nil {
		return BrokerAuthResult{}, fmt.Errorf("broker ID token generation failed: %w", err)
	}

	return BrokerAuthResult{
		BrokerToken:          brokerToken,
		BrokerEmail:          a.cfg.BrokerSAEmail,
		WorkforceToken:       workforceToken,
		WorkforceTokenExpiry: workforceTokenExpiry,
	}, nil
}

func workforceAudience(pool, provider string) string {
	return fmt.Sprintf("//iam.googleapis.com/locations/global/workforcePools/%s/providers/%s", pool, provider)
}

// exchangeSTS exchanges the caller's OIDC token for a Workforce access token.
func (a *BrokerAuth) exchangeSTS(ctx context.Context, callerToken string) (string, time.Time, error) {
	audience := workforceAudience(a.cfg.WorkforcePool, a.cfg.WorkforceProvider)

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
		return "", time.Time{}, fmt.Errorf("failed to create STS request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := a.cfg.HTTPClient.Do(req)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("STS request failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", time.Time{}, fmt.Errorf("failed to read STS response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var oauthErr struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(body, &oauthErr)
		baseErr := fmt.Errorf("STS returned status %d: %s", resp.StatusCode, string(body))
		if resp.StatusCode == http.StatusUnauthorized || oauthErr.Error == "invalid_grant" {
			return "", time.Time{}, newAuthExpiredError(baseErr)
		}
		return "", time.Time{}, baseErr
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", time.Time{}, fmt.Errorf("failed to parse STS response: %w", err)
	}

	if tokenResp.AccessToken == "" {
		return "", time.Time{}, fmt.Errorf("STS response missing access_token")
	}
	if tokenResp.ExpiresIn <= 0 {
		return "", time.Time{}, fmt.Errorf("STS response missing expires_in")
	}

	return tokenResp.AccessToken, time.Now().Add(time.Duration(tokenResp.ExpiresIn) * time.Second), nil
}

// generateIDToken uses the Workforce token to generate a broker ID token.
func (a *BrokerAuth) generateIDToken(ctx context.Context, workforceToken string) (string, error) {
	endpoint := fmt.Sprintf("%s/v1/projects/-/serviceAccounts/%s:generateIdToken",
		a.cfg.IAMEndpoint, a.cfg.BrokerSAEmail)

	requestBody := map[string]any{
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
		baseErr := fmt.Errorf("IAM returned status %d: %s", resp.StatusCode, string(body))
		if resp.StatusCode == http.StatusUnauthorized {
			return "", newAuthExpiredError(baseErr)
		}
		return "", baseErr
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
	jsonData, _ := buildCredentialConfig(cfg, subjectTokenFile, "", true, false)
	return jsonData
}

// buildCredentialConfig builds an external_account credential JSON.
// impersonate adds service_account_impersonation_url; userProject adds workforce_pool_user_project.
func buildCredentialConfig(cfg TargetConfig, subjectTokenFile, tokenURL string, impersonate, userProject bool) ([]byte, error) {
	if tokenURL == "" {
		tokenURL = "https://sts.googleapis.com/v1/token"
	}
	credConfig := map[string]any{
		"type":               "external_account",
		"audience":           workforceAudience(cfg.WorkforcePool, cfg.WorkforceProvider),
		"subject_token_type": "urn:ietf:params:oauth:token-type:jwt",
		"token_url":          tokenURL,
		"credential_source": map[string]any{
			"file": subjectTokenFile,
		},
	}
	if impersonate {
		credConfig["service_account_impersonation_url"] = fmt.Sprintf(
			"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/%s:generateAccessToken",
			cfg.BrokerSAEmail,
		)
	}
	if userProject {
		credConfig["workforce_pool_user_project"] = cfg.GCPProject
	}
	return json.MarshalIndent(credConfig, "", "  ")
}
