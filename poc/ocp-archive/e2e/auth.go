//go:build e2e

package e2e

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// DeviceCodeResponse holds the response from the device authorization endpoint.
type DeviceCodeResponse struct {
	DeviceCode              string `json:"device_code"`
	UserCode                string `json:"user_code"`
	VerificationURI         string `json:"verification_uri"`
	VerificationURIComplete string `json:"verification_uri_complete"`
	ExpiresIn               int    `json:"expires_in"`
	Interval                int    `json:"interval"`
}

// TokenResponse holds the response from the token endpoint.
// The Error field is set on pending/failed responses during the device code flow.
type TokenResponse struct {
	AccessToken  string `json:"access_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	IDToken      string `json:"id_token"`
	Error        string `json:"error,omitempty"`
}

// OIDCDiscovery holds the endpoints needed for the device code flow.
type OIDCDiscovery struct {
	TokenEndpoint               string `json:"token_endpoint"`
	DeviceAuthorizationEndpoint string `json:"device_authorization_endpoint"`
}

// discoverOIDC fetches the OIDC discovery document from the issuer's
// .well-known/openid-configuration endpoint.
func discoverOIDC(ctx context.Context, issuer string) (*OIDCDiscovery, error) {
	wellKnown := strings.TrimRight(issuer, "/") + "/.well-known/openid-configuration"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, wellKnown, nil)
	if err != nil {
		return nil, fmt.Errorf("creating discovery request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching discovery document: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("discovery endpoint returned status %d", resp.StatusCode)
	}

	var doc OIDCDiscovery
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return nil, fmt.Errorf("decoding discovery document: %w", err)
	}

	if doc.DeviceAuthorizationEndpoint == "" {
		return nil, fmt.Errorf("issuer %q does not advertise a device_authorization_endpoint", issuer)
	}

	return &doc, nil
}

// generatePKCE creates a PKCE code verifier and S256 challenge.
func generatePKCE() (verifier, challenge string) {
	b := make([]byte, 32)
	rand.Read(b)
	verifier = base64.RawURLEncoding.EncodeToString(b)
	h := sha256.Sum256([]byte(verifier))
	challenge = base64.RawURLEncoding.EncodeToString(h[:])
	return verifier, challenge
}

// startDeviceCodeFlow initiates the device code flow by posting to the device
// authorization endpoint. It includes PKCE parameters (required by some
// providers like Red Hat SSO). Returns the device code response and the PKCE
// code verifier (needed when polling for the token).
func startDeviceCodeFlow(ctx context.Context, deviceEndpoint, clientID, scope string) (*DeviceCodeResponse, string, error) {
	verifier, challenge := generatePKCE()

	data := url.Values{
		"client_id":             {clientID},
		"scope":                 {scope},
		"code_challenge_method": {"S256"},
		"code_challenge":        {challenge},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, deviceEndpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, "", fmt.Errorf("creating device code request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, "", fmt.Errorf("requesting device code: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, "", fmt.Errorf("device auth endpoint returned status %d: %s", resp.StatusCode, string(body))
	}

	var dcr DeviceCodeResponse
	if err := json.NewDecoder(resp.Body).Decode(&dcr); err != nil {
		return nil, "", fmt.Errorf("decoding device code response: %w", err)
	}

	if dcr.Interval == 0 {
		dcr.Interval = 5
	}

	return &dcr, verifier, nil
}

// pollForToken polls the token endpoint until the user completes authentication,
// the device code expires, or the context is cancelled.
func pollForToken(ctx context.Context, tokenEndpoint, clientID, deviceCode, codeVerifier string, interval int) (*TokenResponse, error) {
	data := url.Values{
		"grant_type":  {"urn:ietf:params:oauth:grant-type:device_code"},
		"client_id":   {clientID},
		"device_code": {deviceCode},
	}
	if codeVerifier != "" {
		data.Set("code_verifier", codeVerifier)
	}

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(interval) * time.Second):
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenEndpoint, strings.NewReader(data.Encode()))
		if err != nil {
			return nil, fmt.Errorf("creating token request: %w", err)
		}
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("polling token endpoint: %w", err)
		}

		body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
		resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("reading token response: %w", err)
		}

		var token TokenResponse
		if err := json.Unmarshal(body, &token); err != nil {
			return nil, fmt.Errorf("decoding token response: %w", err)
		}

		switch token.Error {
		case "authorization_pending":
			continue
		case "slow_down":
			interval += 5
			continue
		case "expired_token":
			return nil, fmt.Errorf("device code expired before user authenticated")
		case "":
			return &token, nil
		default:
			return nil, fmt.Errorf("token endpoint error: %s", token.Error)
		}
	}
}

// DeviceCodeLogin performs the full OIDC device code login flow.
// It discovers endpoints, initiates the device code flow, prints
// instructions for the tester, and polls until authentication completes.
func DeviceCodeLogin(ctx context.Context, issuer, clientID, scope, label string) (*TokenResponse, error) {
	fmt.Printf("\n=== %s ===\n", label)
	fmt.Printf("Discovering OIDC endpoints from %s...\n", issuer)

	doc, err := discoverOIDC(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("OIDC discovery: %w", err)
	}

	dcr, codeVerifier, err := startDeviceCodeFlow(ctx, doc.DeviceAuthorizationEndpoint, clientID, scope)
	if err != nil {
		return nil, fmt.Errorf("device code flow: %w", err)
	}

	fmt.Println()
	fmt.Println("Open this link to authenticate:")
	if dcr.VerificationURIComplete != "" {
		fmt.Printf("  %s\n", dcr.VerificationURIComplete)
	} else {
		fmt.Printf("  %s\n", dcr.VerificationURI)
		fmt.Printf("\nEnter code: %s\n", dcr.UserCode)
	}

	fmt.Printf("\nWaiting for login (expires in %ds)...\n", dcr.ExpiresIn)

	token, err := pollForToken(ctx, doc.TokenEndpoint, clientID, dcr.DeviceCode, codeVerifier, dcr.Interval)
	if err != nil {
		return nil, err
	}

	fmt.Println("✓ Authenticated")
	return token, nil
}

// RefreshAccessToken uses a refresh token to obtain a fresh access token from
// the issuer's token endpoint. This is needed when the original access token
// has expired (e.g. after a long provisioning wait).
func RefreshAccessToken(ctx context.Context, issuer, clientID, refreshToken string) (*TokenResponse, error) {
	doc, err := discoverOIDC(ctx, issuer)
	if err != nil {
		return nil, fmt.Errorf("OIDC discovery: %w", err)
	}

	data := url.Values{
		"grant_type":    {"refresh_token"},
		"client_id":     {clientID},
		"refresh_token": {refreshToken},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, doc.TokenEndpoint, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("creating refresh request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("refreshing token: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("reading refresh response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("refresh endpoint returned status %d: %s", resp.StatusCode, string(body))
	}

	var token TokenResponse
	if err := json.Unmarshal(body, &token); err != nil {
		return nil, fmt.Errorf("decoding refresh response: %w", err)
	}

	return &token, nil
}

// FetchPullSecret uses the provided access token (from Red Hat SSO) to fetch
// the pull secret from the OpenShift accounts API.
func FetchPullSecret(ctx context.Context, accessToken string) ([]byte, error) {
	const pullSecretURL = "https://api.openshift.com/api/accounts_mgmt/v1/access_token"

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, pullSecretURL, strings.NewReader("{}"))
	if err != nil {
		return nil, fmt.Errorf("creating pull secret request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+accessToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetching pull secret: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return nil, fmt.Errorf("pull secret API returned status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1 MiB limit
	if err != nil {
		return nil, fmt.Errorf("reading pull secret response: %w", err)
	}

	// Validate that the response contains an "auths" field.
	var parsed map[string]json.RawMessage
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("pull secret is not valid JSON: %w", err)
	}
	if _, ok := parsed["auths"]; !ok {
		return nil, fmt.Errorf("pull secret response missing 'auths' field")
	}

	return body, nil
}
