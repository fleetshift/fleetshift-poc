package auth

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// DefaultOIDCHTTPTimeout bounds OIDC HTTP when the caller provides no
// context deadline, and is set on [Config.HTTPClient].
const DefaultOIDCHTTPTimeout = 30 * time.Second

// DiscoveryEndpoints holds authorization and token endpoint URLs from OIDC
// discovery.
type DiscoveryEndpoints struct {
	AuthorizationEndpoint string
	TokenEndpoint         string
}

// discoveryDocument is the subset of the OpenID Connect discovery JSON used
// by [DiscoverEndpoints].
type discoveryDocument struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
}

// DiscoverEndpoints fetches {issuer}/.well-known/openid-configuration and
// returns the authorization and token endpoints. issuerURL must be nonempty.
// When httpClient is nil, [http.DefaultClient] is used.
//
// If ctx has no deadline, a [DefaultOIDCHTTPTimeout] deadline is applied so a
// stalled issuer cannot block forever.
//
// The discovery document's issuer must match issuerURL after trimming a single
// trailing slash on each side. Authorization and token endpoints must be
// nonempty.
func DiscoverEndpoints(ctx context.Context, issuerURL string, httpClient *http.Client) (DiscoveryEndpoints, error) {
	issuerURL = strings.TrimSpace(issuerURL)
	if issuerURL == "" {
		return DiscoveryEndpoints{}, fmt.Errorf("issuer URL is required")
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, DefaultOIDCHTTPTimeout)
		defer cancel()
	}

	endpoint := strings.TrimRight(issuerURL, "/") + "/.well-known/openid-configuration"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return DiscoveryEndpoints{}, fmt.Errorf("create discovery request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		return DiscoveryEndpoints{}, fmt.Errorf("fetch OIDC discovery document: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return DiscoveryEndpoints{}, fmt.Errorf("OIDC discovery endpoint returned %d", resp.StatusCode)
	}

	var doc discoveryDocument
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return DiscoveryEndpoints{}, fmt.Errorf("decode OIDC discovery document: %w", err)
	}

	wantIssuer := strings.TrimRight(issuerURL, "/")
	gotIssuer := strings.TrimRight(strings.TrimSpace(doc.Issuer), "/")
	if gotIssuer == "" {
		return DiscoveryEndpoints{}, fmt.Errorf("OIDC discovery document missing issuer")
	}
	if gotIssuer != wantIssuer {
		return DiscoveryEndpoints{}, fmt.Errorf("OIDC discovery issuer %q does not match configured issuer %q", doc.Issuer, issuerURL)
	}
	if strings.TrimSpace(doc.AuthorizationEndpoint) == "" {
		return DiscoveryEndpoints{}, fmt.Errorf("OIDC discovery document missing authorization_endpoint")
	}
	if strings.TrimSpace(doc.TokenEndpoint) == "" {
		return DiscoveryEndpoints{}, fmt.Errorf("OIDC discovery document missing token_endpoint")
	}

	return DiscoveryEndpoints{
		AuthorizationEndpoint: doc.AuthorizationEndpoint,
		TokenEndpoint:         doc.TokenEndpoint,
	}, nil
}
