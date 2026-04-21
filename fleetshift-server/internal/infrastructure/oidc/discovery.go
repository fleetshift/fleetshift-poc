package oidc

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DiscoveryClient implements [domain.OIDCDiscoveryClient] by fetching
// the OpenID Connect discovery document via HTTP.
type DiscoveryClient struct {
	HTTP *http.Client
}

// NewDiscoveryClient creates a discovery client using the given HTTP client.
// If client is nil, [http.DefaultClient] is used.
func NewDiscoveryClient(client *http.Client) *DiscoveryClient {
	if client == nil {
		client = http.DefaultClient
	}
	return &DiscoveryClient{HTTP: client}
}

type discoveryDocument struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	JWKSURI               string `json:"jwks_uri"`
}

// FetchMetadata retrieves the OIDC discovery document from the issuer's
// well-known endpoint.
func (c *DiscoveryClient) FetchMetadata(ctx context.Context, issuerURL domain.IssuerURL) (domain.OIDCMetadata, error) {
	endpoint := string(issuerURL) + "/.well-known/openid-configuration"

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return domain.OIDCMetadata{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.HTTP.Do(req)
	if err != nil {
		return domain.OIDCMetadata{}, fmt.Errorf("fetch discovery document: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return domain.OIDCMetadata{}, fmt.Errorf("discovery endpoint returned %d", resp.StatusCode)
	}

	var doc discoveryDocument
	if err := json.NewDecoder(resp.Body).Decode(&doc); err != nil {
		return domain.OIDCMetadata{}, fmt.Errorf("decode discovery document: %w", err)
	}

	if doc.JWKSURI == "" {
		return domain.OIDCMetadata{}, fmt.Errorf("discovery document missing jwks_uri")
	}

	return domain.OIDCMetadata{
		Issuer:                domain.IssuerURL(doc.Issuer),
		AuthorizationEndpoint: domain.EndpointURL(doc.AuthorizationEndpoint),
		TokenEndpoint:         domain.EndpointURL(doc.TokenEndpoint),
		JWKSURI:               domain.EndpointURL(doc.JWKSURI),
	}, nil
}
