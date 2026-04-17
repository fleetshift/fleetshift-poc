package keyregistry

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// KeycloakClient fetches a user's signing public key from a Keycloak
// user attribute via the Admin REST API. This is needed because
// verification happens server-side (no user session available) when
// an agent delivers a signed deployment.
//
// POC limitation: uses resource-owner password grant with bootstrap
// admin credentials. For production, replace with a dedicated service
// account (client credentials grant) scoped to view-users, or extract
// into a pluggable addon.
type KeycloakClient struct {
	HTTP          *http.Client
	AdminUsername string
	AdminPassword string
}

type keycloakUser struct {
	Attributes map[string][]string `json:"attributes"`
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
}

func (c *KeycloakClient) getAdminToken(ctx context.Context, baseURL, realm string) (string, error) {
	tokenURL := fmt.Sprintf("%s/realms/%s/protocol/openid-connect/token", baseURL, realm)
	body := fmt.Sprintf("grant_type=password&client_id=admin-cli&username=%s&password=%s",
		c.AdminUsername, c.AdminPassword)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("build token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("request admin token: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return "", fmt.Errorf("admin token request returned %d: %s", resp.StatusCode, respBody)
	}

	var tok tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&tok); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}
	return tok.AccessToken, nil
}

func (c *KeycloakClient) FetchSigningKeys(ctx context.Context, endpoint string, subject domain.RegistrySubject) ([]crypto.PublicKey, error) {
	// The endpoint is the Keycloak issuer URL, e.g.
	// https://keycloak.example.com/realms/fleetshift
	// The Admin REST API path is:
	// /admin/realms/{realm}/users?username={subject}&exact=true
	//
	// Extract realm from the issuer URL (last path segment).
	issuer := strings.TrimRight(endpoint, "/")
	parts := strings.Split(issuer, "/")
	realm := parts[len(parts)-1]
	baseURL := strings.TrimSuffix(issuer, "/realms/"+realm)

	// Obtain admin token for the master realm
	adminToken, err := c.getAdminToken(ctx, baseURL, "master")
	if err != nil {
		return nil, fmt.Errorf("get admin token: %w", err)
	}

	url := fmt.Sprintf("%s/admin/realms/%s/users?username=%s&exact=true",
		baseURL, realm, string(subject))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+adminToken)

	client := c.HTTP
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch keycloak user: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("keycloak API returned %d: %s", resp.StatusCode, body)
	}

	var users []keycloakUser
	if err := json.NewDecoder(resp.Body).Decode(&users); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	if len(users) == 0 {
		return nil, fmt.Errorf("keycloak user %q not found", subject)
	}

	attrs := users[0].Attributes
	keyValues, ok := attrs["signing_public_key"]
	if !ok || len(keyValues) == 0 {
		return nil, fmt.Errorf("keycloak user %q has no signing_public_key attribute", subject)
	}

	var out []crypto.PublicKey
	for _, val := range keyValues {
		derBytes, err := base64.StdEncoding.DecodeString(val)
		if err != nil {
			continue
		}
		pub, err := x509.ParsePKIXPublicKey(derBytes)
		if err != nil {
			continue
		}
		ecPub, ok := pub.(*ecdsa.PublicKey)
		if !ok || ecPub.Curve != elliptic.P256() {
			continue
		}
		out = append(out, ecPub)
	}

	return out, nil
}
