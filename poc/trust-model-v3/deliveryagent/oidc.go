package deliveryagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

type oidcIdentity struct {
	Issuer  string
	Subject string
}

var errOIDCEvidenceUnavailable = errors.New("OIDC verification evidence is unavailable")

type discoveryDocument struct {
	Issuer  string `json:"issuer"`
	JWKSURI string `json:"jwks_uri"`
}

func verifyEnrollmentIDToken(ctx context.Context, config Config, nonce, rawToken string) (oidcIdentity, error) {
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	discoveryRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(config.OIDCIssuer, "/")+"/.well-known/openid-configuration", nil)
	if err != nil {
		return oidcIdentity{}, fmt.Errorf("create discovery request: %w", err)
	}
	discoveryResponse, err := httpClient.Do(discoveryRequest)
	if err != nil {
		return oidcIdentity{}, fmt.Errorf("%w: discovery: %v", errOIDCEvidenceUnavailable, err)
	}
	defer discoveryResponse.Body.Close()
	if discoveryResponse.StatusCode != http.StatusOK {
		return oidcIdentity{}, fmt.Errorf("%w: discovery response: %s", errOIDCEvidenceUnavailable, discoveryResponse.Status)
	}
	var discovery discoveryDocument
	if err := json.NewDecoder(io.LimitReader(discoveryResponse.Body, 1<<20)).Decode(&discovery); err != nil {
		return oidcIdentity{}, fmt.Errorf("%w: decode discovery: %v", errOIDCEvidenceUnavailable, err)
	}
	if discovery.Issuer != config.OIDCIssuer || discovery.JWKSURI == "" {
		return oidcIdentity{}, errors.New("OIDC discovery does not match provisioned issuer")
	}

	jwksRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, discovery.JWKSURI, nil)
	if err != nil {
		return oidcIdentity{}, fmt.Errorf("create JWKS request: %w", err)
	}
	jwksResponse, err := httpClient.Do(jwksRequest)
	if err != nil {
		return oidcIdentity{}, fmt.Errorf("%w: fetch JWKS: %v", errOIDCEvidenceUnavailable, err)
	}
	defer jwksResponse.Body.Close()
	if jwksResponse.StatusCode != http.StatusOK {
		return oidcIdentity{}, fmt.Errorf("%w: JWKS response: %s", errOIDCEvidenceUnavailable, jwksResponse.Status)
	}
	keySet, err := jwk.ParseReader(io.LimitReader(jwksResponse.Body, 1<<20))
	if err != nil {
		return oidcIdentity{}, fmt.Errorf("%w: parse JWKS: %v", errOIDCEvidenceUnavailable, err)
	}
	token, err := jwt.ParseString(rawToken,
		jwt.WithKeySet(keySet),
		jwt.WithValidate(true),
		jwt.WithIssuer(config.OIDCIssuer),
		jwt.WithAudience(config.OIDCClientID),
	)
	if err != nil {
		return oidcIdentity{}, fmt.Errorf("verify ID token: %w", err)
	}
	if _, ok := token.Expiration(); !ok {
		return oidcIdentity{}, errors.New("ID token has no expiration")
	}
	if _, ok := token.IssuedAt(); !ok {
		return oidcIdentity{}, errors.New("ID token has no issued-at time")
	}
	issuer, ok := token.Issuer()
	if !ok || issuer == "" {
		return oidcIdentity{}, errors.New("ID token has no issuer")
	}
	subject, ok := token.Subject()
	if !ok || subject == "" {
		return oidcIdentity{}, errors.New("ID token has no subject")
	}
	var tokenNonce string
	if err := token.Get("nonce", &tokenNonce); err != nil || tokenNonce != nonce {
		return oidcIdentity{}, fmt.Errorf("ID token nonce %q does not match enrollment nonce", tokenNonce)
	}
	return oidcIdentity{Issuer: issuer, Subject: subject}, nil
}
