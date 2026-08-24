package client

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

type oidcConfig struct {
	issuer      string
	clientID    string
	redirectURI string
	httpClient  *http.Client
}

type oidcIdentity struct {
	Issuer  string
	Subject string
}

type discoveryDocument struct {
	Issuer                string `json:"issuer"`
	AuthorizationEndpoint string `json:"authorization_endpoint"`
	TokenEndpoint         string `json:"token_endpoint"`
	JWKSURI               string `json:"jwks_uri"`
}

type tokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
	IDToken     string `json:"id_token"`
	Error       string `json:"error"`
}

func authenticateOIDC(ctx context.Context, config oidcConfig, nonce, loginHint string) (oidcIdentity, string, error) {
	httpClient := config.httpClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	discovery, err := discover(ctx, httpClient, config.issuer)
	if err != nil {
		return oidcIdentity{}, "", err
	}
	if discovery.Issuer != config.issuer {
		return oidcIdentity{}, "", fmt.Errorf("discovery issuer %q does not match configured issuer %q", discovery.Issuer, config.issuer)
	}

	state, err := randomToken()
	if err != nil {
		return oidcIdentity{}, "", err
	}
	verifier, err := randomToken()
	if err != nil {
		return oidcIdentity{}, "", err
	}
	challengeBytes := sha256.Sum256([]byte(verifier))
	challenge := base64.RawURLEncoding.EncodeToString(challengeBytes[:])

	authorizationURL, err := url.Parse(discovery.AuthorizationEndpoint)
	if err != nil {
		return oidcIdentity{}, "", fmt.Errorf("parse authorization endpoint: %w", err)
	}
	query := authorizationURL.Query()
	query.Set("response_type", "code")
	query.Set("client_id", config.clientID)
	query.Set("redirect_uri", config.redirectURI)
	query.Set("scope", "openid")
	query.Set("state", state)
	query.Set("nonce", nonce)
	query.Set("code_challenge", challenge)
	query.Set("code_challenge_method", "S256")
	if loginHint != "" {
		query.Set("login_hint", loginHint)
	}
	authorizationURL.RawQuery = query.Encode()

	redirectClient := *httpClient
	redirectClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, authorizationURL.String(), nil)
	if err != nil {
		return oidcIdentity{}, "", fmt.Errorf("create authorization request: %w", err)
	}
	response, err := redirectClient.Do(req)
	if err != nil {
		return oidcIdentity{}, "", fmt.Errorf("authorization request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusFound {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return oidcIdentity{}, "", fmt.Errorf("authorization response %s: %s", response.Status, strings.TrimSpace(string(body)))
	}
	location, err := response.Location()
	if err != nil {
		return oidcIdentity{}, "", fmt.Errorf("parse authorization redirect: %w", err)
	}
	if got := location.Query().Get("state"); got != state {
		return oidcIdentity{}, "", fmt.Errorf("authorization state %q does not match %q", got, state)
	}
	code := location.Query().Get("code")
	if code == "" {
		return oidcIdentity{}, "", errors.New("authorization response has no code")
	}

	form := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"client_id":     {config.clientID},
		"redirect_uri":  {config.redirectURI},
		"code_verifier": {verifier},
	}
	tokenReq, err := http.NewRequestWithContext(ctx, http.MethodPost, discovery.TokenEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return oidcIdentity{}, "", fmt.Errorf("create token request: %w", err)
	}
	tokenReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	tokenHTTPResponse, err := httpClient.Do(tokenReq)
	if err != nil {
		return oidcIdentity{}, "", fmt.Errorf("token request: %w", err)
	}
	defer func() { _ = tokenHTTPResponse.Body.Close() }()
	var tokens tokenResponse
	if err := json.NewDecoder(io.LimitReader(tokenHTTPResponse.Body, 1<<20)).Decode(&tokens); err != nil {
		return oidcIdentity{}, "", fmt.Errorf("decode token response: %w", err)
	}
	if tokenHTTPResponse.StatusCode != http.StatusOK || tokens.Error != "" {
		return oidcIdentity{}, "", fmt.Errorf("token response %s: %s", tokenHTTPResponse.Status, tokens.Error)
	}
	if tokens.IDToken == "" {
		return oidcIdentity{}, "", errors.New("token response has no ID token")
	}

	identity, err := verifyIDToken(ctx, httpClient, discovery, config.clientID, nonce, tokens.IDToken)
	if err != nil {
		return oidcIdentity{}, "", err
	}
	return identity, tokens.IDToken, nil
}

func discover(ctx context.Context, client *http.Client, issuer string) (discoveryDocument, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(issuer, "/")+"/.well-known/openid-configuration", nil)
	if err != nil {
		return discoveryDocument{}, fmt.Errorf("create discovery request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return discoveryDocument{}, fmt.Errorf("OIDC discovery: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return discoveryDocument{}, fmt.Errorf("OIDC discovery response: %s", response.Status)
	}
	var document discoveryDocument
	if err := json.NewDecoder(io.LimitReader(response.Body, 1<<20)).Decode(&document); err != nil {
		return discoveryDocument{}, fmt.Errorf("decode OIDC discovery: %w", err)
	}
	if document.Issuer == "" || document.AuthorizationEndpoint == "" || document.TokenEndpoint == "" || document.JWKSURI == "" {
		return discoveryDocument{}, errors.New("OIDC discovery document is incomplete")
	}
	return document, nil
}

func verifyIDToken(ctx context.Context, client *http.Client, discovery discoveryDocument, clientID, nonce, rawToken string) (oidcIdentity, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, discovery.JWKSURI, nil)
	if err != nil {
		return oidcIdentity{}, fmt.Errorf("create JWKS request: %w", err)
	}
	response, err := client.Do(request)
	if err != nil {
		return oidcIdentity{}, fmt.Errorf("fetch JWKS: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	if response.StatusCode != http.StatusOK {
		return oidcIdentity{}, fmt.Errorf("JWKS response: %s", response.Status)
	}
	keySet, err := jwk.ParseReader(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return oidcIdentity{}, fmt.Errorf("parse JWKS: %w", err)
	}
	token, err := jwt.ParseString(rawToken,
		jwt.WithKeySet(keySet),
		jwt.WithValidate(true),
		jwt.WithIssuer(discovery.Issuer),
		jwt.WithAudience(clientID),
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

func randomToken() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
