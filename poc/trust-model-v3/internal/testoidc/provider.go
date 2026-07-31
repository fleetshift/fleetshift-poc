// Package testoidc implements the smallest useful OIDC provider for this POC.
// It serves discovery, authorization, token, and JWKS endpoints over TLS and
// exercises authorization code, state, nonce, and PKCE instead of handing the
// client a programmatically minted token.
package testoidc

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"
)

type Config struct {
	ClientID    string
	RedirectURI string
}

type authorizationCode struct {
	clientID      string
	redirectURI   string
	subject       string
	nonce         string
	codeChallenge string
	expiresAt     time.Time
}

type Provider struct {
	mu sync.Mutex

	config              Config
	issuer              string
	server              *httptest.Server
	httpClient          *http.Client
	privateKey          jwk.Key
	jwks                []byte
	codes               map[string]authorizationCode
	successfulExchanges int
}

func Start(config Config) (*Provider, error) {
	if config.ClientID == "" || config.RedirectURI == "" {
		return nil, errors.New("OIDC client ID and redirect URI are required")
	}
	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate OIDC signing key: %w", err)
	}
	privateKey, err := jwk.Import(signingKey)
	if err != nil {
		return nil, fmt.Errorf("import private signing key: %w", err)
	}
	if err := privateKey.Set(jwk.KeyIDKey, "test-oidc-key"); err != nil {
		return nil, fmt.Errorf("set private key ID: %w", err)
	}
	publicKey, err := jwk.Import(signingKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("import public signing key: %w", err)
	}
	if err := publicKey.Set(jwk.KeyIDKey, "test-oidc-key"); err != nil {
		return nil, fmt.Errorf("set public key ID: %w", err)
	}
	if err := publicKey.Set(jwk.AlgorithmKey, jwa.ES256()); err != nil {
		return nil, fmt.Errorf("set public key algorithm: %w", err)
	}
	set := jwk.NewSet()
	if err := set.AddKey(publicKey); err != nil {
		return nil, fmt.Errorf("add public key to JWKS: %w", err)
	}
	jwksJSON, err := json.Marshal(set)
	if err != nil {
		return nil, fmt.Errorf("marshal JWKS: %w", err)
	}

	provider := &Provider{
		config:     config,
		privateKey: privateKey,
		jwks:       jwksJSON,
		codes:      make(map[string]authorizationCode),
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, fmt.Errorf("listen for OIDC provider: %w", err)
	}
	server := &httptest.Server{
		Listener: listener,
		Config:   &http.Server{Handler: http.HandlerFunc(provider.serveHTTP)},
	}
	server.StartTLS()
	provider.server = server
	provider.issuer = server.URL
	provider.httpClient = server.Client()
	return provider, nil
}

func (p *Provider) Issuer() string {
	return p.issuer
}

func (p *Provider) HTTPClient() *http.Client {
	return p.httpClient
}

func (p *Provider) SuccessfulCodeExchanges() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.successfulExchanges
}

func (p *Provider) Close() {
	if p.server != nil {
		p.server.Close()
	}
}

func (p *Provider) serveHTTP(response http.ResponseWriter, request *http.Request) {
	switch request.URL.Path {
	case "/.well-known/openid-configuration":
		p.discovery(response)
	case "/authorize":
		p.authorize(response, request)
	case "/token":
		p.token(response, request)
	case "/jwks":
		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write(p.jwks)
	default:
		http.NotFound(response, request)
	}
}

func (p *Provider) discovery(response http.ResponseWriter) {
	document := map[string]any{
		"issuer":                                p.issuer,
		"authorization_endpoint":                p.issuer + "/authorize",
		"token_endpoint":                        p.issuer + "/token",
		"jwks_uri":                              p.issuer + "/jwks",
		"response_types_supported":              []string{"code"},
		"subject_types_supported":               []string{"public"},
		"id_token_signing_alg_values_supported": []string{"ES256"},
		"scopes_supported":                      []string{"openid"},
		"code_challenge_methods_supported":      []string{"S256"},
	}
	writeJSON(response, http.StatusOK, document)
}

func (p *Provider) authorize(response http.ResponseWriter, request *http.Request) {
	query := request.URL.Query()
	if query.Get("response_type") != "code" || query.Get("client_id") != p.config.ClientID || query.Get("redirect_uri") != p.config.RedirectURI {
		http.Error(response, "invalid authorization request", http.StatusBadRequest)
		return
	}
	if !containsToken(query.Get("scope"), "openid") || query.Get("state") == "" || query.Get("nonce") == "" {
		http.Error(response, "openid scope, state, and nonce are required", http.StatusBadRequest)
		return
	}
	if query.Get("code_challenge_method") != "S256" || query.Get("code_challenge") == "" {
		http.Error(response, "PKCE S256 is required", http.StatusBadRequest)
		return
	}
	subject := query.Get("login_hint")
	if subject == "" {
		subject = "test-user"
	}
	code, err := randomValue()
	if err != nil {
		http.Error(response, "generate authorization code", http.StatusInternalServerError)
		return
	}
	p.mu.Lock()
	p.codes[code] = authorizationCode{
		clientID:      p.config.ClientID,
		redirectURI:   p.config.RedirectURI,
		subject:       subject,
		nonce:         query.Get("nonce"),
		codeChallenge: query.Get("code_challenge"),
		expiresAt:     time.Now().Add(time.Minute),
	}
	p.mu.Unlock()

	redirect, _ := url.Parse(p.config.RedirectURI)
	values := redirect.Query()
	values.Set("code", code)
	values.Set("state", query.Get("state"))
	redirect.RawQuery = values.Encode()
	http.Redirect(response, request, redirect.String(), http.StatusFound)
}

func (p *Provider) token(response http.ResponseWriter, request *http.Request) {
	if request.Method != http.MethodPost {
		writeOAuthError(response, http.StatusMethodNotAllowed, "invalid_request")
		return
	}
	if err := request.ParseForm(); err != nil {
		writeOAuthError(response, http.StatusBadRequest, "invalid_request")
		return
	}
	if request.Form.Get("grant_type") != "authorization_code" {
		writeOAuthError(response, http.StatusBadRequest, "unsupported_grant_type")
		return
	}
	codeValue := request.Form.Get("code")
	p.mu.Lock()
	code, ok := p.codes[codeValue]
	delete(p.codes, codeValue)
	p.mu.Unlock()
	if !ok || time.Now().After(code.expiresAt) {
		writeOAuthError(response, http.StatusBadRequest, "invalid_grant")
		return
	}
	if request.Form.Get("client_id") != code.clientID || request.Form.Get("redirect_uri") != code.redirectURI {
		writeOAuthError(response, http.StatusBadRequest, "invalid_grant")
		return
	}
	verifierDigest := sha256.Sum256([]byte(request.Form.Get("code_verifier")))
	wantChallenge := base64.RawURLEncoding.EncodeToString(verifierDigest[:])
	if subtle.ConstantTimeCompare([]byte(wantChallenge), []byte(code.codeChallenge)) != 1 {
		writeOAuthError(response, http.StatusBadRequest, "invalid_grant")
		return
	}

	now := time.Now()
	token, err := jwt.NewBuilder().
		Issuer(p.issuer).
		Subject(code.subject).
		Audience([]string{code.clientID}).
		IssuedAt(now).
		Expiration(now.Add(5*time.Minute)).
		Claim("nonce", code.nonce).
		Build()
	if err != nil {
		writeOAuthError(response, http.StatusInternalServerError, "server_error")
		return
	}
	signed, err := jwt.Sign(token, jwt.WithKey(jwa.ES256(), p.privateKey))
	if err != nil {
		writeOAuthError(response, http.StatusInternalServerError, "server_error")
		return
	}
	accessToken, err := randomValue()
	if err != nil {
		writeOAuthError(response, http.StatusInternalServerError, "server_error")
		return
	}
	p.mu.Lock()
	p.successfulExchanges++
	p.mu.Unlock()
	writeJSON(response, http.StatusOK, map[string]any{
		"access_token": accessToken,
		"token_type":   "Bearer",
		"expires_in":   300,
		"id_token":     string(signed),
	})
}

func writeOAuthError(response http.ResponseWriter, status int, code string) {
	writeJSON(response, status, map[string]string{"error": code})
}

func writeJSON(response http.ResponseWriter, status int, value any) {
	response.Header().Set("Content-Type", "application/json")
	response.WriteHeader(status)
	_ = json.NewEncoder(response).Encode(value)
}

func containsToken(list, want string) bool {
	for _, value := range strings.Fields(list) {
		if value == want {
			return true
		}
	}
	return false
}

func randomValue() (string, error) {
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(value), nil
}
