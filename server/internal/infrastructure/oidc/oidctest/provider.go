// Package oidctest provides a lightweight fake OIDC identity provider for
// testing. It generates real cryptographic keys, issues signed JWTs, and
// serves OIDC discovery and JWKS over HTTPS with a self-signed CA.
//
// This helper is for programmatic API identity (token minting + JWKS
// verification). It is not a browser IdP: authorization-code and token
// exchange endpoints are not implemented. Discovery still advertises
// authorization_endpoint and token_endpoint URLs so production
// CreateAuthMethod / OIDCConfig population can complete; those paths
// return HTTP 404 if called. Browser OIDC is out of scope here.
package oidctest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lestrrat-go/jwx/v3/jwa"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jwt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Provider is a fake OIDC identity provider for testing. It serves
// OIDC discovery and JWKS endpoints over HTTPS and issues signed JWTs
// programmatically via [Provider.IssueToken].
type Provider struct {
	issuerURL  string
	audience   string
	caCertPEM  []byte
	caCertPath string
	jwkPriv    jwk.Key
	jwksJSON   []byte
	server     *http.Server
	listener   net.Listener
	httpClient *http.Client
}

// TokenClaims configures the claims embedded in a token issued by
// [Provider.IssueToken].
type TokenClaims struct {
	Subject  string
	Groups   []string
	Email    string
	Expiry   time.Duration // from now; defaults to 1h
	Audience string        // overrides default audience if non-empty
	Extra    map[string]any
}

// Option configures a [Provider].
type Option func(*providerConfig)

// providerConfig holds options applied by Start.
type providerConfig struct {
	audience      string
	listenAddress string
	issuerURL     string   // override; empty means derive from listen address
	extraSANIPs   []net.IP // additional IP SANs for the server certificate
	caDir         string   // required; owned by the calling test via TempDir
}

// WithAudience sets the default audience for issued tokens.
// Defaults to "fleetshift".
func WithAudience(aud string) Option {
	return func(c *providerConfig) { c.audience = aud }
}

// WithListenAddress sets the address the HTTPS server binds to.
// Defaults to "127.0.0.1:0".
func WithListenAddress(addr string) Option {
	return func(c *providerConfig) { c.listenAddress = addr }
}

// WithExtraSANIPs adds additional IP addresses to the server
// certificate's Subject Alternative Names. Use this when the server
// will be reached via an IP address other than 127.0.0.1 (e.g., a
// Docker bridge gateway IP).
func WithExtraSANIPs(ips ...net.IP) Option {
	return func(c *providerConfig) { c.extraSANIPs = append(c.extraSANIPs, ips...) }
}

// WithIssuerURL overrides the issuer URL reported in discovery and
// embedded in tokens. Use this when the server listens on a different
// address than what external consumers use to reach it (e.g.,
// "https://host.docker.internal:PORT" for Docker reachability).
func WithIssuerURL(url string) Option {
	return func(c *providerConfig) { c.issuerURL = url }
}

// newProvider creates and starts a fake OIDC provider. caDir must be set
// (Start supplies t.TempDir). Callers must call close when finished.
func newProvider(opts ...Option) (*Provider, error) {
	cfg := providerConfig{
		audience:      "fleetshift",
		listenAddress: "127.0.0.1:0",
	}
	for _, o := range opts {
		o(&cfg)
	}

	caCert, caKey, err := generateCA()
	if err != nil {
		return nil, err
	}
	serverCert, serverKey, err := generateServerCert(caCert, caKey, cfg.extraSANIPs)
	if err != nil {
		return nil, err
	}

	signingKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("oidctest: generate ECDSA signing key: %w", err)
	}

	jwkPriv, err := jwk.Import(signingKey)
	if err != nil {
		return nil, fmt.Errorf("oidctest: import private key to JWK: %w", err)
	}
	if err := jwkPriv.Set(jwk.KeyIDKey, "test-kid"); err != nil {
		return nil, fmt.Errorf("oidctest: set key ID: %w", err)
	}

	pubKey, err := jwk.Import(signingKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("oidctest: import public key to JWK: %w", err)
	}
	if err := pubKey.Set(jwk.KeyIDKey, "test-kid"); err != nil {
		return nil, fmt.Errorf("oidctest: set key ID on public key: %w", err)
	}
	if err := pubKey.Set(jwk.AlgorithmKey, jwa.ES256()); err != nil {
		return nil, fmt.Errorf("oidctest: set algorithm on public key: %w", err)
	}

	keySet := jwk.NewSet()
	if err := keySet.AddKey(pubKey); err != nil {
		return nil, fmt.Errorf("oidctest: add public key to set: %w", err)
	}
	jwksJSON, err := json.Marshal(keySet)
	if err != nil {
		return nil, fmt.Errorf("oidctest: marshal JWKS: %w", err)
	}

	caCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCert.Raw,
	})

	if cfg.caDir == "" {
		return nil, fmt.Errorf("oidctest: CA directory is required")
	}
	if err := os.MkdirAll(cfg.caDir, 0o755); err != nil {
		return nil, fmt.Errorf("oidctest: create CA cert dir: %w", err)
	}
	caCertPath := filepath.Join(cfg.caDir, "ca.pem")
	if err := os.WriteFile(caCertPath, caCertPEM, 0o600); err != nil {
		return nil, fmt.Errorf("oidctest: write CA cert: %w", err)
	}

	tlsCert := tls.Certificate{
		Certificate: [][]byte{serverCert.Raw},
		PrivateKey:  serverKey,
	}

	lis, err := net.Listen("tcp", cfg.listenAddress)
	if err != nil {
		return nil, fmt.Errorf("oidctest: listen on %s: %w", cfg.listenAddress, err)
	}

	_, port, _ := net.SplitHostPort(lis.Addr().String())
	lisHost, _, _ := net.SplitHostPort(cfg.listenAddress)
	if lisHost == "" || lisHost == "0.0.0.0" {
		lisHost = "127.0.0.1"
	}
	derivedIssuer := fmt.Sprintf("https://%s:%s", lisHost, port)

	issuerURL := cfg.issuerURL
	if issuerURL == "" {
		issuerURL = derivedIssuer
	}

	caPool := x509.NewCertPool()
	caPool.AddCert(caCert)
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs: caPool,
			},
		},
	}

	p := &Provider{
		issuerURL:  issuerURL,
		audience:   cfg.audience,
		caCertPEM:  caCertPEM,
		caCertPath: caCertPath,
		jwkPriv:    jwkPriv,
		jwksJSON:   jwksJSON,
		listener:   lis,
		httpClient: httpClient,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/.well-known/openid-configuration", p.handleDiscovery)
	mux.HandleFunc("/jwks", p.handleJWKS)

	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
	}

	p.server = &http.Server{
		Handler:   mux,
		TLSConfig: tlsConfig,
	}

	tlsListener := tls.NewListener(lis, tlsConfig)
	go p.server.Serve(tlsListener)

	return p, nil
}

// Start creates and starts a fake OIDC provider. The server is stopped
// automatically when the test finishes. CA material lives under t.TempDir.
func Start(t *testing.T, opts ...Option) *Provider {
	t.Helper()
	caDir := t.TempDir()
	opts = append([]Option{func(c *providerConfig) { c.caDir = caDir }}, opts...)
	p, err := newProvider(opts...)
	if err != nil {
		t.Fatalf("oidctest: %v", err)
	}
	t.Cleanup(func() {
		_ = p.close()
	})
	return p
}

// close stops the HTTPS server. It is safe to call more than once.
func (p *Provider) close() error {
	if p.server == nil {
		return nil
	}
	err := p.server.Close()
	p.server = nil
	return err
}

// SetIssuerURL overrides the issuer URL after startup. This is useful
// when the listen port is ephemeral and the Docker-reachable URL can
// only be computed after the server starts.
func (p *Provider) SetIssuerURL(url domain.IssuerURL) {
	p.issuerURL = string(url)
}

// IssuerURL returns the OIDC issuer URL.
func (p *Provider) IssuerURL() domain.IssuerURL { return domain.IssuerURL(p.issuerURL) }

// Audience returns the configured audience.
func (p *Provider) Audience() domain.Audience { return domain.Audience(p.audience) }

// Port returns the TCP port the server is listening on.
func (p *Provider) Port() string {
	_, port, _ := net.SplitHostPort(p.listener.Addr().String())
	return port
}

// CACertPEM returns the PEM-encoded CA certificate.
func (p *Provider) CACertPEM() []byte { return p.caCertPEM }

// CACertPath returns the path to a file containing the CA cert PEM.
func (p *Provider) CACertPath() string { return p.caCertPath }

// HTTPClient returns an [http.Client] whose transport trusts the
// provider's self-signed CA.
func (p *Provider) HTTPClient() *http.Client { return p.httpClient }

// OIDCConfig returns a [domain.OIDCConfig] pre-filled with the
// provider's issuer, audience, and endpoint URLs. Authorization and
// token endpoint URLs are present for config completeness only; this
// provider does not implement those HTTP handlers.
func (p *Provider) OIDCConfig() domain.OIDCConfig {
	return domain.OIDCConfig{
		IssuerURL:             domain.IssuerURL(p.issuerURL),
		Audience:              domain.Audience(p.audience),
		JWKSURI:               domain.EndpointURL(p.issuerURL + "/jwks"),
		AuthorizationEndpoint: domain.EndpointURL(p.issuerURL + "/authorize"),
		TokenEndpoint:         domain.EndpointURL(p.issuerURL + "/token"),
	}
}

// issue creates a signed JWT with the given claims.
func (p *Provider) issue(claims TokenClaims) (string, error) {
	sub := claims.Subject
	if sub == "" {
		sub = "test-user"
	}
	expiry := claims.Expiry
	if expiry == 0 {
		expiry = time.Hour
	}

	aud := p.audience
	if claims.Audience != "" {
		aud = claims.Audience
	}

	builder := jwt.NewBuilder().
		Subject(sub).
		Issuer(p.issuerURL).
		Audience([]string{aud}).
		IssuedAt(time.Now()).
		Expiration(time.Now().Add(expiry))

	if claims.Email != "" {
		builder = builder.Claim("email", claims.Email)
	}
	if len(claims.Groups) > 0 {
		builder = builder.Claim("groups", claims.Groups)
	}
	for k, v := range claims.Extra {
		builder = builder.Claim(k, v)
	}

	tok, err := builder.Build()
	if err != nil {
		return "", fmt.Errorf("oidctest: build token: %w", err)
	}

	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.ES256(), p.jwkPriv))
	if err != nil {
		return "", fmt.Errorf("oidctest: sign token: %w", err)
	}
	return string(signed), nil
}

// IssueToken creates a signed JWT with the given claims.
func (p *Provider) IssueToken(t *testing.T, claims TokenClaims) string {
	t.Helper()
	token, err := p.issue(claims)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return token
}

// handleDiscovery serves OIDC discovery JSON. Authorization and token
// endpoints are advertised for config completeness but are not served.
func (p *Provider) handleDiscovery(w http.ResponseWriter, _ *http.Request) {
	doc := map[string]string{
		"issuer":                 p.issuerURL,
		"jwks_uri":               p.issuerURL + "/jwks",
		"authorization_endpoint": p.issuerURL + "/authorize",
		"token_endpoint":         p.issuerURL + "/token",
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(doc)
}

// handleJWKS serves the provider's public signing keys as JWKS.
func (p *Provider) handleJWKS(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(p.jwksJSON)
}

// generateCA creates a short-lived self-signed CA certificate and key.
func generateCA() (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("oidctest: generate CA key: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "oidctest-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("oidctest: create CA certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, fmt.Errorf("oidctest: parse CA certificate: %w", err)
	}

	return cert, key, nil
}

// generateServerCert creates a server certificate signed by caCert,
// including localhost DNS names and optional extra IP SANs.
func generateServerCert(caCert *x509.Certificate, caKey *ecdsa.PrivateKey, extraIPs []net.IP) (*x509.Certificate, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("oidctest: generate server key: %w", err)
	}

	ips := []net.IP{net.IPv4(127, 0, 0, 1)}
	ips = append(ips, extraIPs...)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "oidctest-server"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost", "host.docker.internal", "host.containers.internal"},
		IPAddresses:  ips,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, fmt.Errorf("oidctest: create server certificate: %w", err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		return nil, nil, fmt.Errorf("oidctest: parse server certificate: %w", err)
	}

	return cert, key, nil
}
