// Package oidctest provides a fake OIDC identity provider for testing.
// It generates real cryptographic keys, issues signed JWTs, and serves
// standard OIDC discovery and JWKS endpoints over HTTPS with a
// self-signed CA.
//
// The provider is reusable across unit tests (in-process TLS) and
// integration tests where external consumers like the K8s API server
// need to reach it over real HTTPS.
package oidctest

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
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
	signingKey *rsa.PrivateKey
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

type providerConfig struct {
	audience      string
	listenAddress string
	issuerURL     string   // override; empty means derive from listen address
	extraSANIPs   []net.IP // additional IP SANs for the server certificate
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

// Start creates and starts a fake OIDC provider. The server is stopped
// automatically when the test finishes.
func Start(t *testing.T, opts ...Option) *Provider {
	t.Helper()

	cfg := providerConfig{
		audience:      "fleetshift",
		listenAddress: "127.0.0.1:0",
	}
	for _, o := range opts {
		o(&cfg)
	}

	caCert, caKey := generateCA(t)
	serverCert, serverKey := generateServerCert(t, caCert, caKey, cfg.extraSANIPs)

	signingKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("oidctest: generate RSA signing key: %v", err)
	}

	jwkPriv, err := jwk.Import(signingKey)
	if err != nil {
		t.Fatalf("oidctest: import private key to JWK: %v", err)
	}
	if err := jwkPriv.Set(jwk.KeyIDKey, "test-kid"); err != nil {
		t.Fatalf("oidctest: set key ID: %v", err)
	}

	pubKey, err := jwk.Import(signingKey.PublicKey)
	if err != nil {
		t.Fatalf("oidctest: import public key to JWK: %v", err)
	}
	if err := pubKey.Set(jwk.KeyIDKey, "test-kid"); err != nil {
		t.Fatalf("oidctest: set key ID on public key: %v", err)
	}
	if err := pubKey.Set(jwk.AlgorithmKey, jwa.RS256()); err != nil {
		t.Fatalf("oidctest: set algorithm on public key: %v", err)
	}

	keySet := jwk.NewSet()
	keySet.AddKey(pubKey)
	jwksJSON, err := json.Marshal(keySet)
	if err != nil {
		t.Fatalf("oidctest: marshal JWKS: %v", err)
	}

	caCertPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: caCert.Raw,
	})

	caCertFile, err := os.CreateTemp(t.TempDir(), "oidc-ca-*.pem")
	if err != nil {
		t.Fatalf("oidctest: create CA cert temp file: %v", err)
	}
	if _, err := caCertFile.Write(caCertPEM); err != nil {
		t.Fatalf("oidctest: write CA cert: %v", err)
	}
	caCertFile.Close()

	tlsCert := tls.Certificate{
		Certificate: [][]byte{serverCert.Raw},
		PrivateKey:  serverKey,
	}

	lis, err := net.Listen("tcp", cfg.listenAddress)
	if err != nil {
		t.Fatalf("oidctest: listen on %s: %v", cfg.listenAddress, err)
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
		caCertPath: caCertFile.Name(),
		signingKey: signingKey,
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

	t.Cleanup(func() {
		p.server.Close()
	})

	return p
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

// CACertPath returns the path to a temp file containing the CA cert PEM.
// The file is cleaned up when the test finishes.
func (p *Provider) CACertPath() string { return p.caCertPath }

// HTTPClient returns an [http.Client] whose transport trusts the
// provider's self-signed CA.
func (p *Provider) HTTPClient() *http.Client { return p.httpClient }

// OIDCConfig returns a [domain.OIDCConfig] pre-filled with the
// provider's issuer, audience, and endpoint URLs.
func (p *Provider) OIDCConfig() domain.OIDCConfig {
	return domain.OIDCConfig{
		IssuerURL:             domain.IssuerURL(p.issuerURL),
		Audience:              domain.Audience(p.audience),
		JWKSURI:               domain.EndpointURL(p.issuerURL + "/jwks"),
		AuthorizationEndpoint: domain.EndpointURL(p.issuerURL + "/authorize"),
		TokenEndpoint:         domain.EndpointURL(p.issuerURL + "/token"),
	}
}

// IssueToken creates a signed JWT with the given claims.
func (p *Provider) IssueToken(t *testing.T, claims TokenClaims) string {
	t.Helper()

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
		t.Fatalf("oidctest: build token: %v", err)
	}

	signed, err := jwt.Sign(tok, jwt.WithKey(jwa.RS256(), p.jwkPriv))
	if err != nil {
		t.Fatalf("oidctest: sign token: %v", err)
	}
	return string(signed)
}

func (p *Provider) handleDiscovery(w http.ResponseWriter, _ *http.Request) {
	doc := map[string]string{
		"issuer":                 p.issuerURL,
		"jwks_uri":              p.issuerURL + "/jwks",
		"authorization_endpoint": p.issuerURL + "/authorize",
		"token_endpoint":         p.issuerURL + "/token",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(doc)
}

func (p *Provider) handleJWKS(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Write(p.jwksJSON)
}

func generateCA(t *testing.T) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("oidctest: generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "oidctest-ca"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:         true,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("oidctest: create CA certificate: %v", err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("oidctest: parse CA certificate: %v", err)
	}

	return cert, key
}

func generateServerCert(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey, extraIPs []net.IP) (*x509.Certificate, *ecdsa.PrivateKey) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("oidctest: generate server key: %v", err)
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
		DNSNames:     []string{"localhost", "host.docker.internal"},
		IPAddresses:  ips,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("oidctest: create server certificate: %v", err)
	}

	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("oidctest: parse server certificate: %v", err)
	}

	return cert, key
}
