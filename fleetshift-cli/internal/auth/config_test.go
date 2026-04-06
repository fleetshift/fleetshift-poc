package auth_test

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestConfig_HTTPClient_EmptyCAFile(t *testing.T) {
	cfg := auth.Config{}

	client, err := cfg.HTTPClient()
	if err != nil {
		t.Fatalf("HTTPClient() error: %v", err)
	}
	if client != nil {
		t.Error("HTTPClient() returned non-nil client for empty OIDCCAFile")
	}
}

func TestConfig_HTTPClient_ValidCA(t *testing.T) {
	caKey, caCert := generateTestCA(t)
	caPath := writeCAFile(t, caCert)

	// Start a TLS server using a cert signed by our CA.
	serverCert := generateServerCert(t, caCert, caKey)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{serverCert}}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	cfg := auth.Config{OIDCCAFile: caPath}
	client, err := cfg.HTTPClient()
	if err != nil {
		t.Fatalf("HTTPClient() error: %v", err)
	}
	if client == nil {
		t.Fatal("HTTPClient() returned nil for valid CA file")
	}

	// The client should be able to reach the self-signed TLS server.
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET %s with custom CA client: %v", srv.URL, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestConfig_HTTPClient_MissingFile(t *testing.T) {
	cfg := auth.Config{OIDCCAFile: "/nonexistent/ca.pem"}

	_, err := cfg.HTTPClient()
	if err == nil {
		t.Fatal("HTTPClient() expected error for missing file, got nil")
	}
}

func TestConfig_JSONRoundTrip_WithOIDCCAFile(t *testing.T) {
	original := auth.Config{
		IssuerURL:             "https://issuer.example.com",
		ClientID:              "my-client",
		Scopes:                []string{"openid", "profile"},
		AuthorizationEndpoint: "https://issuer.example.com/authorize",
		TokenEndpoint:         "https://issuer.example.com/token",
		KeyEnrollmentClientID: "enroll-client",
		OIDCCAFile:            "/etc/pki/oidc-ca.pem",
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var decoded auth.Config
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if decoded.OIDCCAFile != original.OIDCCAFile {
		t.Errorf("OIDCCAFile = %q, want %q", decoded.OIDCCAFile, original.OIDCCAFile)
	}
	if decoded.IssuerURL != original.IssuerURL {
		t.Errorf("IssuerURL = %q, want %q", decoded.IssuerURL, original.IssuerURL)
	}
	if decoded.ClientID != original.ClientID {
		t.Errorf("ClientID = %q, want %q", decoded.ClientID, original.ClientID)
	}
}

func TestConfig_JSONRoundTrip_OmitsEmptyOIDCCAFile(t *testing.T) {
	cfg := auth.Config{
		IssuerURL: "https://issuer.example.com",
		ClientID:  "my-client",
		Scopes:    []string{"openid"},
	}

	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal raw: %v", err)
	}

	if _, exists := raw["oidc_ca_file"]; exists {
		t.Error("oidc_ca_file should be omitted when empty (omitempty)")
	}
}

func generateTestCA(t *testing.T) (*ecdsa.PrivateKey, *x509.Certificate) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		IsCA:                  true,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return key, cert
}

func generateServerCert(t *testing.T, caCert *x509.Certificate, caKey *ecdsa.PrivateKey) tls.Certificate {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}

	return tls.Certificate{
		Certificate: [][]byte{certDER},
		PrivateKey:  key,
	}
}

func writeCAFile(t *testing.T, cert *x509.Certificate) string {
	t.Helper()

	p := filepath.Join(t.TempDir(), "ca.pem")
	pemData := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Raw,
	})
	if err := os.WriteFile(p, pemData, 0o600); err != nil {
		t.Fatalf("write CA file: %v", err)
	}
	return p
}
