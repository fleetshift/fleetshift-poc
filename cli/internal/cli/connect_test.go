package cli

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/auth"
)

func TestValidateTransportFlags(t *testing.T) {
	tests := []struct {
		name  string
		flags globalFlags
		want  string
	}{
		{
			name:  "ca file requires tls",
			flags: globalFlags{serverCAFile: "/tmp/ca.pem"},
			want:  "--server-ca-file requires --server-tls",
		},
		{
			name:  "insecure requires tls",
			flags: globalFlags{serverInsecure: true},
			want:  "--server-insecure requires --server-tls",
		},
		{
			name:  "tls flags valid together",
			flags: globalFlags{serverTLS: true, serverCAFile: "/tmp/ca.pem", serverInsecure: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTransportFlags(tt.flags)
			if tt.want == "" {
				if err != nil {
					t.Fatalf("validateTransportFlags() unexpected error: %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("validateTransportFlags() expected error containing %q, got nil", tt.want)
			}
			if !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("validateTransportFlags() error = %q, want substring %q", err.Error(), tt.want)
			}
		})
	}
}

func TestDialPlaintext(t *testing.T) {
	addr := startPlaintextTestServer(t)

	conn, err := dial(globalFlags{server: addr})
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	defer conn.Close()

	assertHealthServing(t, conn)
}

func TestDialPlaintext_HomeAuthJSONWithMissingOIDCCAFile(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	if err := auth.SaveConfig(auth.Config{
		ClientID:              "fleetshift-cli",
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		OIDCCAFile:            "ca.crt",
	}); err != nil {
		t.Fatalf("SaveConfig: %v", err)
	}

	addr := startPlaintextTestServer(t)
	conn, err := dial(globalFlags{server: addr})
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	defer conn.Close()

	assertHealthServing(t, conn)
}

func TestDialTLSWithCAFile(t *testing.T) {
	addr, caFile := startTLSTestServer(t)

	conn, err := dial(globalFlags{
		server:       addr,
		serverTLS:    true,
		serverCAFile: caFile,
	})
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	defer conn.Close()

	assertHealthServing(t, conn)
}

func TestDialTLSInsecure(t *testing.T) {
	addr, _ := startTLSTestServer(t)

	conn, err := dial(globalFlags{
		server:         addr,
		serverTLS:      true,
		serverInsecure: true,
	})
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	defer conn.Close()

	assertHealthServing(t, conn)
}

func TestDialTLSCAFileNotFound(t *testing.T) {
	_, err := dial(globalFlags{
		server:       "localhost:0",
		serverTLS:    true,
		serverCAFile: "/no/such/file.pem",
	})
	if err == nil {
		t.Fatal("dial() expected error for missing CA file, got nil")
	}
	if !strings.Contains(err.Error(), "read server CA file") {
		t.Fatalf("dial() error = %q, want substring %q", err.Error(), "read server CA file")
	}
}

func TestDialTLSCAFileInvalidPEM(t *testing.T) {
	caFile := filepath.Join(t.TempDir(), "bad.pem")
	if err := os.WriteFile(caFile, []byte("not a cert"), 0o600); err != nil {
		t.Fatalf("write bad CA file: %v", err)
	}

	_, err := dial(globalFlags{
		server:       "localhost:0",
		serverTLS:    true,
		serverCAFile: caFile,
	})
	if err == nil {
		t.Fatal("dial() expected error for invalid PEM, got nil")
	}
	if !strings.Contains(err.Error(), "no certificates found") {
		t.Fatalf("dial() error = %q, want substring %q", err.Error(), "no certificates found")
	}
}

func TestTokenCredentials_GetRequestMetadata_MissingConfig(t *testing.T) {
	creds := &tokenCredentials{
		store:     &auth.InMemoryTokenStore{},
		configDir: t.TempDir(),
	}
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata() error = %v, want nil", err)
	}
	if md != nil {
		t.Fatalf("metadata = %v, want nil when auth.json is missing", md)
	}
}

func TestTokenCredentials_GetRequestMetadata_ValidToken(t *testing.T) {
	configDir := t.TempDir()
	if err := auth.SaveConfigTo(configDir, auth.Config{
		ClientID:              "fleetshift-cli",
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
	}); err != nil {
		t.Fatalf("SaveConfigTo: %v", err)
	}
	store := &auth.InMemoryTokenStore{}
	if err := store.Save(context.Background(), auth.Tokens{
		AccessToken: "live-access",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	creds := &tokenCredentials{store: store, configDir: configDir}
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata() error = %v", err)
	}
	if got := md["authorization"]; got != "Bearer live-access" {
		t.Fatalf("authorization = %q, want Bearer live-access", got)
	}
}

func TestTokenCredentials_GetRequestMetadata_ExpiredTokenWithoutRefresh_OmitsMetadata(t *testing.T) {
	configDir := t.TempDir()
	if err := auth.SaveConfigTo(configDir, auth.Config{
		ClientID:              "fleetshift-cli",
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
	}); err != nil {
		t.Fatalf("SaveConfigTo: %v", err)
	}
	store := &auth.InMemoryTokenStore{}
	if err := store.Save(context.Background(), auth.Tokens{
		AccessToken: "expired-access",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	creds := &tokenCredentials{store: store, configDir: configDir}
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata() error = %v, want nil", err)
	}
	if md != nil {
		t.Fatalf("metadata = %v, want nil for an expired access token with no refresh token", md)
	}
}

func TestTokenCredentials_GetRequestMetadata_ZeroExpiryStillAttaches(t *testing.T) {
	configDir := t.TempDir()
	if err := auth.SaveConfigTo(configDir, auth.Config{
		ClientID:              "fleetshift-cli",
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
	}); err != nil {
		t.Fatalf("SaveConfigTo: %v", err)
	}
	store := &auth.InMemoryTokenStore{}
	if err := store.Save(context.Background(), auth.Tokens{
		AccessToken: "unexpiring-access",
		TokenType:   "Bearer",
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	creds := &tokenCredentials{store: store, configDir: configDir}
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata() error = %v", err)
	}
	if got := md["authorization"]; got != "Bearer unexpiring-access" {
		t.Fatalf("authorization = %q, want Bearer unexpiring-access", got)
	}
}

func TestTokenCredentials_GetRequestMetadata_ValidToken_MissingOIDCCAFile(t *testing.T) {
	configDir := t.TempDir()
	if err := auth.SaveConfigTo(configDir, auth.Config{
		ClientID:              "fleetshift-cli",
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		OIDCCAFile:            "ca.crt",
	}); err != nil {
		t.Fatalf("SaveConfigTo: %v", err)
	}
	store := &auth.InMemoryTokenStore{}
	if err := store.Save(context.Background(), auth.Tokens{
		AccessToken: "live-access",
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	creds := &tokenCredentials{store: store, configDir: configDir}
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata() error = %v, want nil when the token is still valid", err)
	}
	if got := md["authorization"]; got != "Bearer live-access" {
		t.Fatalf("authorization = %q, want Bearer live-access", got)
	}
}

func TestTokenCredentials_GetRequestMetadata_Refresh_MissingOIDCCAFile_Swallowed(t *testing.T) {
	configDir := t.TempDir()
	if err := auth.SaveConfigTo(configDir, auth.Config{
		ClientID:              "fleetshift-cli",
		AuthorizationEndpoint: "https://issuer.example/auth",
		TokenEndpoint:         "https://issuer.example/token",
		OIDCCAFile:            "ca.crt",
	}); err != nil {
		t.Fatalf("SaveConfigTo: %v", err)
	}
	store := &auth.InMemoryTokenStore{}
	if err := store.Save(context.Background(), auth.Tokens{
		AccessToken:  "expired-access",
		RefreshToken: "refresh",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	creds := &tokenCredentials{store: store, configDir: configDir}
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata() error = %v, want nil (missing OIDC CA is swallowed)", err)
	}
	if md != nil {
		t.Fatalf("metadata = %v, want nil when refresh cannot build an OIDC HTTP client", md)
	}
}

func TestTokenCredentials_GetRequestMetadata_RefreshFailureSwallowed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	configDir := t.TempDir()
	if err := auth.SaveConfigTo(configDir, auth.Config{
		ClientID:              "fleetshift-cli",
		AuthorizationEndpoint: srv.URL,
		TokenEndpoint:         srv.URL,
	}); err != nil {
		t.Fatalf("SaveConfigTo: %v", err)
	}
	store := &auth.InMemoryTokenStore{}
	if err := store.Save(context.Background(), auth.Tokens{
		AccessToken:  "expired-access",
		RefreshToken: "refresh",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	creds := &tokenCredentials{store: store, configDir: configDir}
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata() error = %v, want nil (refresh failures are swallowed)", err)
	}
	if md != nil {
		t.Fatalf("metadata = %v, want nil after failed refresh", md)
	}
}

func TestTokenCredentials_GetRequestMetadata_RefreshSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if r.Form.Get("grant_type") != "refresh_token" {
			http.Error(w, "unexpected grant", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"refreshed-access","token_type":"Bearer","expires_in":3600}`)
	}))
	t.Cleanup(srv.Close)

	configDir := t.TempDir()
	if err := auth.SaveConfigTo(configDir, auth.Config{
		ClientID:              "fleetshift-cli",
		AuthorizationEndpoint: srv.URL,
		TokenEndpoint:         srv.URL,
	}); err != nil {
		t.Fatalf("SaveConfigTo: %v", err)
	}
	store := &auth.InMemoryTokenStore{}
	if err := store.Save(context.Background(), auth.Tokens{
		AccessToken:  "expired-access",
		RefreshToken: "refresh",
		TokenType:    "Bearer",
		Expiry:       time.Now().Add(-time.Minute),
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	creds := &tokenCredentials{store: store, configDir: configDir}
	md, err := creds.GetRequestMetadata(context.Background())
	if err != nil {
		t.Fatalf("GetRequestMetadata() error = %v", err)
	}
	if got := md["authorization"]; got != "Bearer refreshed-access" {
		t.Fatalf("authorization = %q, want Bearer refreshed-access", got)
	}
}

func assertHealthServing(t *testing.T, conn *grpc.ClientConn) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Health.Check() error = %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("Health.Check() status = %v, want %v", resp.GetStatus(), healthpb.HealthCheckResponse_SERVING)
	}
}

func startTLSTestServer(t *testing.T) (string, string) {
	t.Helper()

	caKey, caCert := generateTestCA(t)
	serverCert := generateServerCert(t, caCert, caKey)
	caFile := writeCAFile(t, caCert)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer(
		grpc.Creds(credentials.NewTLS(&tls.Config{
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{serverCert},
		})),
	)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthServer)

	go func() {
		_ = srv.Serve(lis)
	}()

	t.Cleanup(func() {
		srv.GracefulStop()
		_ = lis.Close()
	})

	return lis.Addr().String(), caFile
}

func startPlaintextTestServer(t *testing.T) string {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := grpc.NewServer(grpc.Creds(insecure.NewCredentials()))
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthServer)

	go func() {
		_ = srv.Serve(lis)
	}()

	t.Cleanup(func() {
		srv.GracefulStop()
		_ = lis.Close()
	})

	return lis.Addr().String()
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
