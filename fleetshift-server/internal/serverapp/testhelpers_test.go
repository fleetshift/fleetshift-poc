package serverapp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

type testVerifier struct{}

func (testVerifier) Verify(context.Context, domain.OIDCConfig, string) (domain.SubjectClaims, error) {
	return domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{Subject: "test", Issuer: "test"},
	}, nil
}

func (testVerifier) RegisterKeySet(context.Context, domain.EndpointURL) error { return nil }

type testDiscovery struct{}

func (testDiscovery) FetchMetadata(_ context.Context, issuerURL domain.IssuerURL) (domain.OIDCMetadata, error) {
	return domain.OIDCMetadata{
		Issuer:                issuerURL,
		AuthorizationEndpoint: domain.EndpointURL(string(issuerURL) + "/authorize"),
		TokenEndpoint:         domain.EndpointURL(string(issuerURL) + "/token"),
		JWKSURI:               domain.EndpointURL(string(issuerURL) + "/jwks"),
	}, nil
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
}

// mustTestCAPEM returns a short-lived self-signed CA certificate in PEM form
// for tests that need non-empty OIDC CA bundle input.
func mustTestCAPEM(t *testing.T) []byte {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "fleetshift-test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

func startTestApp(t *testing.T, opts ...Option) *App {
	t.Helper()
	cfg, err := NewConfig(ConfigInput{
		GRPCAddr: "127.0.0.1:0",
		HTTPAddr: "127.0.0.1:0",
		DBPath:   filepath.Join(t.TempDir(), "fleetshift.db"),
	})
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	base := []Option{
		WithWorkflowRuntime(NewMemWorkflowRuntime()),
		WithIdentity(Identity{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) {
			return nil, nil
		}),
	}
	base = append(base, opts...)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	app, err := Start(ctx, cfg, testLogger(), base...)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = app.Close(closeCtx)
	})
	return app
}
