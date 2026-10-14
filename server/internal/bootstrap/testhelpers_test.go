package bootstrap

import (
	"context"
	"log/slog"
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

func (testVerifier) RegisterJWKS(context.Context, domain.EndpointURL) error { return nil }

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

func startTestServer(t *testing.T, opts ...Option) *Server {
	t.Helper()
	return startTestServerWithConfig(t, ConfigInput{
		GRPCAddr:             "127.0.0.1:0",
		HTTPAddr:             "127.0.0.1:0",
		DBPath:               filepath.Join(t.TempDir(), "fleetshift.db"),
		OIDCIssuer:           "https://test-issuer.example",
		OIDCResourceAudience: "fleetshift",
	}, opts...)
}

func startTestServerWithConfig(t *testing.T, in ConfigInput, opts ...Option) *Server {
	t.Helper()
	if in.GRPCAddr == "" {
		in.GRPCAddr = "127.0.0.1:0"
	}
	if in.HTTPAddr == "" {
		in.HTTPAddr = "127.0.0.1:0"
	}
	if in.DBPath == "" {
		in.DBPath = filepath.Join(t.TempDir(), "fleetshift.db")
	}
	// Fail-closed Start requires complete OIDC initial-AuthMethod config when the
	// AuthMethod store is empty. Tests that intentionally omit it must call
	// Start directly.
	if in.OIDCIssuer == "" {
		in.OIDCIssuer = "https://test-issuer.example"
	}
	if in.OIDCResourceAudience == "" {
		in.OIDCResourceAudience = "fleetshift"
	}
	cfg, err := NewConfig(in)
	if err != nil {
		t.Fatalf("NewConfig: %v", err)
	}

	base := []Option{
		WithWorkflowRegistry(NewMemWorkflowRegistry()),
		WithOIDCDeps(OIDCDeps{Discovery: testDiscovery{}, Verifier: testVerifier{}}),
		WithAddonAssembly(func(context.Context, AddonDeps) ([]AddonSpec, error) {
			return nil, nil
		}),
	}
	base = append(base, opts...)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	srv, err := Start(ctx, cfg, testLogger(), base...)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	t.Cleanup(func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_ = srv.Close(closeCtx)
	})
	return srv
}
