package oidc_test

import (
	"context"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
)

func TestVerifier_ValidToken(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	rawToken := idp.IssueToken(t, oidctest.TokenClaims{Subject: "user-123"})

	claims, err := verifier.Verify(ctx, idp.OIDCConfig(), rawToken)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if claims.Subject != "user-123" {
		t.Errorf("Subject: got %q, want %q", claims.Subject, "user-123")
	}
	if claims.Issuer != idp.IssuerURL() {
		t.Errorf("Issuer: got %q, want %q", claims.Issuer, idp.IssuerURL())
	}
}

func TestVerifier_ExpiredToken(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	rawToken := idp.IssueToken(t, oidctest.TokenClaims{
		Subject: "user-123",
		Expiry:  -time.Hour,
	})

	_, err = verifier.Verify(ctx, idp.OIDCConfig(), rawToken)
	if err == nil {
		t.Fatal("Verify: expected error for expired token, got nil")
	}
}

func TestVerifier_WrongIssuer(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	// Issue a token, then verify against a config with a different issuer.
	rawToken := idp.IssueToken(t, oidctest.TokenClaims{Subject: "user-123"})

	config := idp.OIDCConfig()
	config.IssuerURL = "https://wrong-issuer"

	_, err = verifier.Verify(ctx, config, rawToken)
	if err == nil {
		t.Fatal("Verify: expected error for wrong issuer, got nil")
	}
}

func TestVerifier_WrongAudience(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	rawToken := idp.IssueToken(t, oidctest.TokenClaims{Subject: "user-123"})

	config := domain.OIDCConfig{
		IssuerURL: idp.IssuerURL(),
		Audience:  "wrong-audience",
		JWKSURI:   idp.OIDCConfig().JWKSURI,
	}

	_, err = verifier.Verify(ctx, config, rawToken)
	if err == nil {
		t.Fatal("Verify: expected error for wrong audience, got nil")
	}
}

