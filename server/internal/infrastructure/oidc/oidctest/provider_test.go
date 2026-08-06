package oidctest_test

import (
	"context"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
)

func TestProvider_Discovery(t *testing.T) {
	idp := oidctest.Start(t)

	disc := oidc.NewDiscoveryClient(idp.HTTPClient())
	meta, err := disc.FetchMetadata(context.Background(), idp.IssuerURL())
	if err != nil {
		t.Fatalf("FetchMetadata: %v", err)
	}

	if meta.Issuer != idp.IssuerURL() {
		t.Errorf("Issuer = %q, want %q", meta.Issuer, idp.IssuerURL())
	}
	wantJWKS := domain.EndpointURL(string(idp.IssuerURL()) + "/jwks")
	if meta.JWKSURI != wantJWKS {
		t.Errorf("JWKSURI = %q, want %q", meta.JWKSURI, wantJWKS)
	}
	if meta.AuthorizationEndpoint == "" {
		t.Error("AuthorizationEndpoint is empty")
	}
	if meta.TokenEndpoint == "" {
		t.Error("TokenEndpoint is empty")
	}
}

func TestProvider_IssueAndVerify(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t)

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	token := idp.IssueToken(t, oidctest.TokenClaims{
		Subject: "alice",
		Groups:  []string{"developers", "admins"},
		Email:   "alice@example.com",
	})

	claims, err := verifier.Verify(ctx, idp.OIDCConfig(), token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if claims.Subject != "alice" {
		t.Errorf("Subject = %q, want %q", claims.Subject, "alice")
	}
	if claims.Issuer != idp.IssuerURL() {
		t.Errorf("Issuer = %q, want %q", claims.Issuer, idp.IssuerURL())
	}
	if got := claims.Extra["email"]; len(got) != 1 || got[0] != "alice@example.com" {
		t.Errorf("email = %v, want [alice@example.com]", got)
	}
	if got := claims.Extra["groups"]; len(got) != 2 || got[0] != "developers" || got[1] != "admins" {
		t.Errorf("groups = %v, want [developers admins]", got)
	}
}

func TestProvider_ExpiredTokenRejected(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t)

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	token := idp.IssueToken(t, oidctest.TokenClaims{
		Subject: "alice",
		Expiry:  -time.Hour,
	})

	_, err = verifier.Verify(ctx, idp.OIDCConfig(), token)
	if err == nil {
		t.Fatal("Verify: expected error for expired token, got nil")
	}
}

func TestProvider_WrongAudienceRejected(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t)

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	token := idp.IssueToken(t, oidctest.TokenClaims{Subject: "alice"})

	wrongConfig := domain.OIDCConfig{
		IssuerURL: idp.IssuerURL(),
		Audience:  "wrong-audience",
		JWKSURI:   idp.OIDCConfig().JWKSURI,
	}

	_, err = verifier.Verify(ctx, wrongConfig, token)
	if err == nil {
		t.Fatal("Verify: expected error for wrong audience, got nil")
	}
}

func TestProvider_DefaultSubject(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t)

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	token := idp.IssueToken(t, oidctest.TokenClaims{})

	claims, err := verifier.Verify(ctx, idp.OIDCConfig(), token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if claims.Subject != "test-user" {
		t.Errorf("Subject = %q, want %q", claims.Subject, "test-user")
	}
}

func TestProvider_CustomAudience(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t, oidctest.WithAudience("my-app"))

	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier: %v", err)
	}

	token := idp.IssueToken(t, oidctest.TokenClaims{Subject: "bob"})

	claims, err := verifier.Verify(ctx, idp.OIDCConfig(), token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}

	if claims.Subject != "bob" {
		t.Errorf("Subject = %q, want %q", claims.Subject, "bob")
	}
}
