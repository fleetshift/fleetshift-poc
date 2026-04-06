package oidc_test

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
)

func TestDiscoveryClient_CustomCA(t *testing.T) {
	idp := oidctest.Start(t)

	// Use the provider's HTTPClient which trusts its self-signed CA.
	dc := oidc.NewDiscoveryClient(idp.HTTPClient())

	meta, err := dc.FetchMetadata(context.Background(), idp.IssuerURL())
	if err != nil {
		t.Fatalf("FetchMetadata with custom CA: %v", err)
	}

	if meta.Issuer != idp.IssuerURL() {
		t.Errorf("Issuer = %q, want %q", meta.Issuer, idp.IssuerURL())
	}
	if meta.JWKSURI == "" {
		t.Error("JWKSURI is empty")
	}
}

func TestDiscoveryClient_RejectsWithoutCA(t *testing.T) {
	idp := oidctest.Start(t)

	// Default client has no custom CA — should fail on TLS.
	dc := oidc.NewDiscoveryClient(nil)

	_, err := dc.FetchMetadata(context.Background(), idp.IssuerURL())
	if err == nil {
		t.Fatal("FetchMetadata should fail without CA for self-signed server")
	}
}

func TestVerifier_WithHTTPClient_SelfSignedCA(t *testing.T) {
	ctx := context.Background()
	idp := oidctest.Start(t, oidctest.WithAudience("test-audience"))

	// This tests the WithHTTPClient option — the same path used by
	// serve.go when --oidc-ca-file is provided.
	verifier, err := oidc.NewVerifier(ctx, oidc.WithHTTPClient(idp.HTTPClient()))
	if err != nil {
		t.Fatalf("NewVerifier with custom client: %v", err)
	}

	token := idp.IssueToken(t, oidctest.TokenClaims{Subject: "user-456"})
	claims, err := verifier.Verify(ctx, idp.OIDCConfig(), token)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if claims.ID != "user-456" {
		t.Errorf("ID = %q, want %q", claims.ID, "user-456")
	}
}

