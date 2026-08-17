package oidc_test

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
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

func TestDiscoveryClient_RejectsMismatchedIssuer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"issuer":"https://other.example/dex",
			"authorization_endpoint":"https://other.example/dex/auth",
			"token_endpoint":"https://other.example/dex/token",
			"jwks_uri":"https://other.example/dex/keys"
		}`))
	}))
	t.Cleanup(srv.Close)

	dc := oidc.NewDiscoveryClient(srv.Client())
	_, err := dc.FetchMetadata(context.Background(), domain.IssuerURL(srv.URL))
	if err == nil || !strings.Contains(err.Error(), "does not match requested issuer") {
		t.Fatalf("FetchMetadata() = %v, want mismatched issuer", err)
	}
}

func TestDiscoveryClient_RejectsIncompleteMetadata(t *testing.T) {
	tests := []struct {
		name string
		body string
		want string
	}{
		{
			name: "missing authorization_endpoint",
			body: `{"issuer":%q,"token_endpoint":"https://example/token","jwks_uri":"https://example/keys"}`,
			want: "authorization_endpoint",
		},
		{
			name: "missing token_endpoint",
			body: `{"issuer":%q,"authorization_endpoint":"https://example/auth","jwks_uri":"https://example/keys"}`,
			want: "token_endpoint",
		},
		{
			name: "missing jwks_uri",
			body: `{"issuer":%q,"authorization_endpoint":"https://example/auth","token_endpoint":"https://example/token"}`,
			want: "jwks_uri",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var issuer string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(w, tt.body, issuer)
			}))
			t.Cleanup(srv.Close)
			issuer = srv.URL
			dc := oidc.NewDiscoveryClient(srv.Client())
			_, err := dc.FetchMetadata(context.Background(), domain.IssuerURL(issuer))
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("FetchMetadata() = %v, want %q", err, tt.want)
			}
		})
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
	if claims.Subject != "user-456" {
		t.Errorf("Subject = %q, want %q", claims.Subject, "user-456")
	}
}
