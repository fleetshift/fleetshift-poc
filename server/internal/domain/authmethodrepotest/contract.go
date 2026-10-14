// Package authmethodrepotest provides contract tests for [domain.AuthMethodRepository]
// implementations.
package authmethodrepotest

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.AuthMethodRepository] for each test invocation.
type Factory func(t *testing.T) domain.AuthMethodRepository

// Run exercises the [domain.AuthMethodRepository] contract.
func Run(t *testing.T, factory Factory) {
	t.Run("SaveAndGet", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		method := domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{
			ID:   "oidc-1",
			Type: domain.AuthMethodTypeOIDC,
			OIDC: &domain.OIDCConfig{
				IssuerURL:             "https://issuer.example.com",
				Audience:              "my-audience",
				JWKSURI:               "https://issuer.example.com/.well-known/jwks.json",
				AuthorizationEndpoint: "https://issuer.example.com/authorize",
				TokenEndpoint:         "https://issuer.example.com/token",
			},
		})

		if err := repo.Save(ctx, method); err != nil {
			t.Fatalf("Save: %v", err)
		}

		got, err := repo.Get(ctx, "oidc-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.ID() != "oidc-1" {
			t.Errorf("ID = %q, want %q", got.ID(), "oidc-1")
		}
		if got.Type() != domain.AuthMethodTypeOIDC {
			t.Errorf("Type = %q, want %q", got.Type(), domain.AuthMethodTypeOIDC)
		}
		if got.OIDC() == nil {
			t.Fatal("OIDC config is nil")
		}
		if got.OIDC().IssuerURL != "https://issuer.example.com" {
			t.Errorf("OIDC.IssuerURL = %q, want %q", got.OIDC().IssuerURL, "https://issuer.example.com")
		}
		if got.OIDC().Audience != "my-audience" {
			t.Errorf("OIDC.Audience = %q, want %q", got.OIDC().Audience, "my-audience")
		}
		if got.OIDC().JWKSURI != "https://issuer.example.com/.well-known/jwks.json" {
			t.Errorf("OIDC.JWKSURI = %q, want %q", got.OIDC().JWKSURI, "https://issuer.example.com/.well-known/jwks.json")
		}
		if got.OIDC().AuthorizationEndpoint != "https://issuer.example.com/authorize" {
			t.Errorf("OIDC.AuthorizationEndpoint = %q, want %q", got.OIDC().AuthorizationEndpoint, "https://issuer.example.com/authorize")
		}
		if got.OIDC().TokenEndpoint != "https://issuer.example.com/token" {
			t.Errorf("OIDC.TokenEndpoint = %q, want %q", got.OIDC().TokenEndpoint, "https://issuer.example.com/token")
		}
	})

	t.Run("SaveUpsert", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()
		method := domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{
			ID:   "oidc-1",
			Type: domain.AuthMethodTypeOIDC,
			OIDC: &domain.OIDCConfig{
				IssuerURL:             "https://issuer.example.com",
				Audience:              "original-audience",
				JWKSURI:               "https://issuer.example.com/jwks",
				AuthorizationEndpoint: "https://issuer.example.com/authorize",
				TokenEndpoint:         "https://issuer.example.com/token",
			},
		})

		if err := repo.Save(ctx, method); err != nil {
			t.Fatalf("first Save: %v", err)
		}

		snap := method.Snapshot()
		snap.OIDC.Audience = "updated-audience"
		snap.OIDC.JWKSURI = "https://issuer.example.com/updated-jwks"
		method = domain.AuthMethodFromSnapshot(snap)
		if err := repo.Save(ctx, method); err != nil {
			t.Fatalf("second Save (upsert): %v", err)
		}

		got, err := repo.Get(ctx, "oidc-1")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.OIDC().Audience != "updated-audience" {
			t.Errorf("OIDC.Audience = %q, want %q (update)", got.OIDC().Audience, "updated-audience")
		}
		if got.OIDC().JWKSURI != "https://issuer.example.com/updated-jwks" {
			t.Errorf("OIDC.JWKSURI = %q, want %q (update)", got.OIDC().JWKSURI, "https://issuer.example.com/updated-jwks")
		}
	})

	t.Run("GetNotFound", func(t *testing.T) {
		repo := factory(t)
		_, err := repo.Get(context.Background(), "nonexistent")
		if !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("Get: got %v, want ErrNotFound", err)
		}
	})

	t.Run("List", func(t *testing.T) {
		repo := factory(t)
		ctx := context.Background()

		methods := []domain.AuthMethod{
			domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{
				ID:   "oidc-1",
				Type: domain.AuthMethodTypeOIDC,
				OIDC: &domain.OIDCConfig{IssuerURL: "https://a.example.com", Audience: "a"},
			}),
			domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{
				ID:   "oidc-2",
				Type: domain.AuthMethodTypeOIDC,
				OIDC: &domain.OIDCConfig{IssuerURL: "https://b.example.com", Audience: "b"},
			}),
		}
		for _, m := range methods {
			if err := repo.Save(ctx, m); err != nil {
				t.Fatalf("Save %s: %v", m.ID(), err)
			}
		}

		got, err := repo.List(ctx)
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 2 {
			t.Fatalf("List: got %d, want 2", len(got))
		}
	})

	t.Run("ListEmpty", func(t *testing.T) {
		repo := factory(t)
		got, err := repo.List(context.Background())
		if err != nil {
			t.Fatalf("List: %v", err)
		}
		if len(got) != 0 {
			t.Fatalf("List: got %d items, want empty slice", len(got))
		}
	})
}
