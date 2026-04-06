package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func TestSigningKeyBindingRepo_CreateAndGet(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	ctx := context.Background()

	binding := domain.SigningKeyBinding{
		ID: "skb-1",
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "user-1",
			Issuer:  "https://issuer.example.com",
		},
		PublicKeyJWK:        []byte(`{"kty":"EC","crv":"P-256","x":"x","y":"y"}`),
		Algorithm:           "ES256",
		KeyBindingDoc:       []byte(`{"subject":"user-1"}`),
		KeyBindingSignature: []byte("signature-bytes"),
		IdentityToken:       "id-token-value",
		CreatedAt:           time.Date(2026, 3, 11, 0, 0, 0, 0, time.UTC),
		ExpiresAt:           time.Date(2027, 3, 11, 0, 0, 0, 0, time.UTC),
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.SigningKeyBindings().Create(ctx, binding); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tx, err = store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.Rollback()

	got, err := tx.SigningKeyBindings().Get(ctx, "skb-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.ID != "skb-1" {
		t.Errorf("ID = %q, want %q", got.ID, "skb-1")
	}
	if got.Subject != "user-1" {
		t.Errorf("Subject = %q, want %q", got.Subject, "user-1")
	}
	if got.Issuer != "https://issuer.example.com" {
		t.Errorf("Issuer = %q, want %q", got.Issuer, "https://issuer.example.com")
	}
	if got.Algorithm != "ES256" {
		t.Errorf("Algorithm = %q, want %q", got.Algorithm, "ES256")
	}
	if string(got.IdentityToken) != "id-token-value" {
		t.Errorf("IdentityToken = %q, want %q", got.IdentityToken, "id-token-value")
	}
}

func TestSigningKeyBindingRepo_GetNotFound(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	ctx := context.Background()

	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.Rollback()

	_, err = tx.SigningKeyBindings().Get(ctx, "nonexistent")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestSigningKeyBindingRepo_CreateDuplicate(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	ctx := context.Background()

	binding := domain.SigningKeyBinding{
		ID: "skb-dup",
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "user-1",
			Issuer:  "https://issuer.example.com",
		},
		PublicKeyJWK:        []byte(`{}`),
		Algorithm:           "ES256",
		KeyBindingDoc:       []byte(`{}`),
		KeyBindingSignature: []byte("sig"),
		IdentityToken:       "tok",
		CreatedAt:           time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:           time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.SigningKeyBindings().Create(ctx, binding); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tx, err = store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()

	err = tx.SigningKeyBindings().Create(ctx, binding)
	if !errors.Is(err, domain.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got: %v", err)
	}
}

func TestSigningKeyBindingRepo_ListBySubject(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	ctx := context.Background()

	bindings := []domain.SigningKeyBinding{
		{
			ID: "skb-a",
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "user-1",
				Issuer:  "https://issuer.example.com",
			},
			PublicKeyJWK: []byte(`{}`), Algorithm: "ES256",
			KeyBindingDoc: []byte(`{}`), KeyBindingSignature: []byte("sig"),
			IdentityToken: "tok-a",
			CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			ExpiresAt: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			ID: "skb-b",
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "user-1",
				Issuer:  "https://issuer.example.com",
			},
			PublicKeyJWK: []byte(`{}`), Algorithm: "ES256",
			KeyBindingDoc: []byte(`{}`), KeyBindingSignature: []byte("sig"),
			IdentityToken: "tok-b",
			CreatedAt: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			ExpiresAt: time.Date(2027, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			ID: "skb-c",
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "user-2",
				Issuer:  "https://issuer.example.com",
			},
			PublicKeyJWK: []byte(`{}`), Algorithm: "ES256",
			KeyBindingDoc: []byte(`{}`), KeyBindingSignature: []byte("sig"),
			IdentityToken: "tok-c",
			CreatedAt: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			ExpiresAt: time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			ID: "skb-d",
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "user-1",
				Issuer:  "https://other-issuer.example.com",
			},
			PublicKeyJWK: []byte(`{}`), Algorithm: "ES256",
			KeyBindingDoc: []byte(`{}`), KeyBindingSignature: []byte("sig"),
			IdentityToken: "tok-d",
			CreatedAt: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
			ExpiresAt: time.Date(2027, 4, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	for _, b := range bindings {
		if err := tx.SigningKeyBindings().Create(ctx, b); err != nil {
			t.Fatalf("Create %s: %v", b.ID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tx, err = store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.Rollback()

	got, err := tx.SigningKeyBindings().ListBySubject(ctx, domain.FederatedIdentity{
		Subject: "user-1",
		Issuer:  "https://issuer.example.com",
	})
	if err != nil {
		t.Fatalf("ListBySubject: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d bindings, want 2", len(got))
	}

	ids := map[domain.SigningKeyBindingID]bool{}
	for _, b := range got {
		ids[b.ID] = true
	}
	if !ids["skb-a"] || !ids["skb-b"] {
		t.Errorf("expected skb-a and skb-b, got %v", ids)
	}
}
