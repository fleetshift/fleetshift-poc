package sqlite_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func TestSignerEnrollmentRepo_CreateAndGet(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	ctx := context.Background()

	enrollment := domain.SignerEnrollment{
		ID: "se-1",
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "user-1",
			Issuer:  "https://issuer.example.com",
		},
		IdentityToken:   "id-token-value",
		RegistrySubject: "ghuser1",
		RegistryID:      "github.com",
		CreatedAt:       time.Date(2026, 3, 11, 0, 0, 0, 0, time.UTC),
		ExpiresAt:       time.Date(2027, 3, 11, 0, 0, 0, 0, time.UTC),
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.SignerEnrollments().Create(ctx, enrollment); err != nil {
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

	got, err := tx.SignerEnrollments().Get(ctx, "se-1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if got.ID != "se-1" {
		t.Errorf("ID = %q, want %q", got.ID, "se-1")
	}
	if got.Subject != "user-1" {
		t.Errorf("Subject = %q, want %q", got.Subject, "user-1")
	}
	if got.Issuer != "https://issuer.example.com" {
		t.Errorf("Issuer = %q, want %q", got.Issuer, "https://issuer.example.com")
	}
	if string(got.IdentityToken) != "id-token-value" {
		t.Errorf("IdentityToken = %q, want %q", got.IdentityToken, "id-token-value")
	}
	if string(got.RegistrySubject) != "ghuser1" {
		t.Errorf("RegistrySubject = %q, want %q", got.RegistrySubject, "ghuser1")
	}
	if string(got.RegistryID) != "github.com" {
		t.Errorf("RegistryID = %q, want %q", got.RegistryID, "github.com")
	}
}

func TestSignerEnrollmentRepo_GetNotFound(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	ctx := context.Background()

	tx, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.Rollback()

	_, err = tx.SignerEnrollments().Get(ctx, "nonexistent")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got: %v", err)
	}
}

func TestSignerEnrollmentRepo_CreateDuplicate(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	ctx := context.Background()

	enrollment := domain.SignerEnrollment{
		ID: "se-dup",
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "user-1",
			Issuer:  "https://issuer.example.com",
		},
		IdentityToken:   "tok",
		RegistrySubject: "ghuser1",
		RegistryID:      "github.com",
		CreatedAt:       time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		ExpiresAt:       time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	if err := tx.SignerEnrollments().Create(ctx, enrollment); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	tx, err = store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()

	err = tx.SignerEnrollments().Create(ctx, enrollment)
	if !errors.Is(err, domain.ErrAlreadyExists) {
		t.Fatalf("expected ErrAlreadyExists, got: %v", err)
	}
}

func TestSignerEnrollmentRepo_ListBySubject(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	ctx := context.Background()

	enrollments := []domain.SignerEnrollment{
		{
			ID: "se-a",
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "user-1",
				Issuer:  "https://issuer.example.com",
			},
			IdentityToken: "tok-a", RegistrySubject: "ghuser1", RegistryID: "github.com",
			CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
			ExpiresAt: time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			ID: "se-b",
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "user-1",
				Issuer:  "https://issuer.example.com",
			},
			IdentityToken: "tok-b", RegistrySubject: "ghuser1", RegistryID: "github.com",
			CreatedAt: time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC),
			ExpiresAt: time.Date(2027, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			ID: "se-c",
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "user-2",
				Issuer:  "https://issuer.example.com",
			},
			IdentityToken: "tok-c", RegistrySubject: "ghuser2", RegistryID: "github.com",
			CreatedAt: time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC),
			ExpiresAt: time.Date(2027, 3, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			ID: "se-d",
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "user-1",
				Issuer:  "https://other-issuer.example.com",
			},
			IdentityToken: "tok-d", RegistrySubject: "ghuser1", RegistryID: "github.com",
			CreatedAt: time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC),
			ExpiresAt: time.Date(2027, 4, 1, 0, 0, 0, 0, time.UTC),
		},
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	for _, e := range enrollments {
		if err := tx.SignerEnrollments().Create(ctx, e); err != nil {
			t.Fatalf("Create %s: %v", e.ID, err)
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

	got, err := tx.SignerEnrollments().ListBySubject(ctx, domain.FederatedIdentity{
		Subject: "user-1",
		Issuer:  "https://issuer.example.com",
	})
	if err != nil {
		t.Fatalf("ListBySubject: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d enrollments, want 2", len(got))
	}

	ids := map[domain.SignerEnrollmentID]bool{}
	for _, e := range got {
		ids[e.ID] = true
	}
	if !ids["se-a"] || !ids["se-b"] {
		t.Errorf("expected se-a and se-b, got %v", ids)
	}
}
