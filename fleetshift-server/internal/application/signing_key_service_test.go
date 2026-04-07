package application_test

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

const enrollmentAudience = "fleetshift-key-enrollment"

func setupSignerEnrollmentService(t *testing.T) (*application.SignerEnrollmentService, *oidctest.Provider) {
	t.Helper()

	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	provider := oidctest.Start(t)

	verifier, err := oidc.NewVerifier(context.Background(), oidc.WithHTTPClient(provider.HTTPClient()))
	if err != nil {
		t.Fatalf("create verifier: %v", err)
	}
	if err := verifier.RegisterKeySet(context.Background(), domain.EndpointURL(string(provider.IssuerURL())+"/jwks")); err != nil {
		t.Fatalf("register key set: %v", err)
	}

	authMethodRepo := &sqlite.AuthMethodRepo{DB: store.DB}
	if err := authMethodRepo.Save(context.Background(), domain.AuthMethod{
		ID:   "default",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: &domain.OIDCConfig{
			IssuerURL:             provider.IssuerURL(),
			Audience:              provider.Audience(),
			JWKSURI:               domain.EndpointURL(string(provider.IssuerURL()) + "/jwks"),
			KeyEnrollmentAudience: enrollmentAudience,
			RegistrySubjectMapping: &domain.RegistrySubjectMapping{
				RegistryID: "github.com",
				Expression: `claims.preferred_username`,
			},
		},
	}); err != nil {
		t.Fatalf("save auth method: %v", err)
	}

	svc := &application.SignerEnrollmentService{
		Store:       store,
		Verifier:    verifier,
		AuthMethods: authMethodRepo,
	}
	return svc, provider
}

func issueEnrollmentToken(t *testing.T, provider *oidctest.Provider, subject, ghUsername string) string {
	t.Helper()
	return provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  subject,
		Audience: enrollmentAudience,
		Extra:    map[string]any{"preferred_username": ghUsername},
	})
}

func callerCtx(subject string, issuer domain.IssuerURL) context.Context {
	return application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: domain.SubjectID(subject),
				Issuer:  issuer,
			},
		},
		Token: "access-token",
	})
}

func TestSignerEnrollmentService_Create_Valid(t *testing.T) {
	svc, provider := setupSignerEnrollmentService(t)

	idToken := issueEnrollmentToken(t, provider, "user-1", "gh-user-1")
	ctx := callerCtx("user-1", provider.IssuerURL())

	enrollment, err := svc.Create(ctx, application.CreateSignerEnrollmentInput{
		ID:            "se-1",
		IdentityToken: idToken,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if enrollment.ID != "se-1" {
		t.Errorf("ID = %q, want %q", enrollment.ID, "se-1")
	}
	if enrollment.Subject != "user-1" {
		t.Errorf("Subject = %q, want %q", enrollment.Subject, "user-1")
	}
	if enrollment.RegistrySubject != "gh-user-1" {
		t.Errorf("RegistrySubject = %q, want %q", enrollment.RegistrySubject, "gh-user-1")
	}
	if enrollment.RegistryID != "github.com" {
		t.Errorf("RegistryID = %q, want %q", enrollment.RegistryID, "github.com")
	}
}

func TestSignerEnrollmentService_Create_WrongSubject(t *testing.T) {
	svc, provider := setupSignerEnrollmentService(t)

	idToken := issueEnrollmentToken(t, provider, "user-1", "gh-user-1")
	ctx := callerCtx("different-user", provider.IssuerURL())

	_, err := svc.Create(ctx, application.CreateSignerEnrollmentInput{
		ID:            "se-wrong-sub",
		IdentityToken: idToken,
	})
	if err == nil {
		t.Fatal("expected error for mismatched subject, got nil")
	}
	if !containsStr(err.Error(), "does not match caller") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSignerEnrollmentService_Create_WrongAudience(t *testing.T) {
	svc, provider := setupSignerEnrollmentService(t)

	idToken := provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  "user-1",
		Audience: "wrong-audience",
		Extra:    map[string]any{"preferred_username": "gh-user-1"},
	})

	ctx := callerCtx("user-1", provider.IssuerURL())
	_, err := svc.Create(ctx, application.CreateSignerEnrollmentInput{
		ID:            "se-wrong-aud",
		IdentityToken: idToken,
	})
	if err == nil {
		t.Fatal("expected error for wrong audience, got nil")
	}
	if !containsStr(err.Error(), "identity token verification failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSignerEnrollmentService_Create_NoAuth(t *testing.T) {
	svc, provider := setupSignerEnrollmentService(t)

	idToken := issueEnrollmentToken(t, provider, "user-1", "gh-user-1")
	_, err := svc.Create(context.Background(), application.CreateSignerEnrollmentInput{
		ID:            "se-no-auth",
		IdentityToken: idToken,
	})
	if err == nil {
		t.Fatal("expected error for unauthenticated caller, got nil")
	}
	if !containsStr(err.Error(), "authenticated caller") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSignerEnrollmentService_Create_MissingEnrollmentAudience(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	provider := oidctest.Start(t)

	verifier, err := oidc.NewVerifier(context.Background(), oidc.WithHTTPClient(provider.HTTPClient()))
	if err != nil {
		t.Fatalf("create verifier: %v", err)
	}

	authMethodRepo := &sqlite.AuthMethodRepo{DB: store.DB}
	if err := authMethodRepo.Save(context.Background(), domain.AuthMethod{
		ID:   "default",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: &domain.OIDCConfig{
			IssuerURL: provider.IssuerURL(),
			Audience:  provider.Audience(),
			JWKSURI:   domain.EndpointURL(string(provider.IssuerURL()) + "/jwks"),
			// KeyEnrollmentAudience intentionally omitted
		},
	}); err != nil {
		t.Fatalf("save auth method: %v", err)
	}

	svc := &application.SignerEnrollmentService{
		Store:       store,
		Verifier:    verifier,
		AuthMethods: authMethodRepo,
	}

	idToken := provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  "user-1",
		Audience: "anything",
		Extra:    map[string]any{"preferred_username": "gh-user-1"},
	})
	ctx := callerCtx("user-1", provider.IssuerURL())

	_, err = svc.Create(ctx, application.CreateSignerEnrollmentInput{
		ID:            "se-no-aud",
		IdentityToken: idToken,
	})
	if err == nil {
		t.Fatal("expected error for missing enrollment audience, got nil")
	}
	if !containsStr(err.Error(), "key_enrollment_audience") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSignerEnrollmentService_Create_MissingRegistrySubjectMapping(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	provider := oidctest.Start(t)

	verifier, err := oidc.NewVerifier(context.Background(), oidc.WithHTTPClient(provider.HTTPClient()))
	if err != nil {
		t.Fatalf("create verifier: %v", err)
	}

	authMethodRepo := &sqlite.AuthMethodRepo{DB: store.DB}
	if err := authMethodRepo.Save(context.Background(), domain.AuthMethod{
		ID:   "default",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: &domain.OIDCConfig{
			IssuerURL:             provider.IssuerURL(),
			Audience:              provider.Audience(),
			JWKSURI:               domain.EndpointURL(string(provider.IssuerURL()) + "/jwks"),
			KeyEnrollmentAudience: enrollmentAudience,
			// RegistrySubjectMapping intentionally omitted
		},
	}); err != nil {
		t.Fatalf("save auth method: %v", err)
	}

	svc := &application.SignerEnrollmentService{
		Store:       store,
		Verifier:    verifier,
		AuthMethods: authMethodRepo,
	}

	idToken := provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  "user-1",
		Audience: enrollmentAudience,
	})
	ctx := callerCtx("user-1", provider.IssuerURL())

	_, err = svc.Create(ctx, application.CreateSignerEnrollmentInput{
		ID:            "se-no-mapping",
		IdentityToken: idToken,
	})
	if err == nil {
		t.Fatal("expected error for missing registry subject mapping, got nil")
	}
	if !containsStr(err.Error(), "registry subject mapping") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
