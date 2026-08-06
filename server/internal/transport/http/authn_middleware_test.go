package http

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// fakeAuthMethodRepo is an in-memory [domain.AuthMethodRepository].
type fakeAuthMethodRepo struct {
	methods map[domain.AuthMethodID]domain.AuthMethod
	listErr error
}

func newFakeAuthMethodRepo() *fakeAuthMethodRepo {
	return &fakeAuthMethodRepo{methods: make(map[domain.AuthMethodID]domain.AuthMethod)}
}

func (r *fakeAuthMethodRepo) Save(_ context.Context, method domain.AuthMethod) error {
	r.methods[method.ID()] = method
	return nil
}

func (r *fakeAuthMethodRepo) Get(_ context.Context, id domain.AuthMethodID) (domain.AuthMethod, error) {
	m, ok := r.methods[id]
	if !ok {
		return domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{}), domain.ErrNotFound
	}
	return m, nil
}

func (r *fakeAuthMethodRepo) List(_ context.Context) ([]domain.AuthMethod, error) {
	if r.listErr != nil {
		return nil, r.listErr
	}
	out := make([]domain.AuthMethod, 0, len(r.methods))
	for _, m := range r.methods {
		out = append(out, m)
	}
	return out, nil
}

// fakeOIDCTokenVerifier accepts or rejects tokens based on configuration.
type fakeOIDCTokenVerifier struct {
	acceptToken string
	rejectAll   bool
	claims      domain.SubjectClaims
}

func (f *fakeOIDCTokenVerifier) Verify(_ context.Context, _ domain.OIDCConfig, rawToken string) (domain.SubjectClaims, error) {
	if f.rejectAll {
		return domain.SubjectClaims{}, errors.New("token rejected")
	}
	if f.acceptToken != "" && rawToken != f.acceptToken {
		return domain.SubjectClaims{}, errors.New("invalid token")
	}
	return f.claims, nil
}

// fakePerAudienceVerifier accepts tokens only for a specific audience,
// allowing tests to exercise the multi-method loop path.
type fakePerAudienceVerifier struct {
	acceptAudience domain.Audience
	claims         domain.SubjectClaims
}

func (f *fakePerAudienceVerifier) Verify(_ context.Context, cfg domain.OIDCConfig, _ string) (domain.SubjectClaims, error) {
	if cfg.Audience != f.acceptAudience {
		return domain.SubjectClaims{}, errors.New("audience mismatch")
	}
	return f.claims, nil
}

// echoHandler is a simple handler that writes "ok" — used to verify
// the middleware allows the request through.
var echoHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
})

func testOIDCMethod() domain.AuthMethod {
	return domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{
		ID:   "oidc-1",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: &domain.OIDCConfig{
			IssuerURL:             "https://issuer.example.com",
			Audience:              "test-audience",
			JWKSURI:               "https://issuer.example.com/jwks",
			AuthorizationEndpoint: "https://issuer.example.com/authorize",
			TokenEndpoint:         "https://issuer.example.com/token",
		},
	})
}

func setupMiddleware(repo *fakeAuthMethodRepo, verifier *fakeOIDCTokenVerifier) *AuthnMiddleware {
	return &AuthnMiddleware{
		Methods:  &application.AuthMethodService{Methods: repo},
		Verifier: verifier,
		Logger:   slog.Default(),
	}
}

func TestAuthnMiddleware_NoAuthMethods_Anonymous(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	verifier := &fakeOIDCTokenVerifier{}
	mw := setupMiddleware(repo, verifier)

	req := httptest.NewRequest(http.MethodGet, "/api/ui/user-config", nil)
	rec := httptest.NewRecorder()

	mw.Wrap(echoHandler).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if rec.Body.String() != "ok" {
		t.Errorf("body = %q, want %q", rec.Body.String(), "ok")
	}
}

func TestAuthnMiddleware_ValidToken_Authenticated(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	if err := repo.Save(context.Background(), testOIDCMethod()); err != nil {
		t.Fatalf("Save: %v", err)
	}

	wantClaims := domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "user-123",
			Issuer:  "https://issuer.example.com",
		},
		Extra: map[string][]string{"email": {"user@example.com"}},
	}
	verifier := &fakeOIDCTokenVerifier{acceptToken: "valid-token", claims: wantClaims}
	mw := setupMiddleware(repo, verifier)

	// Capture the AuthorizationContext seen by the handler.
	var captured *application.AuthorizationContext
	inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		captured = application.AuthFromContext(r.Context())
	})

	req := httptest.NewRequest(http.MethodGet, "/api/ui/user-config", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()

	mw.Wrap(inner).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if captured == nil {
		t.Fatal("AuthorizationContext is nil")
	}
	if captured.Subject == nil {
		t.Fatal("Subject is nil, want authenticated claims")
	}
	if captured.Subject.Subject != wantClaims.Subject {
		t.Errorf("Subject.Subject = %q, want %q", captured.Subject.Subject, wantClaims.Subject)
	}
	if captured.Subject.Issuer != wantClaims.Issuer {
		t.Errorf("Subject.Issuer = %q, want %q", captured.Subject.Issuer, wantClaims.Issuer)
	}
}

func TestAuthnMiddleware_InvalidToken_Unauthorized(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	if err := repo.Save(context.Background(), testOIDCMethod()); err != nil {
		t.Fatalf("Save: %v", err)
	}

	verifier := &fakeOIDCTokenVerifier{rejectAll: true}
	mw := setupMiddleware(repo, verifier)

	req := httptest.NewRequest(http.MethodGet, "/api/ui/events/ws", nil)
	req.Header.Set("Authorization", "Bearer bad-token")
	rec := httptest.NewRecorder()

	mw.Wrap(echoHandler).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
	if wwwAuth := rec.Header().Get("WWW-Authenticate"); wwwAuth == "" {
		t.Error("missing WWW-Authenticate header")
	}
}

func TestAuthnMiddleware_MissingToken_WithMethodsConfigured(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	if err := repo.Save(context.Background(), testOIDCMethod()); err != nil {
		t.Fatalf("Save: %v", err)
	}

	verifier := &fakeOIDCTokenVerifier{acceptToken: "valid-token"}
	mw := setupMiddleware(repo, verifier)

	req := httptest.NewRequest(http.MethodGet, "/api/ui/github-signing-keys/alice", nil)
	// No Authorization header
	rec := httptest.NewRecorder()

	mw.Wrap(echoHandler).ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}

	var body map[string]string
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body["error"] != "missing bearer token" {
		t.Errorf("error = %q, want %q", body["error"], "missing bearer token")
	}
}

func TestAuthnMiddleware_StoreError_Returns500(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	repo.listErr = errors.New("database connection failed")

	verifier := &fakeOIDCTokenVerifier{}
	mw := setupMiddleware(repo, verifier)

	req := httptest.NewRequest(http.MethodGet, "/api/ui/user-config", nil)
	req.Header.Set("Authorization", "Bearer some-token")
	rec := httptest.NewRecorder()

	mw.Wrap(echoHandler).ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusInternalServerError)
	}
}

func TestAuthnMiddleware_SetupToEnforcedTransition(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	verifier := &fakeOIDCTokenVerifier{acceptToken: "valid-token"}
	mw := setupMiddleware(repo, verifier)

	handler := mw.Wrap(echoHandler)

	// Phase 1: No auth methods — anonymous access succeeds.
	req := httptest.NewRequest(http.MethodGet, "/api/ui/user-config", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("Phase 1: status = %d, want %d", rec.Code, http.StatusOK)
	}

	// Phase 2: Add an auth method.
	if err := repo.Save(context.Background(), testOIDCMethod()); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Phase 3: Request without token should now be rejected.
	req = httptest.NewRequest(http.MethodGet, "/api/ui/user-config", nil)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("Phase 3: status = %d, want %d", rec.Code, http.StatusUnauthorized)
	}
}

func TestAuthnMiddleware_AZPClaim_ClientCaptured(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	if err := repo.Save(context.Background(), testOIDCMethod()); err != nil {
		t.Fatalf("Save: %v", err)
	}

	verifier := &fakeOIDCTokenVerifier{
		acceptToken: "valid-token",
		claims: domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "user-456",
				Issuer:  "https://issuer.example.com",
			},
			Extra: map[string][]string{"azp": {"my-client-id"}},
		},
	}
	mw := setupMiddleware(repo, verifier)

	var captured *application.AuthorizationContext
	inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		captured = application.AuthFromContext(r.Context())
	})

	req := httptest.NewRequest(http.MethodGet, "/api/ui/user-config", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()

	mw.Wrap(inner).ServeHTTP(rec, req)

	if captured == nil || captured.Client == nil {
		t.Fatal("Client claims not captured")
	}
	if captured.Client.ID != "my-client-id" {
		t.Errorf("Client.ID = %q, want %q", captured.Client.ID, "my-client-id")
	}
}

func TestAuthnMiddleware_MultipleOIDCMethods_TriesAll(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	// First method — audience that the verifier will reject.
	if err := repo.Save(context.Background(), domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{
		ID:   "oidc-reject",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: &domain.OIDCConfig{
			IssuerURL:             "https://issuer-a.example.com",
			Audience:              "audience-a",
			JWKSURI:               "https://issuer-a.example.com/jwks",
			AuthorizationEndpoint: "https://issuer-a.example.com/authorize",
			TokenEndpoint:         "https://issuer-a.example.com/token",
		},
	})); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// Second method — audience the verifier accepts.
	if err := repo.Save(context.Background(), domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{
		ID:   "oidc-accept",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: &domain.OIDCConfig{
			IssuerURL:             "https://issuer-b.example.com",
			Audience:              "audience-b",
			JWKSURI:               "https://issuer-b.example.com/jwks",
			AuthorizationEndpoint: "https://issuer-b.example.com/authorize",
			TokenEndpoint:         "https://issuer-b.example.com/token",
		},
	})); err != nil {
		t.Fatalf("Save: %v", err)
	}

	wantClaims := domain.SubjectClaims{
		FederatedIdentity: domain.FederatedIdentity{
			Subject: "multi-user",
			Issuer:  "https://issuer-b.example.com",
		},
	}
	verifier := &fakePerAudienceVerifier{
		acceptAudience: "audience-b",
		claims:         wantClaims,
	}
	mw := &AuthnMiddleware{
		Methods:  &application.AuthMethodService{Methods: repo},
		Verifier: verifier,
		Logger:   slog.Default(),
	}

	var captured *application.AuthorizationContext
	inner := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		captured = application.AuthFromContext(r.Context())
	})

	req := httptest.NewRequest(http.MethodGet, "/api/ui/user-config", nil)
	req.Header.Set("Authorization", "Bearer any-token")
	rec := httptest.NewRecorder()

	mw.Wrap(inner).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (first method should fail, second should match)", rec.Code, http.StatusOK)
	}
	if captured == nil || captured.Subject == nil {
		t.Fatal("AuthorizationContext or Subject is nil")
	}
	if captured.Subject.Subject != wantClaims.Subject {
		t.Errorf("Subject.Subject = %q, want %q", captured.Subject.Subject, wantClaims.Subject)
	}
}

func TestAuthnMiddleware_NilLogger_UsesDefault(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	verifier := &fakeOIDCTokenVerifier{}
	mw := &AuthnMiddleware{
		Methods:  &application.AuthMethodService{Methods: repo},
		Verifier: verifier,
		// Logger intentionally nil — should fall back to slog.Default().
	}

	req := httptest.NewRequest(http.MethodGet, "/api/ui/user-config", nil)
	rec := httptest.NewRecorder()

	mw.Wrap(echoHandler).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestAuthnMiddleware_NilOIDCConfig_Anonymous(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	// Save a method with nil OIDC config — should be filtered out
	// during OIDC pre-filtering, leaving zero valid OIDC methods.
	if err := repo.Save(context.Background(), domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{
		ID:   "oidc-broken",
		Type: domain.AuthMethodTypeOIDC,
		OIDC: nil,
	})); err != nil {
		t.Fatalf("Save: %v", err)
	}

	verifier := &fakeOIDCTokenVerifier{acceptToken: "valid-token"}
	mw := setupMiddleware(repo, verifier)

	req := httptest.NewRequest(http.MethodGet, "/api/ui/user-config", nil)
	req.Header.Set("Authorization", "Bearer valid-token")
	rec := httptest.NewRecorder()

	mw.Wrap(echoHandler).ServeHTTP(rec, req)

	// Method exists but has nil OIDC config — excluded from OIDC
	// pre-filter, so the middleware treats this as setup mode
	// (anonymous access allowed).
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
}

func TestAuthnMiddleware_OnlyNonOIDCMethods_Anonymous(t *testing.T) {
	repo := newFakeAuthMethodRepo()
	// Save a non-OIDC method — should not trigger enforced mode.
	if err := repo.Save(context.Background(), domain.AuthMethodFromSnapshot(domain.AuthMethodSnapshot{
		ID:   "non-oidc",
		Type: "api-key", // hypothetical non-OIDC method type
		OIDC: nil,
	})); err != nil {
		t.Fatalf("Save: %v", err)
	}

	verifier := &fakeOIDCTokenVerifier{}
	mw := setupMiddleware(repo, verifier)

	req := httptest.NewRequest(http.MethodGet, "/api/ui/user-config", nil)
	// No Authorization header — should still be allowed through.
	rec := httptest.NewRecorder()

	mw.Wrap(echoHandler).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d (non-OIDC methods should not trigger enforced mode)", rec.Code, http.StatusOK)
	}
}
