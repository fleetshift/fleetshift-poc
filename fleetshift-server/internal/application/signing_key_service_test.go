package application_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

const enrollmentAudience = "fleetshift-key-enrollment"

func setupSigningKeyService(t *testing.T) (*application.SigningKeyService, *oidctest.Provider) {
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
		},
	}); err != nil {
		t.Fatalf("save auth method: %v", err)
	}

	svc := &application.SigningKeyService{
		Store:       store,
		Verifier:    verifier,
		AuthMethods: authMethodRepo,
	}
	return svc, provider
}

func buildBundle(t *testing.T, provider *oidctest.Provider, subject string) (application.CreateSigningKeyBindingInput, *ecdsa.PrivateKey) {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	pubJWK := ecPubKeyJWK(t, &privateKey.PublicKey)

	idToken := provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  subject,
		Audience: enrollmentAudience,
	})

	doc := map[string]any{
		"public_key_jwk": json.RawMessage(pubJWK),
		"subject":        subject,
		"issuer":         string(provider.IssuerURL()),
		"enrolled_at":    "2026-03-11T00:00:00Z",
	}
	docBytes, err := json.Marshal(doc)
	if err != nil {
		t.Fatalf("marshal doc: %v", err)
	}

	hash := sha256.Sum256(docBytes)
	sig, err := ecdsa.SignASN1(rand.Reader, privateKey, hash[:])
	if err != nil {
		t.Fatalf("sign doc: %v", err)
	}

	return application.CreateSigningKeyBindingInput{
		ID:                  "skb-1",
		KeyBindingDoc:       docBytes,
		KeyBindingSignature: sig,
		IdentityToken:       idToken,
	}, privateKey
}

func ecPubKeyJWK(t *testing.T, pub *ecdsa.PublicKey) []byte {
	t.Helper()
	byteLen := (pub.Curve.Params().BitSize + 7) / 8
	xBytes := make([]byte, byteLen)
	yBytes := make([]byte, byteLen)
	xRaw := pub.X.Bytes()
	yRaw := pub.Y.Bytes()
	copy(xBytes[byteLen-len(xRaw):], xRaw)
	copy(yBytes[byteLen-len(yRaw):], yRaw)

	jwk := struct {
		Kty string `json:"kty"`
		Crv string `json:"crv"`
		X   string `json:"x"`
		Y   string `json:"y"`
	}{
		Kty: "EC",
		Crv: "P-256",
		X:   base64.RawURLEncoding.EncodeToString(xBytes),
		Y:   base64.RawURLEncoding.EncodeToString(yBytes),
	}
	b, err := json.Marshal(jwk)
	if err != nil {
		t.Fatalf("marshal JWK: %v", err)
	}
	return b
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

func TestSigningKeyService_Create_ValidBundle(t *testing.T) {
	svc, provider := setupSigningKeyService(t)

	input, _ := buildBundle(t, provider, "user-1")
	ctx := callerCtx("user-1", provider.IssuerURL())

	binding, err := svc.Create(ctx, input)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if binding.ID != "skb-1" {
		t.Errorf("ID = %q, want %q", binding.ID, "skb-1")
	}
	if binding.Subject != "user-1" {
		t.Errorf("Subject = %q, want %q", binding.Subject, "user-1")
	}
	if binding.Algorithm != "ES256" {
		t.Errorf("Algorithm = %q, want %q", binding.Algorithm, "ES256")
	}
}

func TestSigningKeyService_Create_WrongSubject(t *testing.T) {
	svc, provider := setupSigningKeyService(t)

	input, _ := buildBundle(t, provider, "user-1")
	ctx := callerCtx("different-user", provider.IssuerURL())

	_, err := svc.Create(ctx, input)
	if err == nil {
		t.Fatal("expected error for mismatched subject, got nil")
	}
	if !containsStr(err.Error(), "does not match caller") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSigningKeyService_Create_BadPoP(t *testing.T) {
	svc, provider := setupSigningKeyService(t)

	input, _ := buildBundle(t, provider, "user-1")
	input.KeyBindingSignature = []byte("not-a-valid-signature")
	ctx := callerCtx("user-1", provider.IssuerURL())

	_, err := svc.Create(ctx, input)
	if err == nil {
		t.Fatal("expected error for bad PoP signature, got nil")
	}
	if !containsStr(err.Error(), "proof of possession") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSigningKeyService_Create_WrongAudience(t *testing.T) {
	svc, provider := setupSigningKeyService(t)

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pubJWK := ecPubKeyJWK(t, &privateKey.PublicKey)

	idToken := provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  "user-1",
		Audience: "wrong-audience",
	})

	doc := map[string]any{
		"public_key_jwk": json.RawMessage(pubJWK),
		"subject":        "user-1",
		"issuer":         string(provider.IssuerURL()),
		"enrolled_at":    "2026-03-11T00:00:00Z",
	}
	docBytes, _ := json.Marshal(doc)
	hash := sha256.Sum256(docBytes)
	sig, _ := ecdsa.SignASN1(rand.Reader, privateKey, hash[:])

	input := application.CreateSigningKeyBindingInput{
		ID:                  "skb-wrong-aud",
		KeyBindingDoc:       docBytes,
		KeyBindingSignature: sig,
		IdentityToken:       idToken,
	}

	ctx := callerCtx("user-1", provider.IssuerURL())
	_, err = svc.Create(ctx, input)
	if err == nil {
		t.Fatal("expected error for wrong audience, got nil")
	}
	if !containsStr(err.Error(), "identity token verification failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSigningKeyService_Create_NoAuth(t *testing.T) {
	svc, provider := setupSigningKeyService(t)

	input, _ := buildBundle(t, provider, "user-1")
	_, err := svc.Create(context.Background(), input)
	if err == nil {
		t.Fatal("expected error for unauthenticated caller, got nil")
	}
	if !containsStr(err.Error(), "authenticated caller") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSigningKeyService_Create_MissingEnrollmentAudience(t *testing.T) {
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

	svc := &application.SigningKeyService{
		Store:       store,
		Verifier:    verifier,
		AuthMethods: authMethodRepo,
	}

	input, _ := buildBundle(t, provider, "user-1")
	ctx := callerCtx("user-1", provider.IssuerURL())

	_, err = svc.Create(ctx, input)
	if err == nil {
		t.Fatal("expected error for missing enrollment audience, got nil")
	}
	if !containsStr(err.Error(), "key_enrollment_audience") {
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
