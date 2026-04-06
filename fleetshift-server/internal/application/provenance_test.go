package application_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// enrollKeyBinding creates a signing key binding in the store for the
// given subject, returning the private key for signing.
func enrollKeyBinding(t *testing.T, store domain.Store, subjectID domain.SubjectID, issuer domain.IssuerURL) *ecdsa.PrivateKey {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	pubJWK := ecPubKeyJWK(t, &privateKey.PublicKey)

	now := time.Now().UTC()
	binding := domain.SigningKeyBinding{
		ID:                  "skb-test-1",
		SubjectID:           subjectID,
		Issuer:              issuer,
		PublicKeyJWK:        pubJWK,
		Algorithm:           "ES256",
		KeyBindingDoc:       []byte(`{"subject":"` + string(subjectID) + `"}`),
		KeyBindingSignature: []byte("placeholder-sig"),
		IdentityToken:       "placeholder-token",
		CreatedAt:           now,
		ExpiresAt:           now.Add(365 * 24 * time.Hour),
	}

	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if err := tx.SigningKeyBindings().Create(ctx, binding); err != nil {
		t.Fatalf("create signing key binding: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	return privateKey
}

func signEnvelope(t *testing.T, privKey *ecdsa.PrivateKey, id domain.DeploymentID, ms domain.ManifestStrategySpec, ps domain.PlacementStrategySpec, validUntil time.Time, expectedGen domain.Generation) []byte {
	t.Helper()
	envelopeBytes, err := domain.BuildSignedInputEnvelope(id, ms, ps, validUntil, nil, expectedGen)
	if err != nil {
		t.Fatalf("build signed input envelope: %v", err)
	}
	hash := domain.HashIntent(envelopeBytes)
	sig, err := ecdsa.SignASN1(rand.Reader, privKey, hash)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return sig
}

func defaultManifestStrategy() domain.ManifestStrategySpec {
	return domain.ManifestStrategySpec{
		Type: domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{
			ResourceType: "test.resource",
			Raw:          []byte(`{"name":"test"}`),
		}},
	}
}

func defaultPlacementStrategy() domain.PlacementStrategySpec {
	return domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}
}

func TestCreateDeployment_WithSignature_AttachesProvenance(t *testing.T) {
	h := setup(t)

	subjectID := domain.SubjectID("user-1")
	issuer := domain.IssuerURL("https://issuer.example.com")
	privKey := enrollKeyBinding(t, h.store, subjectID, issuer)

	registerTargets(t, h, "t1")

	ms := defaultManifestStrategy()
	ps := defaultPlacementStrategy()
	validUntil := time.Now().Add(24 * time.Hour)

	sig := signEnvelope(t, privKey, "signed-dep", ms, ps, validUntil, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx = application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{ID: subjectID, Issuer: issuer},
		Token:   "access-token",
	})

	dep, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID:                 "signed-dep",
		ManifestStrategy:   ms,
		PlacementStrategy:  ps,
		UserSignature:      sig,
		ValidUntil:         validUntil,
		ExpectedGeneration: 1,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if dep.Auth.Provenance == nil {
		t.Fatal("expected Provenance to be set on signed deployment")
	}
	if dep.Auth.Provenance.Sig.SignerID != subjectID {
		t.Errorf("SignerID = %q, want %q", dep.Auth.Provenance.Sig.SignerID, subjectID)
	}
	if len(dep.Auth.Provenance.Sig.SignatureBytes) == 0 {
		t.Error("expected non-empty SignatureBytes")
	}
	if dep.Auth.Provenance.ExpectedGeneration != 1 {
		t.Errorf("ExpectedGeneration = %d, want 1", dep.Auth.Provenance.ExpectedGeneration)
	}
}

func TestCreateDeployment_WithoutSignature_NoProvenance(t *testing.T) {
	h := setup(t)

	registerTargets(t, h, "t1")

	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{ID: "user-1", Issuer: "https://issuer.example.com"},
		Token:   "access-token",
	})

	dep, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID:                "unsigned-dep",
		ManifestStrategy:  defaultManifestStrategy(),
		PlacementStrategy: defaultPlacementStrategy(),
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if dep.Auth.Provenance != nil {
		t.Error("expected no Provenance on unsigned deployment")
	}
}

func TestCreateDeployment_WithSignature_NoKeyBinding_Fails(t *testing.T) {
	h := setup(t)

	registerTargets(t, h, "t1")

	privKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ms := defaultManifestStrategy()
	ps := defaultPlacementStrategy()
	validUntil := time.Now().Add(24 * time.Hour)
	sig := signEnvelope(t, privKey, "no-binding-dep", ms, ps, validUntil, 1)

	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{ID: "user-no-binding", Issuer: "https://issuer.example.com"},
		Token:   "access-token",
	})

	_, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID:                 "no-binding-dep",
		ManifestStrategy:   ms,
		PlacementStrategy:  ps,
		UserSignature:      sig,
		ValidUntil:         validUntil,
		ExpectedGeneration: 1,
	})
	if err == nil {
		t.Fatal("expected error for missing key binding")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestCreateDeployment_WithBadSignature_Fails(t *testing.T) {
	h := setup(t)

	subjectID := domain.SubjectID("user-1")
	issuer := domain.IssuerURL("https://issuer.example.com")
	enrollKeyBinding(t, h.store, subjectID, issuer)

	registerTargets(t, h, "t1")

	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{ID: subjectID, Issuer: issuer},
		Token:   "access-token",
	})

	_, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID:                 "bad-sig-dep",
		ManifestStrategy:   defaultManifestStrategy(),
		PlacementStrategy:  defaultPlacementStrategy(),
		UserSignature:      []byte("not-a-valid-signature"),
		ValidUntil:         time.Now().Add(24 * time.Hour),
		ExpectedGeneration: 1,
	})
	if err == nil {
		t.Fatal("expected error for bad signature")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestResumeDeployment_WithProvenance_RequiresReSign(t *testing.T) {
	h := setup(t)

	subjectID := domain.SubjectID("user-1")
	issuer := domain.IssuerURL("https://issuer.example.com")

	seedDeployment(t, h.store, domain.Deployment{
		ID:    "prov-dep",
		State: domain.DeploymentStatePausedAuth,
		Auth: domain.DeliveryAuth{
			Provenance: &domain.Provenance{
				Sig: domain.Signature{
					SignerID:       subjectID,
					SignatureBytes: []byte("old-sig"),
				},
				ExpectedGeneration: 1,
			},
		},
		ManifestStrategy:  defaultManifestStrategy(),
		PlacementStrategy: defaultPlacementStrategy(),
	})

	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{ID: subjectID, Issuer: issuer},
		Token:   "access-token",
	})

	_, err := h.deployments.Resume(ctx, application.ResumeInput{ID: "prov-dep"})
	if err == nil {
		t.Fatal("expected error: provenance requires re-signing")
	}
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("expected ErrInvalidArgument, got: %v", err)
	}
}

func TestResumeDeployment_WithReSign_UpdatesProvenance(t *testing.T) {
	h := setup(t)

	subjectID := domain.SubjectID("user-1")
	issuer := domain.IssuerURL("https://issuer.example.com")
	privKey := enrollKeyBinding(t, h.store, subjectID, issuer)

	ms := defaultManifestStrategy()
	ps := defaultPlacementStrategy()

	seedDeployment(t, h.store, domain.Deployment{
		ID:    "resign-dep",
		State: domain.DeploymentStatePausedAuth,
		Auth: domain.DeliveryAuth{
			Provenance: &domain.Provenance{
				Sig: domain.Signature{
					SignerID:       subjectID,
					SignatureBytes: []byte("old-sig"),
				},
				ExpectedGeneration: 1,
			},
		},
		ManifestStrategy:  ms,
		PlacementStrategy: ps,
		Generation:        1,
	})

	validUntil := time.Now().Add(24 * time.Hour)
	sig := signEnvelope(t, privKey, "resign-dep", ms, ps, validUntil, 2)

	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{ID: subjectID, Issuer: issuer},
		Token:   "access-token",
	})

	dep, err := h.deployments.Resume(ctx, application.ResumeInput{
		ID:            "resign-dep",
		UserSignature: sig,
		ValidUntil:    validUntil,
	})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	if dep.Auth.Provenance == nil {
		t.Fatal("expected fresh Provenance after re-sign")
	}
	if dep.Auth.Provenance.ExpectedGeneration != 2 {
		t.Errorf("ExpectedGeneration = %d, want 2", dep.Auth.Provenance.ExpectedGeneration)
	}
}

func TestResumeDeployment_TokenPassthrough_NoProvenance(t *testing.T) {
	h := setup(t)

	seedDeployment(t, h.store, domain.Deployment{
		ID:                "token-dep",
		State:             domain.DeploymentStatePausedAuth,
		ManifestStrategy:  defaultManifestStrategy(),
		PlacementStrategy: defaultPlacementStrategy(),
	})

	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{ID: "user-1", Issuer: "https://issuer.example.com"},
		Token:   "fresh-token",
	})

	dep, err := h.deployments.Resume(ctx, application.ResumeInput{ID: "token-dep"})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if dep.Auth.Provenance != nil {
		t.Error("expected no Provenance on token-passthrough resume")
	}
}

func TestRepoRoundTrip_ProvenanceInDeliveryAuth(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}

	now := time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)
	dep := domain.Deployment{
		ID:  "prov-rt",
		UID: "uid-prov-rt",
		ManifestStrategy: domain.ManifestStrategySpec{
			Type: domain.ManifestStrategyInline,
			Manifests: []domain.Manifest{{
				ResourceType: "api.kind.cluster",
				Raw:          []byte(`{"name":"test"}`),
			}},
		},
		PlacementStrategy: domain.PlacementStrategySpec{
			Type:    domain.PlacementStrategyStatic,
			Targets: []domain.TargetID{"target-a"},
		},
		Auth: domain.DeliveryAuth{
			Caller: &domain.SubjectClaims{
				ID:     "user-1",
				Issuer: "https://issuer.example.com",
			},
			Token: "test-token",
			Provenance: &domain.Provenance{
				Sig: domain.Signature{
					SignerID:       "user-1",
					PublicKey:      []byte("pubkey-bytes"),
					ContentHash:    []byte("hash-bytes"),
					SignatureBytes: []byte("sig-bytes"),
				},
				KeyBinding: domain.SigningKeyBinding{
					ID:           "skb-1",
					SubjectID:    "user-1",
					Issuer:       "https://issuer.example.com",
					PublicKeyJWK: []byte(`{"kty":"EC","crv":"P-256","x":"abc","y":"def"}`),
					Algorithm:    "ES256",
				},
				ValidUntil:         now.Add(24 * time.Hour),
				ExpectedGeneration: 1,
				OutputConstraints: []domain.OutputConstraint{
					{Name: "test-constraint", Expression: "output.valid == true"},
				},
			},
		},
		State:      domain.DeploymentStateCreating,
		Generation: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
		Etag:       "test-etag",
	}

	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Deployments().Create(ctx, dep); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	tx2, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	defer tx2.Rollback()

	got, err := tx2.Deployments().Get(ctx, "prov-rt")
	if err != nil {
		t.Fatalf("get: %v", err)
	}

	if got.Auth.Provenance == nil {
		t.Fatal("expected Provenance to survive round-trip")
	}

	p := got.Auth.Provenance
	if p.Sig.SignerID != "user-1" {
		t.Errorf("Sig.SignerID = %q, want %q", p.Sig.SignerID, "user-1")
	}
	if string(p.Sig.PublicKey) != "pubkey-bytes" {
		t.Errorf("Sig.PublicKey mismatch")
	}
	if string(p.Sig.ContentHash) != "hash-bytes" {
		t.Errorf("Sig.ContentHash mismatch")
	}
	if string(p.Sig.SignatureBytes) != "sig-bytes" {
		t.Errorf("Sig.SignatureBytes mismatch")
	}
	if p.KeyBinding.ID != "skb-1" {
		t.Errorf("KeyBinding.ID = %q, want %q", p.KeyBinding.ID, "skb-1")
	}
	if p.KeyBinding.Algorithm != "ES256" {
		t.Errorf("KeyBinding.Algorithm = %q, want %q", p.KeyBinding.Algorithm, "ES256")
	}
	if p.ExpectedGeneration != 1 {
		t.Errorf("ExpectedGeneration = %d, want 1", p.ExpectedGeneration)
	}
	if len(p.OutputConstraints) != 1 {
		t.Fatalf("OutputConstraints: got %d, want 1", len(p.OutputConstraints))
	}
	if p.OutputConstraints[0].Name != "test-constraint" {
		t.Errorf("OutputConstraints[0].Name = %q, want %q", p.OutputConstraints[0].Name, "test-constraint")
	}
}
