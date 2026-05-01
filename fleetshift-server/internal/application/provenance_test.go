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
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// enrollSigner creates a signer enrollment in the store and registers
// the private key's public key in the fake registry, returning the
// private key for signing.
func enrollSigner(t *testing.T, store domain.Store, fakeReg *keyregistry.Fake, subjectID domain.SubjectID, issuer domain.IssuerURL) *ecdsa.PrivateKey {
	t.Helper()

	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	registrySubject := domain.RegistrySubject("gh-" + string(subjectID))
	fakeReg.Register("https://api.github.com", registrySubject, &privateKey.PublicKey)

	now := time.Now().UTC()
	enrollment := domain.SignerEnrollment{
		ID: "se-test-1",
		FederatedIdentity: domain.FederatedIdentity{
			Subject: subjectID,
			Issuer:  issuer,
		},
		IdentityToken:   "placeholder-token",
		RegistrySubject: registrySubject,
		RegistryID:      "github.com",
		CreatedAt:       now,
		ExpiresAt:       now.Add(365 * 24 * time.Hour),
	}

	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if err := tx.SignerEnrollments().Create(ctx, enrollment); err != nil {
		t.Fatalf("create signer enrollment: %v", err)
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
	privKey := enrollSigner(t, h.store, h.fakeReg, subjectID, issuer)

	registerTargets(t, h, "t1")

	ms := defaultManifestStrategy()
	ps := defaultPlacementStrategy()
	validUntil := time.Now().Add(24 * time.Hour)

	sig := signEnvelope(t, privKey, "signed-dep", ms, ps, validUntil, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx = application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: subjectID, Issuer: issuer}},
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

	if dep.Fulfillment.Provenance == nil {
		t.Fatal("expected Provenance to be set on signed fulfillment")
	}
	if dep.Fulfillment.Provenance.Sig.Signer.Subject != subjectID {
		t.Errorf("Signer.Subject = %q, want %q", dep.Fulfillment.Provenance.Sig.Signer.Subject, subjectID)
	}
	if len(dep.Fulfillment.Provenance.Sig.SignatureBytes) == 0 {
		t.Error("expected non-empty SignatureBytes")
	}
	if dep.Fulfillment.Provenance.ExpectedGeneration != 1 {
		t.Errorf("ExpectedGeneration = %d, want 1", dep.Fulfillment.Provenance.ExpectedGeneration)
	}
}

func TestCreateDeployment_WithoutSignature_NoProvenance(t *testing.T) {
	h := setup(t)

	registerTargets(t, h, "t1")

	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
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

	if dep.Fulfillment.Provenance != nil {
		t.Error("expected no Provenance on unsigned fulfillment")
	}
}

func TestCreateDeployment_WithSignature_NoEnrollment_Fails(t *testing.T) {
	h := setup(t)

	registerTargets(t, h, "t1")

	privKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ms := defaultManifestStrategy()
	ps := defaultPlacementStrategy()
	validUntil := time.Now().Add(24 * time.Hour)
	sig := signEnvelope(t, privKey, "no-binding-dep", ms, ps, validUntil, 1)

	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-no-binding", Issuer: "https://issuer.example.com"}},
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
	enrollSigner(t, h.store, h.fakeReg, subjectID, issuer)

	registerTargets(t, h, "t1")

	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: subjectID, Issuer: issuer}},
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

	seedDeployment(t, h.store, "prov-dep", func(f *domain.Fulfillment) {
		f.State = domain.FulfillmentStatePausedAuth
		f.Provenance = &domain.Provenance{
			Content: domain.DeploymentContent{
				DeploymentID:      "prov-dep",
				ManifestStrategy:  defaultManifestStrategy(),
				PlacementStrategy: defaultPlacementStrategy(),
			},
			Sig: domain.Signature{
				Signer:         domain.FederatedIdentity{Subject: subjectID, Issuer: issuer},
				SignatureBytes: []byte("old-sig"),
			},
			ExpectedGeneration: 1,
		}
	})

	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: subjectID, Issuer: issuer}},
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
	privKey := enrollSigner(t, h.store, h.fakeReg, subjectID, issuer)

	ms := defaultManifestStrategy()
	ps := defaultPlacementStrategy()

	seedDeployment(t, h.store, "resign-dep", func(f *domain.Fulfillment) {
		f.State = domain.FulfillmentStatePausedAuth
		f.ManifestStrategy = ms
		f.PlacementStrategy = ps
		f.Provenance = &domain.Provenance{
			Content: domain.DeploymentContent{
				DeploymentID:      "resign-dep",
				ManifestStrategy:  ms,
				PlacementStrategy: ps,
			},
			Sig: domain.Signature{
				Signer:         domain.FederatedIdentity{Subject: subjectID, Issuer: issuer},
				SignatureBytes: []byte("old-sig"),
			},
			ExpectedGeneration: 1,
		}
		f.Generation = 1
	})

	validUntil := time.Now().Add(24 * time.Hour)
	sig := signEnvelope(t, privKey, "resign-dep", ms, ps, validUntil, 2)

	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: subjectID, Issuer: issuer}},
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

	if dep.Fulfillment.Provenance == nil {
		t.Fatal("expected fresh Provenance after re-sign")
	}
	if dep.Fulfillment.Provenance.ExpectedGeneration != 2 {
		t.Errorf("ExpectedGeneration = %d, want 2", dep.Fulfillment.Provenance.ExpectedGeneration)
	}
}

func TestResumeDeployment_TokenPassthrough_NoProvenance(t *testing.T) {
	h := setup(t)

	seedDeployment(t, h.store, "token-dep", func(f *domain.Fulfillment) {
		f.State = domain.FulfillmentStatePausedAuth
	})

	ctx := application.ContextWithAuth(context.Background(), &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"}},
		Token:   "fresh-token",
	})

	dep, err := h.deployments.Resume(ctx, application.ResumeInput{ID: "token-dep"})
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if dep.Fulfillment.Provenance != nil {
		t.Error("expected no Provenance on token-passthrough resume")
	}
}

func TestRepoRoundTrip_ProvenanceOnFulfillment(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}

	now := time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC)
	fID := domain.FulfillmentID("ful-prov-rt")

	f := domain.Fulfillment{
		ID:        fID,
		CreatedAt: now,
		UpdatedAt: now,
	}
	f.AdvanceManifestStrategy(domain.ManifestStrategySpec{
		Type: domain.ManifestStrategyInline,
		Manifests: []domain.Manifest{{
			ResourceType: "api.kind.cluster",
			Raw:          []byte(`{"name":"test"}`),
		}},
	}, now)
	f.AdvancePlacementStrategy(domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"target-a"},
	}, now)
	f.AdvanceRolloutStrategy(nil, now)

	f.Auth = domain.DeliveryAuth{
		Caller: &domain.SubjectClaims{
			FederatedIdentity: domain.FederatedIdentity{
				Subject: "user-1",
				Issuer:  "https://issuer.example.com",
			},
		},
		Token: "test-token",
	}
	f.Provenance = &domain.Provenance{
		Content: domain.DeploymentContent{
			DeploymentID:      "prov-rt",
			ManifestStrategy:  f.ManifestStrategy,
			PlacementStrategy: f.PlacementStrategy,
		},
		Sig: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "user-1", Issuer: "https://issuer.example.com"},
			ContentHash:    []byte("hash-bytes"),
			SignatureBytes: []byte("sig-bytes"),
		},
		ValidUntil:         now.Add(24 * time.Hour),
		ExpectedGeneration: 1,
		OutputConstraints: []domain.OutputConstraint{
			{Name: "test-constraint", Expression: "output.valid == true"},
		},
	}
	f.State = domain.FulfillmentStateCreating
	f.Generation = 1

	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := tx.Fulfillments().Create(ctx, f); err != nil {
		t.Fatalf("create fulfillment: %v", err)
	}
	if err := tx.Deployments().Create(ctx, domain.Deployment{
		ID:            "prov-rt",
		UID:           "uid-prov-rt",
		FulfillmentID: fID,
		CreatedAt:     now,
		UpdatedAt:     now,
		Etag:          "test-etag",
	}); err != nil {
		t.Fatalf("create deployment: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	tx2, err := store.BeginReadOnly(ctx)
	if err != nil {
		t.Fatalf("begin read: %v", err)
	}
	defer tx2.Rollback()

	got, err := tx2.Deployments().GetView(ctx, "prov-rt")
	if err != nil {
		t.Fatalf("get view: %v", err)
	}

	if got.Fulfillment.Provenance == nil {
		t.Fatal("expected Provenance to survive round-trip")
	}

	p := got.Fulfillment.Provenance
	if p.Sig.Signer.Subject != "user-1" {
		t.Errorf("Sig.Signer.Subject = %q, want %q", p.Sig.Signer.Subject, "user-1")
	}
	if p.Sig.Signer.Issuer != "https://issuer.example.com" {
		t.Errorf("Sig.Signer.Issuer = %q, want %q", p.Sig.Signer.Issuer, "https://issuer.example.com")
	}
	if string(p.Sig.ContentHash) != "hash-bytes" {
		t.Errorf("Sig.ContentHash mismatch")
	}
	if string(p.Sig.SignatureBytes) != "sig-bytes" {
		t.Errorf("Sig.SignatureBytes mismatch")
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
