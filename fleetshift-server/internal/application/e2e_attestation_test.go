package application_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/attestation"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// TestEndToEnd_CreateDeployment_AssemblesAndVerifiesAttestation exercises
// the full attestation lifecycle with the registry-backed key model:
//  1. Enroll a signer (ECDSA key in fake registry + SignerEnrollment)
//  2. Create a deployment with a user signature → provenance attached
//  3. Orchestration assembles an Attestation and passes it to the delivery agent
//  4. The delivery agent's attestation is verified end-to-end (key resolved from registry)
func TestEndToEnd_CreateDeployment_AssemblesAndVerifiesAttestation(t *testing.T) {
	provider := oidctest.Start(t, oidctest.WithAudience("fleetshift-enroll"))
	signerID := domain.SubjectID("e2e-user")
	issuer := provider.IssuerURL()

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	registrySubject := domain.RegistrySubject("gh-" + string(signerID))

	identityToken := provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  string(signerID),
		Audience: "fleetshift-enroll",
		Extra:    map[string]any{"preferred_username": registrySubject},
	})

	store := newStore(t)
	inner := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC) },
	}
	agent := &capturingDeliveryAgent{inner: inner}
	h := setupWithStoreAndAgent(t, store, agent)

	h.fakeReg.Register("https://api.github.com", registrySubject, &privKey.PublicKey)

	registerTargets(t, h, "t1")

	enrollSignerDirect(t, store, domain.SignerEnrollment{
		ID:                "se-e2e",
		FederatedIdentity: domain.FederatedIdentity{Subject: signerID, Issuer: issuer},
		IdentityToken:     domain.RawToken(identityToken),
		RegistrySubject:   registrySubject,
		RegistryID:        "github.com",
		CreatedAt:         time.Now().UTC(),
		ExpiresAt:         time.Now().Add(365 * 24 * time.Hour).UTC(),
	})

	ms := defaultManifestStrategy()
	ps := defaultPlacementStrategy()
	validUntil := time.Now().Add(24 * time.Hour)

	sig := signEnvelope(t, privKey, "e2e-dep", ms, ps, validUntil, 1)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ctx = application.ContextWithAuth(ctx, &application.AuthorizationContext{
		Subject: &domain.SubjectClaims{FederatedIdentity: domain.FederatedIdentity{Subject: signerID, Issuer: issuer}},
		Token:   "access-token",
	})

	dep, err := h.deployments.Create(ctx, domain.CreateDeploymentInput{
		ID:                 "e2e-dep",
		ManifestStrategy:   ms,
		PlacementStrategy:  ps,
		UserSignature:      sig,
		ValidUntil:         validUntil,
		ExpectedGeneration: 1,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if dep.Provenance == nil {
		t.Fatal("expected Provenance on deployment")
	}

	awaitDeploymentState(ctx, t, store, "e2e-dep", domain.DeploymentStateActive)

	att := agent.capturedAttestation()
	if att == nil {
		t.Fatal("delivery agent received nil Attestation; expected assembled attestation")
	}

	if att.Input.Content.DeploymentID != "e2e-dep" {
		t.Errorf("Attestation.Input.Content.DeploymentID = %q, want %q",
			att.Input.Content.DeploymentID, "e2e-dep")
	}
	if att.Input.Signer.RegistrySubject != registrySubject {
		t.Errorf("Attestation.Input.Signer.RegistrySubject = %q, want %q",
			att.Input.Signer.RegistrySubject, registrySubject)
	}
	if att.Input.Signer.RegistryID != "github.com" {
		t.Errorf("Attestation.Input.Signer.RegistryID = %q, want %q",
			att.Input.Signer.RegistryID, "github.com")
	}
	put, ok := att.Output.(*domain.PutManifests)
	if !ok {
		t.Fatalf("Attestation.Output is %T, want *PutManifests", att.Output)
	}
	if len(put.Manifests) == 0 {
		t.Fatal("PutManifests.Manifests is empty")
	}

	// Build a verifier with the same fake registry for key resolution.
	fakeVerifierReg := keyregistry.NewFake()
	fakeVerifierReg.Register("https://api.github.com", registrySubject, &privKey.PublicKey)
	verifierKeyResolver := &application.KeyResolver{
		Registries: domain.BuiltInKeyRegistries(),
		Clients: map[domain.KeyRegistryType]domain.RegistryClient{
			domain.KeyRegistryTypeGitHub: fakeVerifierReg,
		},
	}

	celExpr := `claims.preferred_username`
	jwksURI := string(issuer) + "/jwks"
	verifier := attestation.NewVerifier(
		map[domain.IssuerURL]attestation.TrustedIssuer{
			issuer: {
				JWKSURI:  domain.EndpointURL(jwksURI),
				Audience: "fleetshift-enroll",
				RegistrySubjectMapping: &domain.RegistrySubjectMapping{
					RegistryID: "github.com",
					Expression: celExpr,
				},
			},
		},
		attestation.WithHTTPClient(provider.HTTPClient()),
		attestation.WithKeyResolver(verifierKeyResolver),
	)

	if err := verifier.Verify(ctx, att); err != nil {
		t.Fatalf("Verify: %v", err)
	}
}

// capturingDeliveryAgent wraps another [domain.DeliveryAgent] and records
// the last [domain.Attestation] passed to Deliver.
type capturingDeliveryAgent struct {
	inner domain.DeliveryAgent
	mu    sync.Mutex
	att   *domain.Attestation
}

func (a *capturingDeliveryAgent) Deliver(ctx context.Context, target domain.TargetInfo, id domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, signaler *domain.DeliverySignaler) (domain.DeliveryResult, error) {
	a.mu.Lock()
	a.att = att
	a.mu.Unlock()
	return a.inner.Deliver(ctx, target, id, manifests, auth, att, signaler)
}

func (a *capturingDeliveryAgent) Remove(ctx context.Context, target domain.TargetInfo, id domain.DeliveryID, signaler *domain.DeliverySignaler) error {
	return a.inner.Remove(ctx, target, id, signaler)
}

func (a *capturingDeliveryAgent) capturedAttestation() *domain.Attestation {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.att
}

func enrollSignerDirect(t *testing.T, store domain.Store, enrollment domain.SignerEnrollment) {
	t.Helper()
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
}
