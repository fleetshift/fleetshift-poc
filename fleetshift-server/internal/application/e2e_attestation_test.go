package application_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/attestation"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// TestEndToEnd_CreateDeployment_AssemblesAndVerifiesAttestation exercises
// the full attestation lifecycle:
//  1. Enroll a signing key binding (real ECDSA + OIDC identity token)
//  2. Create a deployment with a user signature → provenance attached
//  3. Orchestration assembles an Attestation and passes it to the delivery agent
//  4. The delivery agent's attestation is verified end-to-end
func TestEndToEnd_CreateDeployment_AssemblesAndVerifiesAttestation(t *testing.T) {
	provider := oidctest.Start(t, oidctest.WithAudience("fleetshift-enroll"))
	signerID := domain.SubjectID("e2e-user")
	issuer := provider.IssuerURL()

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	pubJWK := ecPubKeyJWK(t, &privKey.PublicKey)

	identityToken := provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  string(signerID),
		Audience: "fleetshift-enroll",
	})

	kbDoc, kbSig := buildKeyBindingDoc(t, privKey, signerID)

	store := newStore(t)
	inner := &sqlite.RecordingDeliveryService{
		Store: store,
		Now:   func() time.Time { return time.Date(2026, 3, 11, 12, 0, 0, 0, time.UTC) },
	}
	agent := &capturingDeliveryAgent{inner: inner}
	h := setupWithStoreAndAgent(t, store, agent)

	registerTargets(t, h, "t1")

	enrollKeyBindingDirect(t, store, domain.SigningKeyBinding{
		ID:                  "skb-e2e",
		FederatedIdentity:   domain.FederatedIdentity{Subject: signerID, Issuer: issuer},
		PublicKeyJWK:        pubJWK,
		Algorithm:           "ES256",
		KeyBindingDoc:       kbDoc,
		KeyBindingSignature: kbSig,
		IdentityToken:       domain.RawToken(identityToken),
		CreatedAt:           time.Now().UTC(),
		ExpiresAt:           time.Now().Add(365 * 24 * time.Hour).UTC(),
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

	dep = awaitDeploymentState(ctx, t, store, "e2e-dep", domain.DeploymentStateActive)

	att := agent.capturedAttestation()
	if att == nil {
		t.Fatal("delivery agent received nil Attestation; expected assembled attestation")
	}

	if att.Input.Content.DeploymentID != "e2e-dep" {
		t.Errorf("Attestation.Input.Content.DeploymentID = %q, want %q",
			att.Input.Content.DeploymentID, "e2e-dep")
	}
	if att.Input.KeyBinding.ID != "skb-e2e" {
		t.Errorf("Attestation.Input.KeyBinding.ID = %q, want %q",
			att.Input.KeyBinding.ID, "skb-e2e")
	}
	put, ok := att.Output.(*domain.PutManifests)
	if !ok {
		t.Fatalf("Attestation.Output is %T, want *PutManifests", att.Output)
	}
	if len(put.Manifests) == 0 {
		t.Fatal("PutManifests.Manifests is empty")
	}

	// Verify the attestation using the delivery-agent-side verifier.
	jwksURI := string(issuer) + "/jwks"
	verifier := attestation.NewVerifier(
		map[domain.IssuerURL]attestation.TrustedIssuer{
			issuer: {
				JWKSURI:  domain.EndpointURL(jwksURI),
				Audience: "fleetshift-enroll",
			},
		},
		attestation.WithHTTPClient(provider.HTTPClient()),
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

func (a *capturingDeliveryAgent) Remove(ctx context.Context, target domain.TargetInfo, id domain.DeliveryID, manifests []domain.Manifest, auth domain.DeliveryAuth, att *domain.Attestation, signaler *domain.DeliverySignaler) error {
	return a.inner.Remove(ctx, target, id, manifests, auth, att, signaler)
}

func (a *capturingDeliveryAgent) capturedAttestation() *domain.Attestation {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.att
}

func enrollKeyBindingDirect(t *testing.T, store domain.Store, kb domain.SigningKeyBinding) {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback()
	if err := tx.SigningKeyBindings().Create(ctx, kb); err != nil {
		t.Fatalf("create signing key binding: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

func buildKeyBindingDoc(t *testing.T, privKey *ecdsa.PrivateKey, signerID domain.SubjectID) (doc, sig []byte) {
	t.Helper()
	ecdhKey, err := privKey.PublicKey.ECDH()
	if err != nil {
		t.Fatalf("ECDH: %v", err)
	}
	kbDoc := map[string]string{
		"public_key": base64.RawURLEncoding.EncodeToString(ecdhKey.Bytes()),
		"signer_id":  string(signerID),
	}
	docBytes, err := json.Marshal(kbDoc)
	if err != nil {
		t.Fatalf("marshal kb doc: %v", err)
	}
	hash := sha256.Sum256(docBytes)
	sigBytes, err := ecdsa.SignASN1(rand.Reader, privKey, hash[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return docBytes, sigBytes
}

