package attestation_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/attestation"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
)

type testHarness struct {
	provider        *oidctest.Provider
	privKey         *ecdsa.PrivateKey
	signerID        domain.SubjectID
	issuer          domain.IssuerURL
	identityToken   string
	registrySubject domain.RegistrySubject
	fakeReg         *keyregistry.Fake
	verifier        *attestation.Verifier
}

func setupHarness(t *testing.T) *testHarness {
	t.Helper()

	provider := oidctest.Start(t, oidctest.WithAudience("fleetshift-enroll"))

	signerID := domain.SubjectID("test-user")
	issuer := provider.IssuerURL()
	registrySubject := domain.RegistrySubject("gh-test-user")

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	identityToken := provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  string(signerID),
		Audience: "fleetshift-enroll",
		Extra:    map[string]any{"preferred_username": registrySubject},
	})

	fakeReg := keyregistry.NewFake()
	fakeReg.Register("https://api.github.com", registrySubject, &privKey.PublicKey)

	keyResolver := &application.KeyResolver{
		Registries: domain.BuiltInKeyRegistries(),
		Clients: map[domain.KeyRegistryType]domain.RegistryClient{
			domain.KeyRegistryTypeGitHub: fakeReg,
		},
	}

	jwksURI := string(issuer) + "/jwks"
	verifier := attestation.NewVerifier(
		map[domain.IssuerURL]attestation.TrustedIssuer{
			issuer: {
				JWKSURI:  domain.EndpointURL(jwksURI),
				Audience: "fleetshift-enroll",
				RegistrySubjectMapping: &domain.RegistrySubjectMapping{
					RegistryID: "github.com",
					Expression: `claims.preferred_username`,
				},
			},
		},
		attestation.WithHTTPClient(provider.HTTPClient()),
		attestation.WithClock(func() time.Time { return time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC) }),
		attestation.WithKeyResolver(keyResolver),
	)

	return &testHarness{
		provider:        provider,
		privKey:         privKey,
		signerID:        signerID,
		issuer:          issuer,
		identityToken:   identityToken,
		registrySubject: registrySubject,
		fakeReg:         fakeReg,
		verifier:        verifier,
	}
}

func (h *testHarness) buildValidAttestation(t *testing.T) *domain.Attestation {
	t.Helper()
	manifests := []domain.Manifest{{
		ResourceType: "kubernetes",
		Raw:          json.RawMessage(`{"kind":"ConfigMap","apiVersion":"v1","metadata":{"name":"test","namespace":"default"}}`),
	}}
	ms := domain.ManifestStrategySpec{
		Type:      domain.ManifestStrategyInline,
		Manifests: manifests,
	}
	ps := domain.PlacementStrategySpec{
		Type:    domain.PlacementStrategyStatic,
		Targets: []domain.TargetID{"t1"},
	}
	validUntil := time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
	gen := domain.Generation(1)

	envelope, err := domain.BuildSignedInputEnvelope("dep-1", ms, ps, validUntil, nil, gen)
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	envelopeHash := domain.HashIntent(envelope)

	sigBytes := signEnvelope(t, h.privKey, envelope)

	return &domain.Attestation{
		Input: domain.SignedInput{
			Content: domain.DeploymentContent{
				DeploymentID:      "dep-1",
				ManifestStrategy:  ms,
				PlacementStrategy: ps,
			},
			Sig: domain.Signature{
				Signer:         domain.FederatedIdentity{Subject: h.signerID, Issuer: h.issuer},
				ContentHash:    envelopeHash,
				SignatureBytes: sigBytes,
			},
			Signer: domain.SignerAssertion{
				IdentityToken:   domain.RawToken(h.identityToken),
				RegistryID:      "github.com",
				RegistrySubject: h.registrySubject,
			},
			ValidUntil:         validUntil,
			ExpectedGeneration: gen,
		},
		Output: &domain.PutManifests{
			Manifests: manifests,
		},
	}
}

func TestVerifyAttestation_HappyPath(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)

	err := h.verifier.Verify(context.Background(), att)
	if err != nil {
		t.Fatalf("expected verification to pass, got: %v", err)
	}
}

func TestVerifyAttestation_BadSignature(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)
	att.Input.Sig.SignatureBytes = []byte("tampered-sig")

	err := h.verifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail with bad signature")
	}
}

func TestVerifyAttestation_Expired(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)

	expiredVerifier := attestation.NewVerifier(
		h.verifier.TrustedIssuers(),
		attestation.WithHTTPClient(h.provider.HTTPClient()),
		attestation.WithClock(func() time.Time { return time.Date(2028, 1, 1, 0, 0, 0, 0, time.UTC) }),
		attestation.WithKeyResolver(&application.KeyResolver{
			Registries: domain.BuiltInKeyRegistries(),
			Clients: map[domain.KeyRegistryType]domain.RegistryClient{
				domain.KeyRegistryTypeGitHub: h.fakeReg,
			},
		}),
	)

	err := expiredVerifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail for expired attestation")
	}
}

func TestVerifyAttestation_SignerSubjectMismatch(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)
	att.Input.Sig.Signer.Subject = "wrong-signer"

	err := h.verifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail for signer mismatch")
	}
}

func TestVerifyAttestation_RegistrySubjectMismatch(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)
	att.Input.Signer.RegistrySubject = "wrong-registry-subject"

	err := h.verifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail for registry subject mismatch")
	}
}

func TestVerifyAttestation_ManifestContentMismatch(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)
	att.Output = &domain.PutManifests{
		Manifests: []domain.Manifest{{
			ResourceType: "kubernetes",
			Raw:          json.RawMessage(`{"tampered":true}`),
		}},
	}

	err := h.verifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail for manifest mismatch")
	}
}

func TestVerifyAttestation_RemoveDeploymentIDMismatch(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)
	att.Output = &domain.RemoveByDeploymentId{
		DeploymentID: "wrong-dep",
	}

	err := h.verifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail for remove deployment ID mismatch")
	}
}

func TestVerifyAttestation_RemoveDeploymentIDMatch(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)
	att.Output = &domain.RemoveByDeploymentId{
		DeploymentID: "dep-1",
	}

	err := h.verifier.Verify(context.Background(), att)
	if err != nil {
		t.Fatalf("expected verification to pass for matching remove deployment ID, got: %v", err)
	}
}

func TestVerifyAttestation_UntrustedIssuer(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)
	att.Input.Sig.Signer.Issuer = "https://evil.example.com"

	err := h.verifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail for untrusted issuer")
	}
}

func TestVerifyAttestation_IdentityTokenSubjectMismatch(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)

	wrongToken := h.provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  "wrong-user",
		Audience: "fleetshift-enroll",
		Extra:    map[string]any{"preferred_username": h.registrySubject},
	})
	att.Input.Signer.IdentityToken = domain.RawToken(wrongToken)

	err := h.verifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail for identity token subject mismatch")
	}
}

func TestVerifyAttestation_ContentHashMismatch(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)
	att.Input.Sig.ContentHash = []byte("wrong-hash")

	err := h.verifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail for content hash mismatch")
	}
}

func TestVerifyAttestation_NoKeyResolver(t *testing.T) {
	provider := oidctest.Start(t, oidctest.WithAudience("fleetshift-enroll"))
	issuer := provider.IssuerURL()

	verifier := attestation.NewVerifier(
		map[domain.IssuerURL]attestation.TrustedIssuer{
			issuer: {
				JWKSURI:  domain.EndpointURL(string(issuer) + "/jwks"),
				Audience: "fleetshift-enroll",
			},
		},
		attestation.WithHTTPClient(provider.HTTPClient()),
	)

	h := setupHarness(t)
	att := h.buildValidAttestation(t)
	att.Input.Sig.Signer.Issuer = issuer

	err := verifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail without key resolver")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func signDoc(t *testing.T, privKey *ecdsa.PrivateKey, doc []byte) []byte {
	t.Helper()
	hash := sha256.Sum256(doc)
	sig, err := ecdsa.SignASN1(rand.Reader, privKey, hash[:])
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return sig
}

func signEnvelope(t *testing.T, privKey *ecdsa.PrivateKey, envelope []byte) []byte {
	t.Helper()
	return signDoc(t, privKey, envelope)
}
