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

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/attestation"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/oidc/oidctest"
)

// testHarness holds all the cryptographic material needed to build a
// valid attestation for testing.
type testHarness struct {
	provider      *oidctest.Provider
	privKey       *ecdsa.PrivateKey
	pubKeyJWK     json.RawMessage
	pubKeyBytes   []byte
	signerID      domain.SubjectID
	issuer        domain.IssuerURL
	identityToken string
	keyBindingDoc []byte
	keyBindingSig []byte
	verifier      *attestation.Verifier
}

func setupHarness(t *testing.T) *testHarness {
	t.Helper()

	provider := oidctest.Start(t, oidctest.WithAudience("fleetshift-enroll"))

	signerID := domain.SubjectID("test-user")
	issuer := provider.IssuerURL()

	privKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	pubKeyJWK := ecPublicKeyToJWK(t, &privKey.PublicKey)
	ecdhKey, err := privKey.PublicKey.ECDH()
	if err != nil {
		t.Fatalf("ECDH: %v", err)
	}
	pubKeyBytes := ecdhKey.Bytes()

	identityToken := provider.IssueToken(t, oidctest.TokenClaims{
		Subject:  string(signerID),
		Audience: "fleetshift-enroll",
	})

	kbDoc, kbSig := buildKeyBinding(t, privKey, pubKeyJWK, signerID)

	jwksURI := string(issuer) + "/jwks"
	verifier := attestation.NewVerifier(
		map[domain.IssuerURL]attestation.TrustedIssuer{
			issuer: {
				JWKSURI:  domain.EndpointURL(jwksURI),
				Audience: "fleetshift-enroll",
			},
		},
		attestation.WithHTTPClient(provider.HTTPClient()),
		attestation.WithClock(func() time.Time { return time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC) }),
	)

	return &testHarness{
		provider:      provider,
		privKey:       privKey,
		pubKeyJWK:     pubKeyJWK,
		pubKeyBytes:   pubKeyBytes,
		signerID:      signerID,
		issuer:        issuer,
		identityToken: identityToken,
		keyBindingDoc: kbDoc,
		keyBindingSig: kbSig,
		verifier:      verifier,
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
				PublicKey:      h.pubKeyBytes,
				ContentHash:    envelopeHash,
				SignatureBytes: sigBytes,
			},
			KeyBinding: domain.SigningKeyBinding{
				ID: "kb-1",
				FederatedIdentity: domain.FederatedIdentity{
					Subject: h.signerID,
					Issuer:  h.issuer,
				},
				PublicKeyJWK:        h.pubKeyJWK,
				Algorithm:           "ES256",
				KeyBindingDoc:       h.keyBindingDoc,
				KeyBindingSignature: h.keyBindingSig,
				IdentityToken:       domain.RawToken(h.identityToken),
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
	)

	err := expiredVerifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail for expired attestation")
	}
}

func TestVerifyAttestation_KeyBindingPoPFailure(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)
	att.Input.KeyBinding.KeyBindingSignature = []byte("wrong-sig")

	err := h.verifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail for bad key binding PoP")
	}
}

func TestVerifyAttestation_SignerMismatch(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)
	att.Input.Sig.Signer.Subject = "wrong-signer"

	err := h.verifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail for signer mismatch")
	}
}

func TestVerifyAttestation_PublicKeyMismatch(t *testing.T) {
	h := setupHarness(t)
	att := h.buildValidAttestation(t)

	otherKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	otherECDH, _ := otherKey.PublicKey.ECDH()
	att.Input.Sig.PublicKey = otherECDH.Bytes()

	err := h.verifier.Verify(context.Background(), att)
	if err == nil {
		t.Fatal("expected verification to fail for public key mismatch")
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
	att.Input.KeyBinding.Issuer = "https://evil.example.com"

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
	})
	att.Input.KeyBinding.IdentityToken = domain.RawToken(wrongToken)

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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func ecPublicKeyToJWK(t *testing.T, pub *ecdsa.PublicKey) json.RawMessage {
	t.Helper()
	xBytes := pub.X.Bytes()
	yBytes := pub.Y.Bytes()
	padTo32 := func(b []byte) []byte {
		if len(b) >= 32 {
			return b
		}
		padded := make([]byte, 32)
		copy(padded[32-len(b):], b)
		return padded
	}
	xBytes = padTo32(xBytes)
	yBytes = padTo32(yBytes)
	jwk := map[string]string{
		"kty": "EC",
		"crv": "P-256",
		"x":   b64url(xBytes),
		"y":   b64url(yBytes),
	}
	raw, err := json.Marshal(jwk)
	if err != nil {
		t.Fatalf("marshal JWK: %v", err)
	}
	return raw
}

func b64url(data []byte) string {
	const enc = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	n := len(data)
	buf := make([]byte, 0, (n*4+2)/3)
	for i := 0; i < n; i += 3 {
		val := uint(data[i]) << 16
		if i+1 < n {
			val |= uint(data[i+1]) << 8
		}
		if i+2 < n {
			val |= uint(data[i+2])
		}
		buf = append(buf, enc[(val>>18)&0x3f])
		buf = append(buf, enc[(val>>12)&0x3f])
		if i+1 < n {
			buf = append(buf, enc[(val>>6)&0x3f])
		}
		if i+2 < n {
			buf = append(buf, enc[val&0x3f])
		}
	}
	return string(buf)
}

func buildKeyBinding(t *testing.T, privKey *ecdsa.PrivateKey, pubKeyJWK json.RawMessage, signerID domain.SubjectID) (doc, sig []byte) {
	t.Helper()
	ecdhKey, _ := privKey.PublicKey.ECDH()
	kbDoc := map[string]string{
		"public_key": b64url(ecdhKey.Bytes()),
		"signer_id":  string(signerID),
	}
	docBytes, err := json.Marshal(kbDoc)
	if err != nil {
		t.Fatalf("marshal kb doc: %v", err)
	}
	sigBytes := signDoc(t, privKey, docBytes)
	return docBytes, sigBytes
}

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

