// Package attestation implements the delivery-agent-side attestation
// verification algorithm. It is designed to be independent of the
// server's OIDC infrastructure — each [Verifier] owns its own JWKS
// cache and trust configuration.
package attestation

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	fscrypto "github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/crypto"
)

// TrustedIssuer describes an OIDC issuer the delivery agent trusts.
type TrustedIssuer struct {
	JWKSURI  domain.EndpointURL
	Audience domain.Audience // enrollment audience; checked to prevent token reuse
}

// Verifier verifies attestation bundles. It owns a JWKS cache for
// identity token verification, populated lazily from the trusted
// issuers' JWKS endpoints via the injected [http.Client].
//
// Construct with [NewVerifier]; the zero value is not usable.
type Verifier struct {
	trustedIssuers map[domain.IssuerURL]TrustedIssuer
	now            func() time.Time
	jwks           *jwksFetcher
}

// VerifierOption configures a [Verifier].
type VerifierOption func(*Verifier)

// WithHTTPClient sets the HTTP client used to fetch JWKS endpoints.
// Defaults to [http.DefaultClient]. Tests typically pass the oidctest
// provider's client so JWKS fetches stay in-process.
func WithHTTPClient(c *http.Client) VerifierOption {
	return func(v *Verifier) { v.jwks = newJWKSFetcher(c) }
}

// WithClock overrides the wall-clock used for temporal validation
// (e.g. attestation expiry). Defaults to [time.Now].
func WithClock(now func() time.Time) VerifierOption {
	return func(v *Verifier) { v.now = now }
}

// NewVerifier creates a Verifier with the given trust bundle and
// options. The trust bundle maps issuer URLs to their JWKS endpoints
// and expected enrollment audiences.
func NewVerifier(issuers map[domain.IssuerURL]TrustedIssuer, opts ...VerifierOption) *Verifier {
	v := &Verifier{
		trustedIssuers: issuers,
		now:            time.Now,
		jwks:           newJWKSFetcher(nil),
	}
	for _, o := range opts {
		o(v)
	}
	return v
}

// TrustedIssuers returns the trust bundle. Useful when constructing a
// derived verifier with different options (e.g. a different clock in
// tests).
func (v *Verifier) TrustedIssuers() map[domain.IssuerURL]TrustedIssuer {
	return v.trustedIssuers
}

// Verify verifies the full attestation bundle: signed input
// (9-step verification) followed by output verification.
func (v *Verifier) Verify(ctx context.Context, att *domain.Attestation) error {
	if err := v.verifySignedInput(ctx, &att.Input); err != nil {
		return fmt.Errorf("signed input verification: %w", err)
	}
	if err := verifyOutput(&att.Input, att.Output); err != nil {
		return fmt.Errorf("output verification: %w", err)
	}
	return nil
}

// verifySignedInput implements the 9-step SignedInput verification,
// mapped from the hybrid PoC's SignedInput.verify method.
func (v *Verifier) verifySignedInput(ctx context.Context, input *domain.SignedInput) error {
	sig := &input.Sig
	kb := &input.KeyBinding

	// 1. Signer consistency: signature and key binding identify the same subject.
	if sig.Signer != kb.FederatedIdentity {
		return fmt.Errorf("signer mismatch: signature %v != key binding %v", sig.Signer, kb.FederatedIdentity)
	}

	// 2. Issuer trusted: sig.Signer.Issuer exists in trustedIssuers
	trusted, ok := v.trustedIssuers[sig.Signer.Issuer]
	if !ok {
		return fmt.Errorf("untrusted issuer: %s", sig.Signer.Issuer)
	}

	// 3. Identity token valid: verify JWT signature against JWKS, check
	//    aud matches enrollment audience, skip exp.
	if err := v.verifyIdentityToken(ctx, string(kb.IdentityToken), trusted); err != nil {
		return fmt.Errorf("identity token verification: %w", err)
	}

	// 4. Subject claim matches: verified JWT sub == keyBinding.Subject
	sub, err := extractSubjectFromToken(string(kb.IdentityToken))
	if err != nil {
		return fmt.Errorf("extract identity token subject: %w", err)
	}
	if domain.SubjectID(sub) != kb.Subject {
		return fmt.Errorf("identity token subject %q != key binding subject %q", sub, kb.Subject)
	}

	// 5. Proof of possession: verify keyBinding.KeyBindingSignature over
	//    keyBinding.KeyBindingDoc using public key from keyBinding.PublicKeyJWK
	pubKey, err := fscrypto.ParseECPublicKeyFromJWK(kb.PublicKeyJWK)
	if err != nil {
		return fmt.Errorf("parse key binding public key: %w", err)
	}
	if err := fscrypto.VerifyECDSASignature(pubKey, kb.KeyBindingDoc, kb.KeyBindingSignature); err != nil {
		return fmt.Errorf("key binding proof-of-possession: %w", err)
	}

	// 6. Public key consistency: raw bytes from sig.PublicKey ==
	//    parsed keyBinding.PublicKeyJWK
	ecdhKey, err := pubKey.ECDH()
	if err != nil {
		return fmt.Errorf("convert key binding public key to ECDH: %w", err)
	}
	if !bytes.Equal(sig.PublicKey, ecdhKey.Bytes()) {
		return fmt.Errorf("public key mismatch: signature key != key binding key")
	}

	// 7. Envelope reconstruction: BuildSignedInputEnvelope(content...)
	//    then hash(envelope) == sig.ContentHash
	envelope, err := domain.BuildSignedInputEnvelope(
		input.Content.DeploymentID,
		input.Content.ManifestStrategy,
		input.Content.PlacementStrategy,
		input.ValidUntil,
		input.OutputConstraints,
		input.ExpectedGeneration,
	)
	if err != nil {
		return fmt.Errorf("reconstruct signed input envelope: %w", err)
	}
	envelopeHash := domain.HashIntent(envelope)
	if !bytes.Equal(sig.ContentHash, envelopeHash) {
		return fmt.Errorf("content hash mismatch")
	}

	// 8. Signature verify: ECDSA verify sig.SignatureBytes over
	//    sig.ContentHash
	if err := fscrypto.VerifyECDSASignature(pubKey, envelope, sig.SignatureBytes); err != nil {
		return fmt.Errorf("signature verification: %w", err)
	}

	// 9. Temporal: now() <= input.ValidUntil
	now := v.now()
	if now.After(input.ValidUntil) {
		return fmt.Errorf("attestation expired: valid_until %s, now %s", input.ValidUntil, now)
	}

	return nil
}

// verifyOutput verifies that the delivery output is consistent with
// the signed input. For Cap 8 scope: inline manifest comparison and
// remove deployment ID match (no CEL evaluation).
func verifyOutput(input *domain.SignedInput, output domain.DeliveryOutput) error {
	switch o := output.(type) {
	case *domain.PutManifests:
		return verifyPutManifests(input, o)
	case *domain.RemoveByDeploymentId:
		return verifyRemoveByDeploymentId(input, o)
	default:
		return fmt.Errorf("unsupported delivery output type %T", output)
	}
}

// verifyPutManifests checks that inline manifests in the output match
// the manifests declared in the input's manifest strategy.
// TODO: CEL evaluation for non-inline strategies.
func verifyPutManifests(input *domain.SignedInput, put *domain.PutManifests) error {
	expected := input.Content.ManifestStrategy.Manifests
	actual := put.Manifests
	if len(expected) != len(actual) {
		return fmt.Errorf("manifest count mismatch: expected %d, got %d", len(expected), len(actual))
	}
	for i := range expected {
		if expected[i].ResourceType != actual[i].ResourceType {
			return fmt.Errorf("manifest[%d] resource type mismatch: expected %q, got %q",
				i, expected[i].ResourceType, actual[i].ResourceType)
		}
		if !bytes.Equal(expected[i].Raw, actual[i].Raw) {
			return fmt.Errorf("manifest[%d] content mismatch", i)
		}
	}
	return nil
}

func verifyRemoveByDeploymentId(input *domain.SignedInput, remove *domain.RemoveByDeploymentId) error {
	if remove.DeploymentID != input.Content.DeploymentID {
		return fmt.Errorf("remove deployment_id mismatch: output %q, input %q",
			remove.DeploymentID, input.Content.DeploymentID)
	}
	return nil
}
