// Package attestation implements the delivery-agent-side attestation
// verification algorithm. It is designed to be independent of the
// server's OIDC infrastructure — each [Verifier] owns its own JWKS
// cache and trust configuration.
package attestation

import (
	"bytes"
	"context"
	"crypto"
	"crypto/ecdsa"
	"fmt"
	"net/http"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
	fscrypto "github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/crypto"
)

// TrustedIssuer describes an OIDC issuer the delivery agent trusts.
type TrustedIssuer struct {
	JWKSURI                 domain.EndpointURL
	Audience                domain.Audience // enrollment audience
	PublicKeyClaimExpression string
	RegistrySubjectMapping  *domain.RegistrySubjectMapping
}

// Verifier verifies attestation bundles. It owns a JWKS cache for
// identity token verification, populated lazily from the trusted
// issuers' JWKS endpoints via the injected [http.Client].
//
// Construct with [NewVerifier]; the zero value is not usable.
type Verifier struct {
	// TODO: this probably needs to be some store abstraction
	trustedIssuers map[domain.IssuerURL]TrustedIssuer
	now            func() time.Time
	jwks           *jwksFetcher
	keyResolver    *application.KeyResolver
}

// VerifierOption configures a [Verifier].
type VerifierOption func(*Verifier)

// WithHTTPClient sets the HTTP client used to fetch JWKS endpoints.
// Defaults to [http.DefaultClient].
func WithHTTPClient(c *http.Client) VerifierOption {
	return func(v *Verifier) { v.jwks = newJWKSFetcher(c) }
}

// WithClock overrides the wall-clock used for temporal validation
// (e.g. attestation expiry). Defaults to [time.Now].
func WithClock(now func() time.Time) VerifierOption {
	return func(v *Verifier) { v.now = now }
}

// WithKeyResolver sets the key resolver used to fetch signing keys
// from external registries.
func WithKeyResolver(r *application.KeyResolver) VerifierOption {
	return func(v *Verifier) { v.keyResolver = r }
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

// TrustedIssuers returns the trust bundle.
func (v *Verifier) TrustedIssuers() map[domain.IssuerURL]TrustedIssuer {
	return v.trustedIssuers
}

// Verify verifies the full attestation bundle: signed input followed
// by output verification.
func (v *Verifier) Verify(ctx context.Context, att *domain.Attestation) error {
	if err := v.verifySignedInput(ctx, &att.Input); err != nil {
		return fmt.Errorf("signed input verification: %w", err)
	}
	if err := verifyOutput(&att.Input, att.Output); err != nil {
		return fmt.Errorf("output verification: %w", err)
	}
	return nil
}

// verifySignedInput implements the updated verification sequence:
//
//  1. Issuer trusted
//  2. Identity token valid (JWT signature via JWKS, aud check, skip exp)
//  3. Subject claim matches
//  4. Registry subject derivation (CEL mapping)
//  5. Key resolution (fetch from registry)
//  6. Envelope reconstruction
//  7. Signature verification
//  8. Temporal validity
func (v *Verifier) verifySignedInput(ctx context.Context, input *domain.SignedInput) error {
	sig := &input.Sig
	sa := &input.Signer

	// 1. Issuer trusted
	trusted, ok := v.trustedIssuers[sig.Signer.Issuer]
	if !ok {
		return fmt.Errorf("untrusted issuer: %s", sig.Signer.Issuer)
	}

	// 2. Identity token valid: verify JWT signature against JWKS, check
	//    aud matches enrollment audience, skip exp.
	if err := v.verifyIdentityToken(ctx, string(sa.IdentityToken), trusted); err != nil {
		return fmt.Errorf("identity token verification: %w", err)
	}

	// 3. Subject claim matches: verified JWT sub == sig.Signer.Subject
	sub, err := extractSubjectFromToken(string(sa.IdentityToken))
	if err != nil {
		return fmt.Errorf("extract identity token subject: %w", err)
	}
	if domain.SubjectID(sub) != sig.Signer.Subject {
		return fmt.Errorf("identity token subject %q != signer subject %q", sub, sig.Signer.Subject)
	}

	// 4. Registry subject derivation: evaluate the issuer's CEL mapping
	//    over the ID token claims and confirm the result matches the
	//    assertion's registry subject.
	if trusted.RegistrySubjectMapping != nil {
		derived, err := application.EvalClaimMapping(trusted.RegistrySubjectMapping, string(sa.IdentityToken))
		if err != nil {
			return fmt.Errorf("registry subject derivation: %w", err)
		}
		if derived != sa.RegistrySubject {
			return fmt.Errorf("registry subject mismatch: derived %q != asserted %q", derived, sa.RegistrySubject)
		}
	}

	// 5. Key resolution: for OIDC registries evaluate the CEL
	//    expression to extract the public key from the verified ID
	//    token; for external registries (e.g. GitHub) delegate to
	//    the KeyResolver.
	var keys []crypto.PublicKey
	if trusted.PublicKeyClaimExpression != "" {
		base64Key, celErr := application.EvalCELClaim(trusted.PublicKeyClaimExpression, string(sa.IdentityToken))
		if celErr != nil {
			return fmt.Errorf("extract public key claim from identity token: %w", celErr)
		}
		keys, err = keyregistry.ParsePublicKeyFromBase64(base64Key)
		if err != nil {
			return fmt.Errorf("parse public key from identity token: %w", err)
		}
	} else {
		keys, err = v.resolveKeys(ctx, sa.RegistryID, sa.RegistrySubject)
		if err != nil {
			return fmt.Errorf("key resolution: %w", err)
		}
	}

	// 6. Envelope reconstruction
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

	// 7. Signature verification: try each resolved key.
	if err := verifyAgainstKeySet(envelope, sig.SignatureBytes, keys); err != nil {
		return fmt.Errorf("signature verification: %w", err)
	}

	// 8. Temporal validity
	now := v.now()
	if now.After(input.ValidUntil) {
		return fmt.Errorf("attestation expired: valid_until %s, now %s", input.ValidUntil, now)
	}

	return nil
}

func (v *Verifier) resolveKeys(ctx context.Context, registryID domain.KeyRegistryID, registrySubject domain.RegistrySubject) ([]crypto.PublicKey, error) {
	if v.keyResolver == nil {
		return nil, fmt.Errorf("no key resolver configured")
	}
	return v.keyResolver.Resolve(ctx, registryID, registrySubject)
}

func verifyAgainstKeySet(doc, sig []byte, keys []crypto.PublicKey) error {
	for _, k := range keys {
		ecKey, ok := k.(*ecdsa.PublicKey)
		if !ok {
			continue
		}
		if err := fscrypto.VerifyECDSASignature(ecKey, doc, sig); err == nil {
			return nil
		}
	}
	return fmt.Errorf("no key in the set verified the signature")
}

// verifyOutput verifies that the delivery output is consistent with
// the signed input.
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
