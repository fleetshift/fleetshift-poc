package domain

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"time"

	fscrypto "github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/crypto"
)

// ProvenanceService constructs and verifies [Provenance] for signed
// mutations. It resolves the caller's enrolled signing keys (from an
// external registry or via OIDC token claims) and verifies user
// signatures against the canonical envelope.
type ProvenanceService struct {
	KeyResolver *KeyResolver
	AuthMethods AuthMethodRepository
}

// BuildDeploymentProvenance verifies a detached user signature over
// the deployment intent envelope and returns the persisted [Provenance].
func (s *ProvenanceService) BuildDeploymentProvenance(
	ctx context.Context,
	enrollments SignerEnrollmentRepository,
	caller *SubjectClaims,
	id DeploymentID,
	ms ManifestStrategySpec,
	ps PlacementStrategySpec,
	generation Generation,
	userSig []byte,
	validUntil time.Time,
) (*Provenance, error) {
	enrollment, err := s.loadEnrollment(ctx, enrollments, caller)
	if err != nil {
		return nil, err
	}

	envelopeBytes, err := BuildSignedInputEnvelope(id, ms, ps, validUntil, nil, generation)
	if err != nil {
		return nil, fmt.Errorf("build signed input envelope: %w", err)
	}
	envelopeHash := HashIntent(envelopeBytes)

	keys, err := s.resolveSigningKeys(ctx, enrollment)
	if err != nil {
		return nil, fmt.Errorf("resolve signing keys: %w", err)
	}

	if err := verifySignatureAgainstKeySet(envelopeBytes, userSig, keys); err != nil {
		return nil, fmt.Errorf("%w: signature verification failed", ErrInvalidArgument)
	}

	return &Provenance{
		Content: DeploymentContent{
			DeploymentID:      id,
			ManifestStrategy:  ms,
			PlacementStrategy: ps,
		},
		Sig: Signature{
			Signer:         caller.FederatedIdentity,
			ContentHash:    envelopeHash,
			SignatureBytes: userSig,
		},
		ValidUntil:         validUntil,
		ExpectedGeneration: generation,
		OutputConstraints:  nil,
	}, nil
}

// BuildManagedResourceProvenance verifies a detached user signature over
// the managed resource intent envelope and returns the persisted
// [Provenance].
func (s *ProvenanceService) BuildManagedResourceProvenance(
	ctx context.Context,
	enrollments SignerEnrollmentRepository,
	caller *SubjectClaims,
	resourceType ResourceType,
	resourceName ResourceName,
	spec json.RawMessage,
	generation Generation,
	userSig []byte,
	validUntil time.Time,
) (*Provenance, error) {
	enrollment, err := s.loadEnrollment(ctx, enrollments, caller)
	if err != nil {
		return nil, err
	}

	envelopeBytes, err := BuildManagedResourceEnvelope(
		resourceType,
		resourceName,
		spec,
		validUntil,
		nil,
		generation,
	)
	if err != nil {
		return nil, fmt.Errorf("build managed resource envelope: %w", err)
	}
	envelopeHash := HashIntent(envelopeBytes)

	keys, err := s.resolveSigningKeys(ctx, enrollment)
	if err != nil {
		return nil, fmt.Errorf("resolve signing keys: %w", err)
	}

	if err := verifySignatureAgainstKeySet(envelopeBytes, userSig, keys); err != nil {
		return nil, fmt.Errorf("%w: signature verification failed", ErrInvalidArgument)
	}

	return &Provenance{
		Content: ManagedResourceContent{
			ResourceType: resourceType,
			ResourceName: resourceName,
			Spec:         spec,
		},
		Sig: Signature{
			Signer:         caller.FederatedIdentity,
			ContentHash:    envelopeHash,
			SignatureBytes: userSig,
		},
		ValidUntil:         validUntil,
		ExpectedGeneration: generation,
		OutputConstraints:  nil,
	}, nil
}

// VerifySignature checks whether sig is a valid ECDSA signature over
// doc for the given caller's enrolled signing key. Used for test-sign
// verification after enrollment.
func (s *ProvenanceService) VerifySignature(
	ctx context.Context,
	enrollments SignerEnrollmentRepository,
	caller *SubjectClaims,
	doc, sig []byte,
) error {
	enrollment, err := s.loadEnrollment(ctx, enrollments, caller)
	if err != nil {
		return err
	}
	keys, err := s.resolveSigningKeys(ctx, enrollment)
	if err != nil {
		return fmt.Errorf("resolve signing keys: %w", err)
	}
	if err := verifySignatureAgainstKeySet(doc, sig, keys); err != nil {
		return fmt.Errorf("%w: signature verification failed", ErrInvalidArgument)
	}
	return nil
}

func (s *ProvenanceService) loadEnrollment(
	ctx context.Context,
	enrollments SignerEnrollmentRepository,
	caller *SubjectClaims,
) (SignerEnrollment, error) {
	if caller == nil {
		return SignerEnrollment{}, fmt.Errorf("%w: caller identity required for provenance", ErrInvalidArgument)
	}
	found, err := enrollments.ListBySubject(ctx, caller.FederatedIdentity)
	if err != nil {
		return SignerEnrollment{}, fmt.Errorf("list signer enrollments: %w", err)
	}
	if len(found) == 0 {
		return SignerEnrollment{}, fmt.Errorf("%w: no signer enrollment found for %s", ErrInvalidArgument, caller.Subject)
	}

	// TODO: just getting the first one?
	enrollment := found[0]
	if !enrollment.ExpiresAt.IsZero() && time.Now().After(enrollment.ExpiresAt) {
		return SignerEnrollment{}, fmt.Errorf("%w: signer enrollment %s has expired", ErrInvalidArgument, enrollment.ID)
	}

	return enrollment, nil
}

// resolveSigningKeys returns the public keys for a signer enrollment.
// For OIDC enrollments (no external registry) the key is extracted
// from the enrollment's identity token via the configured CEL
// expression. For external registries (GitHub) it delegates to
// KeyResolver.
func (s *ProvenanceService) resolveSigningKeys(ctx context.Context, enrollment SignerEnrollment) ([]crypto.PublicKey, error) {
	if enrollment.RegistryID == "oidc" {
		oidcConfig, err := s.loadOIDCConfig(ctx)
		if err != nil {
			return nil, err
		}
		if oidcConfig.PublicKeyClaimExpression == "" {
			return nil, fmt.Errorf("OIDC auth method has no public_key_claim_expression configured")
		}
		base64Key, err := EvalCELClaim(oidcConfig.PublicKeyClaimExpression, string(enrollment.IdentityToken))
		if err != nil {
			return nil, fmt.Errorf("evaluate public key claim: %w", err)
		}
		return ParsePublicKeyFromBase64(base64Key)
	}
	return s.KeyResolver.Resolve(ctx, enrollment.RegistryID, enrollment.RegistrySubject)
}

// verifySignatureAgainstKeySet tries each public key in the set until
// one successfully verifies the ECDSA signature. Returns an error if
// none succeed.
func verifySignatureAgainstKeySet(doc, sig []byte, keys []crypto.PublicKey) error {
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

func (s *ProvenanceService) loadOIDCConfig(ctx context.Context) (OIDCConfig, error) {
	methods, err := s.AuthMethods.List(ctx)
	if err != nil {
		return OIDCConfig{}, fmt.Errorf("list auth methods: %w", err)
	}
	for _, m := range methods {
		if m.Type == AuthMethodTypeOIDC && m.OIDC != nil {
			return *m.OIDC, nil
		}
	}
	return OIDCConfig{}, fmt.Errorf("no OIDC auth method configured")
}
