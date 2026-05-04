package application

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
	fscrypto "github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/crypto"
)

// KeyResolverProvenanceBuilder adapts [KeyResolver] to the
// provenance-builder interfaces used by signed mutation workflows.
// AuthMethods is required for OIDC-based enrollments where the signing
// key is extracted from the identity token rather than an external
// registry.
type KeyResolverProvenanceBuilder struct {
	KeyResolver *KeyResolver
	AuthMethods domain.AuthMethodRepository
}

func (b *KeyResolverProvenanceBuilder) BuildProvenance(
	ctx context.Context,
	enrollments domain.SignerEnrollmentRepository,
	caller *domain.SubjectClaims,
	id domain.DeploymentID,
	ms domain.ManifestStrategySpec,
	ps domain.PlacementStrategySpec,
	generation domain.Generation,
	userSig []byte,
	validUntil time.Time,
) (*domain.Provenance, error) {
	enrollment, err := b.loadEnrollment(ctx, enrollments, caller)
	if err != nil {
		return nil, err
	}

	envelopeBytes, err := domain.BuildSignedInputEnvelope(id, ms, ps, validUntil, nil, generation)
	if err != nil {
		return nil, fmt.Errorf("build signed input envelope: %w", err)
	}
	envelopeHash := domain.HashIntent(envelopeBytes)

	keys, err := b.resolveSigningKeys(ctx, enrollment)
	if err != nil {
		return nil, fmt.Errorf("resolve signing keys: %w", err)
	}

	if err := verifySignatureAgainstKeySet(envelopeBytes, userSig, keys); err != nil {
		return nil, fmt.Errorf("%w: signature verification failed", domain.ErrInvalidArgument)
	}

	return &domain.Provenance{
		Content: domain.DeploymentContent{
			DeploymentID:      id,
			ManifestStrategy:  ms,
			PlacementStrategy: ps,
		},
		Sig: domain.Signature{
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
// [domain.Provenance].
func (b *KeyResolverProvenanceBuilder) BuildManagedResourceProvenance(
	ctx context.Context,
	enrollments domain.SignerEnrollmentRepository,
	caller *domain.SubjectClaims,
	resourceType domain.ResourceType,
	resourceName domain.ResourceName,
	spec json.RawMessage,
	generation domain.Generation,
	userSig []byte,
	validUntil time.Time,
) (*domain.Provenance, error) {
	enrollment, err := b.loadEnrollment(ctx, enrollments, caller)
	if err != nil {
		return nil, err
	}

	envelopeBytes, err := domain.BuildManagedResourceEnvelope(
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
	envelopeHash := domain.HashIntent(envelopeBytes)

	keys, err := b.resolveSigningKeys(ctx, enrollment)
	if err != nil {
		return nil, fmt.Errorf("resolve signing keys: %w", err)
	}

	if err := verifySignatureAgainstKeySet(envelopeBytes, userSig, keys); err != nil {
		return nil, fmt.Errorf("%w: signature verification failed", domain.ErrInvalidArgument)
	}

	return &domain.Provenance{
		Content: domain.ManagedResourceContent{
			ResourceType: resourceType,
			ResourceName: resourceName,
			Spec:         spec,
		},
		Sig: domain.Signature{
			Signer:         caller.FederatedIdentity,
			ContentHash:    envelopeHash,
			SignatureBytes: userSig,
		},
		ValidUntil:         validUntil,
		ExpectedGeneration: generation,
		OutputConstraints:  nil,
	}, nil
}

func (b *KeyResolverProvenanceBuilder) loadEnrollment(
	ctx context.Context,
	enrollments domain.SignerEnrollmentRepository,
	caller *domain.SubjectClaims,
) (domain.SignerEnrollment, error) {
	if caller == nil {
		return domain.SignerEnrollment{}, fmt.Errorf("%w: caller identity required for provenance", domain.ErrInvalidArgument)
	}
	found, err := enrollments.ListBySubject(ctx, caller.FederatedIdentity)
	if err != nil {
		return domain.SignerEnrollment{}, fmt.Errorf("list signer enrollments: %w", err)
	}
	if len(found) == 0 {
		return domain.SignerEnrollment{}, fmt.Errorf("%w: no signer enrollment found for %s", domain.ErrInvalidArgument, caller.Subject)
	}

	// TODO: just getting the first one?
	enrollment := found[0]
	if !enrollment.ExpiresAt.IsZero() && time.Now().After(enrollment.ExpiresAt) {
		return domain.SignerEnrollment{}, fmt.Errorf("%w: signer enrollment %s has expired", domain.ErrInvalidArgument, enrollment.ID)
	}

	return enrollment, nil
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

// resolveSigningKeys returns the public keys for a signer enrollment.
// For OIDC enrollments (no external registry) the key is extracted
// from the enrollment's identity token via the configured CEL
// expression. For external registries (GitHub) it delegates to
// KeyResolver.
func (b *KeyResolverProvenanceBuilder) resolveSigningKeys(ctx context.Context, enrollment domain.SignerEnrollment) ([]crypto.PublicKey, error) {
	if enrollment.RegistryID == "oidc" {
		oidcConfig, err := b.loadOIDCConfig(ctx)
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
		return keyregistry.ParsePublicKeyFromBase64(base64Key)
	}
	return b.KeyResolver.Resolve(ctx, enrollment.RegistryID, enrollment.RegistrySubject)
}

func (b *KeyResolverProvenanceBuilder) loadOIDCConfig(ctx context.Context) (domain.OIDCConfig, error) {
	methods, err := b.AuthMethods.List(ctx)
	if err != nil {
		return domain.OIDCConfig{}, fmt.Errorf("list auth methods: %w", err)
	}
	for _, m := range methods {
		if m.Type == domain.AuthMethodTypeOIDC && m.OIDC != nil {
			return *m.OIDC, nil
		}
	}
	return domain.OIDCConfig{}, fmt.Errorf("no OIDC auth method configured")
}
