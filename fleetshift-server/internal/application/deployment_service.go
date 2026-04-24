package application

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/keyregistry"
	fscrypto "github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/crypto"
)

// DeploymentService manages deployment lifecycle and triggers orchestration.
type DeploymentService struct {
	Store             domain.Store
	CreateWF          domain.CreateDeploymentWorkflow
	DeleteWF          domain.DeleteDeploymentWorkflow
	ResumeWF          domain.ResumeDeploymentWorkflow
	ProvenanceBuilder domain.ProvenanceBuilder // nil when signing is not configured
}

// Create starts the durable create-deployment workflow, which persists
// the deployment and launches orchestration as a child workflow.
func (s *DeploymentService) Create(ctx context.Context, in domain.CreateDeploymentInput) (domain.Deployment, error) {
	if in.ID == "" {
		return domain.Deployment{}, fmt.Errorf("%w: deployment ID is required", domain.ErrInvalidArgument)
	}

	ac := AuthFromContext(ctx)
	if ac != nil && ac.Subject != nil {
		in.Auth = domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		}
	}

	if len(in.UserSignature) > 0 {
		if ac == nil || ac.Subject == nil {
			return domain.Deployment{}, fmt.Errorf(
				"%w: signing a deployment requires an authenticated caller",
				domain.ErrInvalidArgument)
		}
		if s.ProvenanceBuilder == nil {
			return domain.Deployment{}, fmt.Errorf(
				"%w: signing not configured", domain.ErrInvalidArgument)
		}
		tx, err := s.Store.BeginReadOnly(ctx)
		if err != nil {
			return domain.Deployment{}, fmt.Errorf("begin read tx: %w", err)
		}
		defer tx.Rollback()
		prov, err := s.ProvenanceBuilder.BuildProvenance(
			ctx, tx.SignerEnrollments(), ac.Subject,
			in.ID, in.ManifestStrategy, in.PlacementStrategy,
			in.ExpectedGeneration, in.UserSignature, in.ValidUntil,
		)
		if err != nil {
			return domain.Deployment{}, fmt.Errorf("build provenance: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return domain.Deployment{}, fmt.Errorf("commit read tx: %w", err)
		}
		// TODO: I don't like this modification of the input after the fact
		in.Provenance = prov
	}

	// TODO: don't store token; keep it in memory. use peer cluster to retrieve from peers on concurrent updates.
	exec, err := s.CreateWF.Start(ctx, in)
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("start create-deployment workflow: %w", err)
	}

	dep, err := exec.AwaitResult(ctx)
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("create-deployment workflow: %w", err)
	}

	return dep, nil
}

// Get retrieves a deployment by ID.
func (s *DeploymentService) Get(ctx context.Context, id domain.DeploymentID) (domain.Deployment, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	dep, err := tx.Deployments().Get(ctx, id)
	if err != nil {
		return domain.Deployment{}, err
	}
	return dep, tx.Commit()
}

// List returns all deployments.
func (s *DeploymentService) List(ctx context.Context) ([]domain.Deployment, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	deps, err := tx.Deployments().List(ctx)
	if err != nil {
		return nil, err
	}
	return deps, tx.Commit()
}

// ResumeInput carries the optional re-signing parameters for resuming
// a deployment. When UserSignature is non-empty, the server constructs
// fresh provenance for the resuming user.
type ResumeInput struct {
	ID            domain.DeploymentID
	UserSignature []byte
	ValidUntil    time.Time
}

// Resume resumes a deployment that is paused for authentication by
// starting a durable resume-deployment workflow. The workflow updates
// auth/provenance, bumps the generation, and guarantees orchestration
// converges the new state.
func (s *DeploymentService) Resume(ctx context.Context, in ResumeInput) (domain.Deployment, error) {
	ac := AuthFromContext(ctx)
	if ac == nil || ac.Subject == nil {
		return domain.Deployment{}, fmt.Errorf("%w: resuming a deployment requires an authenticated caller",
			domain.ErrInvalidArgument)
	}

	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.Rollback()

	dep, err := tx.Deployments().Get(ctx, in.ID)
	if err != nil {
		return domain.Deployment{}, err
	}
	currentGen := dep.Generation
	if err := tx.Commit(); err != nil {
		return domain.Deployment{}, fmt.Errorf("commit read tx: %w", err)
	}

	exec, err := s.ResumeWF.Start(ctx, domain.ResumeDeploymentInput{
		ID: in.ID,
		Auth: domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		},
		UserSignature: in.UserSignature,
		ValidUntil:    in.ValidUntil,
	}, currentGen)
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("start resume-deployment workflow: %w", err)
	}

	result, err := exec.AwaitResult(ctx)
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("resume-deployment workflow: %w", err)
	}

	return result, nil
}

// Delete starts a durable delete-deployment workflow that transitions
// the deployment to [domain.DeploymentStateDeleting], bumps its
// generation, and guarantees orchestration converges the delete.
func (s *DeploymentService) Delete(ctx context.Context, id domain.DeploymentID) (domain.Deployment, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.Rollback()

	dep, err := tx.Deployments().Get(ctx, id)
	if err != nil {
		return domain.Deployment{}, err
	}
	currentGen := dep.Generation
	if err := tx.Commit(); err != nil {
		return domain.Deployment{}, fmt.Errorf("commit read tx: %w", err)
	}

	exec, err := s.DeleteWF.Start(ctx, id, currentGen)
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("start delete-deployment workflow: %w", err)
	}

	result, err := exec.AwaitResult(ctx)
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("delete-deployment workflow: %w", err)
	}

	return result, nil
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

// KeyResolverProvenanceBuilder adapts [KeyResolver] to the
// [domain.ProvenanceBuilder] interface used by mutation workflows
// that require provenance construction. AuthMethods is required for
// OIDC-based enrollments where the signing key is extracted from the
// identity token rather than an external registry.
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
	if caller == nil {
		return nil, fmt.Errorf("%w: caller identity required for provenance", domain.ErrInvalidArgument)
	}
	found, err := enrollments.ListBySubject(ctx, caller.FederatedIdentity)
	if err != nil {
		return nil, fmt.Errorf("list signer enrollments: %w", err)
	}
	if len(found) == 0 {
		return nil, fmt.Errorf("%w: no signer enrollment found for %s", domain.ErrInvalidArgument, caller.Subject)
	}

	// TODO: just getting the first one?
	enrollment := found[0]

	if !enrollment.ExpiresAt.IsZero() && time.Now().After(enrollment.ExpiresAt) {
		return nil, fmt.Errorf("%w: signer enrollment %s has expired", domain.ErrInvalidArgument, enrollment.ID)
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

// resolveSigningKeys returns the public keys for a signer enrollment.
// For OIDC enrollments (no external registry) the key is extracted
// from the enrollment's identity token via the configured CEL
// expression. For external registries (GitHub) it delegates to KeyResolver.
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
