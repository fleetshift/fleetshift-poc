package application

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	fscrypto "github.com/fleetshift/fleetshift-poc/fleetshift-server/pkg/crypto"
)

// DeploymentService manages deployment lifecycle and triggers orchestration.
type DeploymentService struct {
	Store         domain.Store
	CreateWF      domain.CreateDeploymentWorkflow
	Orchestration domain.OrchestrationWorkflow
	KeyResolver   *KeyResolver
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
		tx, err := s.Store.BeginReadOnly(ctx)
		if err != nil {
			return domain.Deployment{}, fmt.Errorf("begin read tx: %w", err)
		}
		defer tx.Rollback()
		prov, err := s.buildProvenance(
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

// Resume resumes a deployment that is paused for authentication. It
// updates the deployment's auth with the caller's fresh token, bumps
// the generation, and triggers a new reconciliation.
//
// If the deployment has provenance, the caller must provide a fresh
// signature (re-signing). Token-passthrough deployments resume as before.
func (s *DeploymentService) Resume(ctx context.Context, in ResumeInput) (domain.Deployment, error) {
	ac := AuthFromContext(ctx)
	if ac == nil || ac.Subject == nil {
		return domain.Deployment{}, fmt.Errorf("%w: resuming a deployment requires an authenticated caller",
			domain.ErrInvalidArgument)
	}

	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	dep, err := tx.Deployments().Get(ctx, in.ID)
	if err != nil {
		return domain.Deployment{}, err
	}

	if dep.State != domain.DeploymentStatePausedAuth {
		return domain.Deployment{}, fmt.Errorf("%w: deployment %q is in state %q, not paused_auth",
			domain.ErrInvalidArgument, in.ID, dep.State)
	}

	hadProvenance := dep.Provenance != nil

	dep.Auth = domain.DeliveryAuth{
		Caller:   ac.Subject,
		Audience: ac.Audience,
		Token:    ac.Token,
	}

	if hadProvenance || len(in.UserSignature) > 0 {
		if hadProvenance && len(in.UserSignature) == 0 {
			return domain.Deployment{}, fmt.Errorf(
				"%w: deployment %q has provenance; re-signing is required to resume",
				domain.ErrInvalidArgument, in.ID)
		}
		nextGen := dep.Generation + 1
		prov, err := s.buildProvenance(
			ctx, tx.SignerEnrollments(), ac.Subject,
			dep.ID, dep.ManifestStrategy, dep.PlacementStrategy,
			nextGen, in.UserSignature, in.ValidUntil,
		)
		if err != nil {
			return domain.Deployment{}, fmt.Errorf("build provenance: %w", err)
		}
		dep.Provenance = prov
	}

	dep.BumpGeneration()
	if err := tx.Deployments().Update(ctx, dep); err != nil {
		return domain.Deployment{}, fmt.Errorf("update deployment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.Deployment{}, fmt.Errorf("commit: %w", err)
	}

	if err := s.Reconcile(ctx, in.ID); err != nil {
		return domain.Deployment{}, fmt.Errorf("reconcile: %w", err)
	}

	return dep, nil
}

// Delete transitions a deployment to the deleting state, bumps its
// generation, and triggers a reconciliation that will execute the
// delete pipeline.
func (s *DeploymentService) Delete(ctx context.Context, id domain.DeploymentID) error {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	dep, err := tx.Deployments().Get(ctx, id)
	if err != nil {
		return err
	}
	dep.State = domain.DeploymentStateDeleting
	dep.BumpGeneration()
	if err := tx.Deployments().Update(ctx, dep); err != nil {
		return fmt.Errorf("update deployment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return s.Reconcile(ctx, id)
}

// Reconcile starts a reconciliation workflow for the given deployment.
// The workflow engine enforces at-most-one-active-instance per
// deployment ID; if a workflow is already running, [domain.ErrAlreadyRunning]
// is swallowed and the method returns nil — the running workflow will
// observe the new generation when it finishes.
func (s *DeploymentService) Reconcile(ctx context.Context, id domain.DeploymentID) error {
	_, err := s.Orchestration.Start(ctx, id)
	if errors.Is(err, domain.ErrAlreadyRunning) {
		return nil
	}
	return err
}

// buildProvenance constructs [domain.Provenance] by looking up the
// caller's signer enrollment, resolving public keys from the external
// registry via [KeyResolver], and verifying the signature.
func (s *DeploymentService) buildProvenance(
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

	keys, err := s.KeyResolver.Resolve(ctx, enrollment.RegistryID, enrollment.RegistrySubject)
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

// Invalidate bumps the deployment's generation and triggers a
// reconciliation. Use this when an external change (placement,
// manifests, spec) requires re-evaluation.
func (s *DeploymentService) Invalidate(ctx context.Context, id domain.DeploymentID) error {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	dep, err := tx.Deployments().Get(ctx, id)
	if err != nil {
		return fmt.Errorf("get deployment: %w", err)
	}
	dep.BumpGeneration()
	if err := tx.Deployments().Update(ctx, dep); err != nil {
		return fmt.Errorf("update deployment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	return s.Reconcile(ctx, id)
}
