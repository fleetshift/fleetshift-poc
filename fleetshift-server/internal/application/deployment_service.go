package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DeploymentService manages deployment lifecycle and triggers orchestration.
type DeploymentService struct {
	Store         domain.Store
	CreateWF      domain.CreateDeploymentWorkflow
	Orchestration domain.OrchestrationWorkflow
}

// Create starts the durable create-deployment workflow, which persists
// the deployment and launches orchestration as a child workflow.
func (s *DeploymentService) Create(ctx context.Context, in domain.CreateDeploymentInput) (domain.Deployment, error) {
	if in.ID == "" {
		return domain.Deployment{}, fmt.Errorf("%w: deployment ID is required", domain.ErrInvalidArgument)
	}

	if ac := AuthFromContext(ctx); ac != nil && ac.Subject != nil {
		in.Auth = domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		}
	}

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

// Resume resumes a deployment that is paused for authentication. It
// updates the deployment's auth with the caller's fresh token, bumps
// the generation, and triggers a new reconciliation.
func (s *DeploymentService) Resume(ctx context.Context, id domain.DeploymentID) (domain.Deployment, error) {
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

	dep, err := tx.Deployments().Get(ctx, id)
	if err != nil {
		return domain.Deployment{}, err
	}

	if dep.State != domain.DeploymentStatePausedAuth {
		return domain.Deployment{}, fmt.Errorf("%w: deployment %q is in state %q, not paused_auth",
			domain.ErrInvalidArgument, id, dep.State)
	}

	dep.Auth = domain.DeliveryAuth{
		Caller:   ac.Subject,
		Audience: ac.Audience,
		Token:    ac.Token,
	}
	dep.BumpGeneration()
	if err := tx.Deployments().Update(ctx, dep); err != nil {
		return domain.Deployment{}, fmt.Errorf("update deployment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.Deployment{}, fmt.Errorf("commit: %w", err)
	}

	if err := s.Reconcile(ctx, id); err != nil {
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

// Reconcile attempts to start a reconciliation workflow for the given
// deployment. It uses a CAS gate so that at most one workflow runs per
// deployment at a time. If a reconciliation is already in progress,
// the method returns nil — the running workflow will observe the new
// generation when it finishes.
func (s *DeploymentService) Reconcile(ctx context.Context, id domain.DeploymentID) error {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	dep, err := tx.Deployments().Get(ctx, id)
	if err != nil {
		return err
	}
	if !dep.TryAcquireReconciliation() {
		return nil
	}
	if err := tx.Deployments().Update(ctx, dep); err != nil {
		return fmt.Errorf("update deployment: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	_, err = s.Orchestration.Start(ctx, id)
	return err
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
