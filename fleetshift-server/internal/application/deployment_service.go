package application

import (
	"context"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
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
func (s *DeploymentService) Create(ctx context.Context, in domain.CreateDeploymentInput) (domain.DeploymentView, error) {
	if in.ID == "" {
		return domain.DeploymentView{}, fmt.Errorf("%w: deployment ID is required", domain.ErrInvalidArgument)
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
			return domain.DeploymentView{}, fmt.Errorf(
				"%w: signing a deployment requires an authenticated caller",
				domain.ErrInvalidArgument)
		}
		if s.ProvenanceBuilder == nil {
			return domain.DeploymentView{}, fmt.Errorf(
				"%w: signing not configured", domain.ErrInvalidArgument)
		}
		tx, err := s.Store.BeginReadOnly(ctx)
		if err != nil {
			return domain.DeploymentView{}, fmt.Errorf("begin read tx: %w", err)
		}
		defer tx.Rollback()
		prov, err := s.ProvenanceBuilder.BuildProvenance(
			ctx, tx.SignerEnrollments(), ac.Subject,
			in.ID, in.ManifestStrategy, in.PlacementStrategy,
			in.ExpectedGeneration, in.UserSignature, in.ValidUntil,
		)
		if err != nil {
			return domain.DeploymentView{}, fmt.Errorf("build provenance: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return domain.DeploymentView{}, fmt.Errorf("commit read tx: %w", err)
		}
		// TODO: I don't like this modification of the input after the fact
		in.Provenance = prov
	}

	// TODO: don't store token; keep it in memory. use peer cluster to retrieve from peers on concurrent updates.
	exec, err := s.CreateWF.Start(ctx, in)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("start create-deployment workflow: %w", err)
	}

	view, err := exec.AwaitResult(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("create-deployment workflow: %w", err)
	}

	return view, nil
}

// Get retrieves a deployment by ID.
func (s *DeploymentService) Get(ctx context.Context, id domain.DeploymentID) (domain.DeploymentView, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	view, err := tx.Deployments().GetView(ctx, id)
	if err != nil {
		return domain.DeploymentView{}, err
	}
	return view, tx.Commit()
}

// List returns all deployments (each joined with its fulfillment).
func (s *DeploymentService) List(ctx context.Context) ([]domain.DeploymentView, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	views, err := tx.Deployments().ListView(ctx)
	if err != nil {
		return nil, err
	}
	return views, tx.Commit()
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
func (s *DeploymentService) Resume(ctx context.Context, in ResumeInput) (domain.DeploymentView, error) {
	ac := AuthFromContext(ctx)
	if ac == nil || ac.Subject == nil {
		return domain.DeploymentView{}, fmt.Errorf("%w: resuming a deployment requires an authenticated caller",
			domain.ErrInvalidArgument)
	}

	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.Rollback()

	dep, err := tx.Deployments().Get(ctx, in.ID)
	if err != nil {
		return domain.DeploymentView{}, err
	}
	fulfillment, err := tx.Fulfillments().Get(ctx, dep.FulfillmentID)
	if err != nil {
		return domain.DeploymentView{}, err
	}
	currentGen := fulfillment.Generation
	if err := tx.Commit(); err != nil {
		return domain.DeploymentView{}, fmt.Errorf("commit read tx: %w", err)
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
		return domain.DeploymentView{}, fmt.Errorf("start resume-deployment workflow: %w", err)
	}

	result, err := exec.AwaitResult(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("resume-deployment workflow: %w", err)
	}

	return result, nil
}

// Delete starts a durable delete-deployment workflow that transitions
// the fulfillment to [domain.FulfillmentStateDeleting], bumps its
// generation, and guarantees orchestration converges the delete. If
// the fulfillment is already deleting, the current view is returned
// without starting a new workflow (idempotent).
func (s *DeploymentService) Delete(ctx context.Context, id domain.DeploymentID) (domain.DeploymentView, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.Rollback()

	dep, err := tx.Deployments().Get(ctx, id)
	if err != nil {
		return domain.DeploymentView{}, err
	}
	fulfillment, err := tx.Fulfillments().Get(ctx, dep.FulfillmentID)
	if err != nil {
		return domain.DeploymentView{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.DeploymentView{}, fmt.Errorf("commit read tx: %w", err)
	}

	if fulfillment.State == domain.FulfillmentStateDeleting {
		return domain.DeploymentView{Deployment: dep, Fulfillment: *fulfillment}, nil
	}

	exec, err := s.DeleteWF.Start(ctx, id, fulfillment.Generation)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("start delete-deployment workflow: %w", err)
	}

	result, err := exec.AwaitResult(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("delete-deployment workflow: %w", err)
	}

	return result, nil
}
