package application

import (
	"context"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DeploymentService manages deployment lifecycle and triggers orchestration.
type DeploymentService struct {
	Store         domain.Store
	CreateWF      domain.CreateDeploymentWorkflow
	DeleteWF      domain.DeleteDeploymentWorkflow
	ResumeWF      domain.ResumeDeploymentWorkflow
	ProvenanceSvc *domain.ProvenanceService
}

// Create starts the durable create-deployment workflow, which persists
// the deployment and launches orchestration as a child workflow.
func (s *DeploymentService) Create(ctx context.Context, in domain.CreateDeploymentInput) (domain.DeploymentView, error) {
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
		tx, err := s.Store.BeginReadOnly(ctx)
		if err != nil {
			return domain.DeploymentView{}, fmt.Errorf("begin read tx: %w", err)
		}
		defer tx.Rollback()
		prov, err := s.ProvenanceSvc.BuildDeploymentProvenance(
			ctx, tx.SignerEnrollments(), ac.Subject,
			in.Name, in.ManifestStrategy, in.PlacementStrategy,
			1, in.UserSignature, in.ValidUntil,
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

// Get retrieves a deployment by resource name.
func (s *DeploymentService) Get(ctx context.Context, name domain.ResourceName) (domain.DeploymentView, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	view, err := tx.Deployments().GetView(ctx, name)
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
	Name               domain.ResourceName
	UserSignature      []byte
	ValidUntil         time.Time
	Etag               domain.Etag
	ExpectedGeneration domain.Generation
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

	dep, err := tx.Deployments().Get(ctx, in.Name)
	if err != nil {
		return domain.DeploymentView{}, err
	}
	fulfillment, err := tx.Fulfillments().Get(ctx, dep.FulfillmentID())
	if err != nil {
		return domain.DeploymentView{}, err
	}
	currentGen := fulfillment.Generation()
	if err := tx.Commit(); err != nil {
		return domain.DeploymentView{}, fmt.Errorf("commit read tx: %w", err)
	}

	exec, err := s.ResumeWF.Start(ctx, domain.ResumeDeploymentInput{
		Name: in.Name,
		Auth: domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		},
		UserSignature:      in.UserSignature,
		ValidUntil:         in.ValidUntil,
		Etag:               in.Etag,
		ExpectedGeneration: in.ExpectedGeneration,
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
// the fulfillment is already deleting and not paused, the current
// view is returned without starting a new workflow (idempotent).
func (s *DeploymentService) Delete(ctx context.Context, name domain.ResourceName) (domain.DeploymentView, error) {
	var auth domain.DeliveryAuth
	ac := AuthFromContext(ctx)
	if ac != nil && ac.Subject != nil {
		auth = domain.DeliveryAuth{
			Caller:   ac.Subject,
			Audience: ac.Audience,
			Token:    ac.Token,
		}
	}

	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("begin read tx: %w", err)
	}
	defer tx.Rollback()

	dep, err := tx.Deployments().Get(ctx, name)
	if err != nil {
		return domain.DeploymentView{}, err
	}
	fulfillment, err := tx.Fulfillments().Get(ctx, dep.FulfillmentID())
	if err != nil {
		return domain.DeploymentView{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.DeploymentView{}, fmt.Errorf("commit read tx: %w", err)
	}

	if fulfillment.State() == domain.FulfillmentStateDeleting && !fulfillment.Paused() {
		return domain.DeploymentView{Deployment: dep, Fulfillment: *fulfillment}, nil
	}

	exec, err := s.DeleteWF.Start(ctx, domain.DeleteDeploymentInput{
		Name: name,
		Auth: auth,
	}, fulfillment.Generation())
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("start delete-deployment workflow: %w", err)
	}

	result, err := exec.AwaitResult(ctx)
	if err != nil {
		return domain.DeploymentView{}, fmt.Errorf("delete-deployment workflow: %w", err)
	}

	return result, nil
}
