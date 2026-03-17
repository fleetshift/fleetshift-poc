package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DeploymentService manages deployment lifecycle and triggers orchestration.
type DeploymentService struct {
	Store    domain.Store
	CreateWF domain.CreateDeploymentWorkflow
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
	tx, err := s.Store.Begin(ctx)
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
	tx, err := s.Store.Begin(ctx)
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

// Delete removes a deployment and its delivery records atomically.
func (s *DeploymentService) Delete(ctx context.Context, id domain.DeploymentID) error {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := tx.Deliveries().DeleteByDeployment(ctx, id); err != nil {
		return fmt.Errorf("delete deliveries: %w", err)
	}
	if err := tx.Deployments().Delete(ctx, id); err != nil {
		return err
	}
	return tx.Commit()
}
