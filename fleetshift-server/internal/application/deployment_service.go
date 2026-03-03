package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// DeploymentService manages deployment lifecycle and triggers orchestration.
type DeploymentService struct {
	Deployments domain.DeploymentRepository
	Deliveries  domain.DeliveryRepository
	CreateWF    domain.CreateDeploymentRunner
}

// Create starts the durable create-deployment workflow, which persists
// the deployment and launches orchestration as a child workflow.
func (s *DeploymentService) Create(ctx context.Context, in domain.CreateDeploymentInput) (domain.Deployment, error) {
	if in.ID == "" {
		return domain.Deployment{}, fmt.Errorf("%w: deployment ID is required", domain.ErrInvalidArgument)
	}

	handle, err := s.CreateWF.Run(ctx, in)
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("start create-deployment workflow: %w", err)
	}

	dep, err := handle.AwaitResult(ctx)
	if err != nil {
		return domain.Deployment{}, fmt.Errorf("create-deployment workflow: %w", err)
	}

	return dep, nil
}

// Get retrieves a deployment by ID.
func (s *DeploymentService) Get(ctx context.Context, id domain.DeploymentID) (domain.Deployment, error) {
	return s.Deployments.Get(ctx, id)
}

// List returns all deployments.
func (s *DeploymentService) List(ctx context.Context) ([]domain.Deployment, error) {
	return s.Deployments.List(ctx)
}

// Delete removes a deployment and its delivery records.
func (s *DeploymentService) Delete(ctx context.Context, id domain.DeploymentID) error {
	if err := s.Deliveries.DeleteByDeployment(ctx, id); err != nil {
		return fmt.Errorf("delete deliveries: %w", err)
	}
	return s.Deployments.Delete(ctx, id)
}
