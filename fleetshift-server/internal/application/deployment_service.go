package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// CreateDeploymentInput is the caller-provided input for creating a deployment.
type CreateDeploymentInput struct {
	ID                domain.DeploymentID
	ManifestStrategy  domain.ManifestStrategySpec
	PlacementStrategy domain.PlacementStrategySpec
	RolloutStrategy   *domain.RolloutStrategySpec
}

// DeploymentService manages deployment lifecycle and triggers orchestration.
type DeploymentService struct {
	Deployments   domain.DeploymentRepository
	Records       domain.DeliveryRecordRepository
	Orchestration *OrchestrationService
}

// Create persists a new deployment and runs the orchestration pipeline.
func (s *DeploymentService) Create(ctx context.Context, in CreateDeploymentInput) (domain.Deployment, error) {
	if in.ID == "" {
		return domain.Deployment{}, fmt.Errorf("%w: deployment ID is required", domain.ErrInvalidArgument)
	}

	dep := domain.Deployment{
		ID:                in.ID,
		ManifestStrategy:  in.ManifestStrategy,
		PlacementStrategy: in.PlacementStrategy,
		RolloutStrategy:   in.RolloutStrategy,
		State:             domain.DeploymentStatePending,
	}

	if err := s.Deployments.Create(ctx, dep); err != nil {
		return domain.Deployment{}, err
	}

	if err := s.Orchestration.Orchestrate(ctx, dep.ID); err != nil {
		return domain.Deployment{}, fmt.Errorf("orchestrate: %w", err)
	}

	return s.Deployments.Get(ctx, dep.ID)
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
	if err := s.Records.DeleteByDeployment(ctx, id); err != nil {
		return fmt.Errorf("delete delivery records: %w", err)
	}
	return s.Deployments.Delete(ctx, id)
}
