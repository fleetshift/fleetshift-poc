package domain

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreateDeploymentInput is the specification for creating a new deployment.
type CreateDeploymentInput struct {
	ID                DeploymentID
	ManifestStrategy  ManifestStrategySpec
	PlacementStrategy PlacementStrategySpec
	RolloutStrategy   *RolloutStrategySpec
}

// CreateDeploymentRunner starts create-deployment workflows (app-facing API).
type CreateDeploymentRunner interface {
	Run(ctx context.Context, input CreateDeploymentInput) (WorkflowHandle[Deployment], error)
}

// CreateDeploymentWorkflow is a short-lived parent workflow that
// persists a new deployment and starts the orchestration child
// workflow. Both steps are durable: on crash the engine replays
// from the last completed step.
type CreateDeploymentWorkflow struct {
	Store Store
	Now   func() time.Time

	// StartOrchestration durably starts the orchestration child
	// workflow for the given deployment. Set by the engine during
	// [WorkflowEngine.Register]. Runs as an activity through the
	// journal so the start survives replay.
	StartOrchestration Activity[DeploymentID, struct{}]
}

func (w *CreateDeploymentWorkflow) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

func (w *CreateDeploymentWorkflow) Name() string { return "create-deployment" }

// PersistDeployment creates a pending deployment record.
func (w *CreateDeploymentWorkflow) PersistDeployment() Activity[CreateDeploymentInput, Deployment] {
	return NewActivity("persist-deployment", func(ctx context.Context, in CreateDeploymentInput) (Deployment, error) {
		tx, err := w.Store.Begin(ctx)
		if err != nil {
			return Deployment{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		now := w.now()
		uid := uuid.New().String()
		dep := Deployment{
			ID:                in.ID,
			UID:               uid,
			ManifestStrategy:  in.ManifestStrategy,
			PlacementStrategy: in.PlacementStrategy,
			RolloutStrategy:   in.RolloutStrategy,
			State:             DeploymentStateCreating,
			CreatedAt:         now,
			UpdatedAt:         now,
			Etag:              uid,
		}
		if err := tx.Deployments().Create(ctx, dep); err != nil {
			return Deployment{}, err
		}
		if err := tx.Commit(); err != nil {
			return Deployment{}, fmt.Errorf("commit: %w", err)
		}
		return dep, nil
	})
}

// Run is the workflow body: persist the deployment, then start
// orchestration as a durable child workflow.
func (w *CreateDeploymentWorkflow) Run(journal Journal, input CreateDeploymentInput) (Deployment, error) {
	dep, err := RunActivity(journal, w.PersistDeployment(), input)
	if err != nil {
		return Deployment{}, fmt.Errorf("persist deployment: %w", err)
	}

	if _, err := RunActivity(journal, w.StartOrchestration, dep.ID); err != nil {
		return Deployment{}, fmt.Errorf("start orchestration: %w", err)
	}

	return dep, nil
}
