package domain

import (
	"context"
	"errors"
	"fmt"
)

// DeleteDeploymentWorkflowSpec transitions a fulfillment to
// [FulfillmentStateDeleting], bumps its generation, and runs a
// convergence loop to guarantee orchestration picks up the new state.
// After orchestration completes (deleting the fulfillment), the
// deployment row is cleaned up.
//
// Pass this spec to [Registry.RegisterDeleteDeployment] to obtain a
// [DeleteDeploymentWorkflow] that can start instances.
type DeleteDeploymentWorkflowSpec struct {
	Store         Store
	Orchestration OrchestrationWorkflow
}

func (s *DeleteDeploymentWorkflowSpec) Name() string { return "delete-deployment" }

// MutateToDeleting transitions the fulfillment to [FulfillmentStateDeleting]
// and bumps its generation inside a serialized write transaction.
//
// TODO: move delete transition rules onto Fulfillment so other mutations
// cannot accidentally clear Deleting and effectively "undelete" later.
func (s *DeleteDeploymentWorkflowSpec) MutateToDeleting() Activity[DeploymentID, MutationResult] {
	return NewActivity("mutate-to-deleting", func(ctx context.Context, id DeploymentID) (MutationResult, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return MutationResult{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		dep, err := tx.Deployments().Get(ctx, id)
		if err != nil {
			return MutationResult{}, err
		}

		f, err := tx.Fulfillments().Get(ctx, dep.FulfillmentID)
		if err != nil {
			return MutationResult{}, err
		}

		f.State = FulfillmentStateDeleting
		f.BumpGeneration()
		if err := tx.Fulfillments().Update(ctx, f); err != nil {
			return MutationResult{}, fmt.Errorf("update fulfillment: %w", err)
		}
		if err := tx.Commit(); err != nil {
			return MutationResult{}, fmt.Errorf("commit: %w", err)
		}
		return MutationResult{
			View:          DeploymentView{Deployment: dep, Fulfillment: f},
			FulfillmentID: dep.FulfillmentID,
			MyGen:         f.Generation,
		}, nil
	})
}

// LoadFulfillment reads the current fulfillment state for convergence checks.
func (s *DeleteDeploymentWorkflowSpec) LoadFulfillment() Activity[FulfillmentID, *Fulfillment] {
	return NewActivity("load-fulfillment-for-delete", func(ctx context.Context, id FulfillmentID) (*Fulfillment, error) {
		tx, err := s.Store.BeginReadOnly(ctx)
		if err != nil {
			return nil, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		f, err := tx.Fulfillments().Get(ctx, id)
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		return &f, tx.Commit()
	})
}

// DeleteDeploymentRow removes the thin deployment row after
// orchestration has cleaned up the fulfillment.
func (s *DeleteDeploymentWorkflowSpec) DeleteDeploymentRow() Activity[DeploymentID, struct{}] {
	return NewActivity("delete-deployment-row", func(ctx context.Context, id DeploymentID) (struct{}, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()
		if err := tx.Deployments().Delete(ctx, id); err != nil && !errors.Is(err, ErrNotFound) {
			return struct{}{}, fmt.Errorf("delete deployment row: %w", err)
		}
		return struct{}{}, tx.Commit()
	})
}

// Run is the workflow body: mutate, run the convergence-start loop,
// then clean up the deployment row. [ErrNotFound] during convergence
// is terminal success (the delete completed while waiting).
func (s *DeleteDeploymentWorkflowSpec) Run(record Record, deploymentID DeploymentID) (DeploymentView, error) {
	mr, err := RunActivity(record, s.MutateToDeleting(), deploymentID)
	if err != nil {
		return DeploymentView{}, fmt.Errorf("mutate to deleting: %w", err)
	}

	if err := convergenceLoop(record, s.Orchestration, s.LoadFulfillment(), mr.FulfillmentID, mr.MyGen, true); err != nil {
		return DeploymentView{}, err
	}

	if _, err := RunActivity(record, s.DeleteDeploymentRow(), deploymentID); err != nil {
		return DeploymentView{}, fmt.Errorf("delete deployment row: %w", err)
	}

	return mr.View, nil
}
