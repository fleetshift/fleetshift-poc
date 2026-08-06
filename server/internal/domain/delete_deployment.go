package domain

import (
	"context"
	"errors"
	"fmt"
)

// DeleteDeploymentInput identifies the deployment to delete and
// carries fresh caller auth so retried deletes use current
// credentials rather than stale create-time auth.
type DeleteDeploymentInput struct {
	Name ResourceName
	Auth DeliveryAuth
}

// DeleteDeploymentWorkflowSpec transitions a fulfillment to
// [FulfillmentStateDeleting], bumps its generation, starts a
// background [DeleteDeploymentCleanupWorkflow], and runs a
// convergence loop to guarantee orchestration picks up the new state.
// The actual hard-delete of the deployment and fulfillment rows is
// handled by the cleanup workflow after orchestration signals
// completion.
//
// Pass this spec to [Registry.RegisterDeleteDeployment] to obtain a
// [DeleteDeploymentWorkflow] that can start instances.
type DeleteDeploymentWorkflowSpec struct {
	Store         Store
	Orchestration OrchestrationWorkflow
	Cleanup       DeleteDeploymentCleanupWorkflow
	Observer      DeleteObserver
}

func (s *DeleteDeploymentWorkflowSpec) deleteObserver() DeleteObserver {
	if s.Observer != nil {
		return s.Observer
	}
	return NoOpDeleteObserver{}
}

func (s *DeleteDeploymentWorkflowSpec) Name() string { return "delete-deployment" }

// MutateToDeleting transitions the fulfillment to [FulfillmentStateDeleting]
// and bumps its generation inside a serialized write transaction.
//
// TODO: move delete transition rules onto Fulfillment so other mutations
// cannot accidentally clear Deleting and effectively "undelete" later.
func (s *DeleteDeploymentWorkflowSpec) MutateToDeleting() Activity[DeleteDeploymentInput, deploymentMutationResult] {
	return NewActivity("mutate-to-deleting", func(ctx context.Context, in DeleteDeploymentInput) (deploymentMutationResult, error) {
		ctx, probe := s.deleteObserver().MutateDeploymentStarted(ctx, in.Name)
		defer probe.End()

		tx, err := s.Store.Begin(ctx)
		if err != nil {
			probe.Error(err)
			return deploymentMutationResult{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		dep, err := tx.Deployments().Get(ctx, in.Name)
		if err != nil {
			probe.Error(err)
			return deploymentMutationResult{}, err
		}

		f, err := tx.Fulfillments().Get(ctx, dep.FulfillmentID())
		if err != nil {
			probe.Error(err)
			return deploymentMutationResult{}, err
		}

		f.TransitionToDeleting(in.Auth)
		if err := tx.Fulfillments().Update(ctx, f); err != nil {
			probe.Error(err)
			return deploymentMutationResult{}, fmt.Errorf("update fulfillment: %w", err)
		}
		if err := tx.Commit(); err != nil {
			probe.Error(err)
			return deploymentMutationResult{}, fmt.Errorf("commit: %w", err)
		}
		return deploymentMutationResult{
			View:          DeploymentView{Deployment: dep, Fulfillment: *f},
			FulfillmentID: dep.FulfillmentID(),
			MyGen:         f.Generation(),
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
		return f, tx.Commit()
	})
}

// StartCleanup starts the background
// [DeleteDeploymentCleanupWorkflow] via the [Cleanup] handle. The
// cleanup workflow awaits the
// [DeleteCleanupCompleteSignal] from orchestration before
// hard-deleting both rows. Idempotent: treats [ErrAlreadyRunning] as
// success so activity replays are safe.
func (s *DeleteDeploymentWorkflowSpec) StartCleanup() Activity[DeleteDeploymentCleanupInput, struct{}] {
	return NewActivity("start-delete-cleanup", func(ctx context.Context, input DeleteDeploymentCleanupInput) (struct{}, error) {
		_, err := s.Cleanup.Start(ctx, input)
		if err != nil && errors.Is(err, ErrAlreadyRunning) {
			return struct{}{}, nil
		}
		return struct{}{}, err
	})
}

// Run is the workflow body: mutate to deleting, start the background
// cleanup workflow, then run the convergence loop to ensure
// orchestration picks up the new state. Returns the DELETING snapshot
// immediately; the actual row deletion happens asynchronously in the
// cleanup workflow.
func (s *DeleteDeploymentWorkflowSpec) Run(record Record, input DeleteDeploymentInput) (DeploymentView, error) {
	_, probe := s.deleteObserver().DeleteDeploymentStarted(record.Context(), input.Name)
	defer probe.End()

	mr, err := RunActivity(record, s.MutateToDeleting(), input)
	if err != nil {
		probe.Error(err)
		return DeploymentView{}, fmt.Errorf("mutate to deleting: %w", err)
	}
	probe.Mutated(mr.FulfillmentID, mr.MyGen)

	if _, err := RunActivity(record, s.StartCleanup(), DeleteDeploymentCleanupInput{
		Name:          input.Name,
		FulfillmentID: mr.FulfillmentID,
	}); err != nil {
		probe.Error(err)
		return DeploymentView{}, fmt.Errorf("start cleanup: %w", err)
	}
	probe.CleanupStarted()

	if err := convergenceLoop(record, s.Orchestration, s.LoadFulfillment(), mr.FulfillmentID, mr.MyGen, true); err != nil {
		probe.Error(err)
		return DeploymentView{}, err
	}

	return mr.View, nil
}
