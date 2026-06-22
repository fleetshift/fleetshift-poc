package domain

import (
	"context"
	"errors"
	"fmt"
)

// DeleteDeploymentCleanupWorkflowSpec is a long-running background
// workflow that waits for orchestration to signal that delivery data
// has been cleaned up, then atomically hard-deletes the deployment row
// and the fulfillment row in FK-safe order.
//
// Pass this spec to [Registry.RegisterDeleteDeploymentCleanup] to
// obtain a [DeleteDeploymentCleanupWorkflow] that can start instances.
type DeleteDeploymentCleanupWorkflowSpec struct {
	Store    Store
	Observer DeleteObserver
}

func (s *DeleteDeploymentCleanupWorkflowSpec) deleteObserver() DeleteObserver {
	if s.Observer != nil {
		return s.Observer
	}
	return NoOpDeleteObserver{}
}

func (s *DeleteDeploymentCleanupWorkflowSpec) Name() string { return "delete-deployment-cleanup" }

// DeleteDeploymentAndFulfillment atomically removes the deployment row
// and the fulfillment row in a single transaction. Both deletes
// tolerate [ErrNotFound] so the activity is idempotent on replay.
func (s *DeleteDeploymentCleanupWorkflowSpec) DeleteDeploymentAndFulfillment() Activity[DeleteDeploymentCleanupInput, struct{}] {
	return NewActivity("delete-deployment-and-fulfillment", func(ctx context.Context, input DeleteDeploymentCleanupInput) (struct{}, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		if err := tx.Deployments().Delete(ctx, input.Name); err != nil && !errors.Is(err, ErrNotFound) {
			return struct{}{}, fmt.Errorf("delete deployment row: %w", err)
		}
		if err := tx.Fulfillments().Delete(ctx, input.FulfillmentID); err != nil && !errors.Is(err, ErrNotFound) {
			return struct{}{}, fmt.Errorf("delete fulfillment row: %w", err)
		}

		return struct{}{}, tx.Commit()
	})
}

// Run is the workflow body: wait for the cleanup-complete signal from
// orchestration, then atomically delete the deployment and fulfillment
// rows.
func (s *DeleteDeploymentCleanupWorkflowSpec) Run(record Record, input DeleteDeploymentCleanupInput) (struct{}, error) {
	_, probe := s.deleteObserver().DeploymentCleanupStarted(record.Context(), input)
	defer probe.End()

	if _, err := AwaitSignal(record, DeleteCleanupCompleteSignal); err != nil {
		probe.Error(err)
		return struct{}{}, fmt.Errorf("await delete-cleanup-complete: %w", err)
	}
	probe.SignalReceived()

	if _, err := RunActivity(record, s.DeleteDeploymentAndFulfillment(), input); err != nil {
		probe.Error(err)
		return struct{}{}, fmt.Errorf("delete deployment and fulfillment: %w", err)
	}
	probe.RowsDeleted()

	return struct{}{}, nil
}
