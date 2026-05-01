package domain

import (
	"context"
	"errors"
	"fmt"
)

// DeleteCleanupWorkflowSpec is a long-running background workflow that
// waits for orchestration to signal that delivery data has been cleaned
// up, then atomically hard-deletes the deployment and fulfillment rows
// in FK-safe order.
//
// Pass this spec to [Registry.RegisterDeleteCleanup] to obtain a
// [DeleteCleanupWorkflow] that can start instances.
type DeleteCleanupWorkflowSpec struct {
	Store Store
}

func (s *DeleteCleanupWorkflowSpec) Name() string { return "delete-cleanup" }

// DeleteDeploymentAndFulfillment atomically removes the deployment row
// (clearing the FK reference) and the fulfillment row in a single
// transaction. Both deletes tolerate [ErrNotFound] so the activity is
// idempotent on replay.
func (s *DeleteCleanupWorkflowSpec) DeleteDeploymentAndFulfillment() Activity[DeleteCleanupInput, struct{}] {
	return NewActivity("delete-deployment-and-fulfillment", func(ctx context.Context, input DeleteCleanupInput) (struct{}, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		if err := tx.Deployments().Delete(ctx, input.DeploymentID); err != nil && !errors.Is(err, ErrNotFound) {
			return struct{}{}, fmt.Errorf("delete deployment row: %w", err)
		}
		if err := tx.Fulfillments().Delete(ctx, input.FulfillmentID); err != nil && !errors.Is(err, ErrNotFound) {
			return struct{}{}, fmt.Errorf("delete fulfillment row: %w", err)
		}

		return struct{}{}, tx.Commit()
	})
}

// Run is the workflow body: wait for the cleanup-complete signal from
// orchestration, then atomically delete both rows.
func (s *DeleteCleanupWorkflowSpec) Run(record Record, input DeleteCleanupInput) (struct{}, error) {
	if _, err := AwaitSignal(record, DeleteCleanupCompleteSignal); err != nil {
		return struct{}{}, fmt.Errorf("await delete-cleanup-complete: %w", err)
	}

	if _, err := RunActivity(record, s.DeleteDeploymentAndFulfillment(), input); err != nil {
		return struct{}{}, fmt.Errorf("delete deployment and fulfillment: %w", err)
	}

	return struct{}{}, nil
}
