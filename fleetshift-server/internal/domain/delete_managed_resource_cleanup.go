package domain

import (
	"context"
	"errors"
	"fmt"
)

// DeleteManagedResourceCleanupWorkflowSpec is a long-running
// background workflow that waits for orchestration to signal that
// delivery data has been cleaned up, then atomically hard-deletes the
// managed resource row and fulfillment row.
//
// Pass this spec to [Registry.RegisterDeleteManagedResourceCleanup] to
// obtain a [DeleteManagedResourceCleanupWorkflow] that can start
// instances.
type DeleteManagedResourceCleanupWorkflowSpec struct {
	Store Store
}

func (s *DeleteManagedResourceCleanupWorkflowSpec) Name() string {
	return "delete-managed-resource-cleanup"
}

// DeleteManagedResourceAndFulfillment removes the managed resource row,
// all versioned resource intents for that resource, and the fulfillment
// row in a single transaction after orchestration has finished
// delivery-side cleanup. The managed-resource and fulfillment deletes
// tolerate [ErrNotFound] so the activity is idempotent on replay.
func (s *DeleteManagedResourceCleanupWorkflowSpec) DeleteManagedResourceAndFulfillment() Activity[DeleteManagedResourceCleanupInput, struct{}] {
	return NewActivity("delete-managed-resource-and-fulfillment", func(ctx context.Context, input DeleteManagedResourceCleanupInput) (struct{}, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		if err := tx.ManagedResources().DeleteInstance(ctx, input.ResourceType, input.Name); err != nil && !errors.Is(err, ErrNotFound) {
			return struct{}{}, fmt.Errorf("delete managed resource row: %w", err)
		}
		if err := tx.ManagedResources().DeleteIntents(ctx, input.ResourceType, input.Name); err != nil {
			return struct{}{}, fmt.Errorf("delete managed resource intents: %w", err)
		}
		if err := tx.Fulfillments().Delete(ctx, input.FulfillmentID); err != nil && !errors.Is(err, ErrNotFound) {
			return struct{}{}, fmt.Errorf("delete fulfillment row: %w", err)
		}

		return struct{}{}, tx.Commit()
	})
}

// Run is the workflow body: wait for the cleanup-complete signal from
// orchestration, then delete the managed resource and fulfillment
// rows.
func (s *DeleteManagedResourceCleanupWorkflowSpec) Run(record Record, input DeleteManagedResourceCleanupInput) (struct{}, error) {
	if _, err := AwaitSignal(record, DeleteCleanupCompleteSignal); err != nil {
		return struct{}{}, fmt.Errorf("await delete-cleanup-complete: %w", err)
	}

	if _, err := RunActivity(record, s.DeleteManagedResourceAndFulfillment(), input); err != nil {
		return struct{}{}, fmt.Errorf("delete managed resource and fulfillment: %w", err)
	}

	return struct{}{}, nil
}
