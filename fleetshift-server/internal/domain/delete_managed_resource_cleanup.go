package domain

import (
	"context"
	"errors"
	"fmt"
)

// DeleteManagedResourceCleanupWorkflowSpec is a long-running
// background workflow that waits for orchestration to signal that
// delivery data has been cleaned up, then hard-deletes the fulfillment
// row for a managed resource whose HEAD record has already been
// removed.
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

// DeleteFulfillment removes the fulfillment row for a managed resource
// after orchestration has finished delivery-side cleanup. It tolerates
// [ErrNotFound] so the activity is idempotent on replay.
func (s *DeleteManagedResourceCleanupWorkflowSpec) DeleteFulfillment() Activity[DeleteManagedResourceCleanupInput, struct{}] {
	return NewActivity("delete-managed-resource-fulfillment", func(ctx context.Context, input DeleteManagedResourceCleanupInput) (struct{}, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return struct{}{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		if err := tx.Fulfillments().Delete(ctx, input.FulfillmentID); err != nil && !errors.Is(err, ErrNotFound) {
			return struct{}{}, fmt.Errorf("delete fulfillment row: %w", err)
		}

		return struct{}{}, tx.Commit()
	})
}

// Run is the workflow body: wait for the cleanup-complete signal from
// orchestration, then delete the fulfillment row.
func (s *DeleteManagedResourceCleanupWorkflowSpec) Run(record Record, input DeleteManagedResourceCleanupInput) (struct{}, error) {
	if _, err := AwaitSignal(record, DeleteCleanupCompleteSignal); err != nil {
		return struct{}{}, fmt.Errorf("await delete-cleanup-complete: %w", err)
	}

	if _, err := RunActivity(record, s.DeleteFulfillment(), input); err != nil {
		return struct{}{}, fmt.Errorf("delete fulfillment: %w", err)
	}

	return struct{}{}, nil
}
