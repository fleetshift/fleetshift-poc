package domain

import (
	"context"
	"errors"
	"fmt"
)

// DeleteManagedResourceInput identifies the managed resource to delete.
type DeleteManagedResourceInput struct {
	ResourceType ResourceType
	Name         ResourceName
}

// DeleteManagedResourceWorkflowSpec transitions the derived fulfillment
// to [FulfillmentStateDeleting], deletes the managed resource instance
// record, starts a background [DeleteManagedResourceCleanupWorkflow],
// and starts orchestration to process the deletion on the delivery
// side.
type DeleteManagedResourceWorkflowSpec struct {
	Store         Store
	Orchestration OrchestrationWorkflow
	Cleanup       DeleteManagedResourceCleanupWorkflow
}

func (s *DeleteManagedResourceWorkflowSpec) Name() string { return "delete-managed-resource" }

// MutateToDeleting transitions the fulfillment to deleting, bumps its
// generation, and removes the managed resource instance HEAD record.
func (s *DeleteManagedResourceWorkflowSpec) MutateToDeleting() Activity[DeleteManagedResourceInput, ManagedResourceView] {
	return NewActivity("mr-mutate-to-deleting", func(ctx context.Context, in DeleteManagedResourceInput) (ManagedResourceView, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return ManagedResourceView{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		mr, err := tx.ManagedResources().GetInstance(ctx, in.ResourceType, in.Name)
		if err != nil {
			return ManagedResourceView{}, err
		}

		intent, err := tx.ManagedResources().GetIntent(ctx, in.ResourceType, in.Name, mr.CurrentVersion)
		if err != nil {
			return ManagedResourceView{}, fmt.Errorf("get intent: %w", err)
		}

		f, err := tx.Fulfillments().Get(ctx, mr.FulfillmentID)
		if err != nil {
			return ManagedResourceView{}, err
		}

		f.State = FulfillmentStateDeleting
		f.BumpGeneration()
		if err := tx.Fulfillments().Update(ctx, f); err != nil {
			return ManagedResourceView{}, fmt.Errorf("update fulfillment: %w", err)
		}

		if err := tx.ManagedResources().DeleteInstance(ctx, in.ResourceType, in.Name); err != nil {
			return ManagedResourceView{}, fmt.Errorf("delete instance: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return ManagedResourceView{}, fmt.Errorf("commit: %w", err)
		}

		return ManagedResourceView{
			ManagedResource: *mr,
			Intent:          intent,
			Fulfillment:     *f,
		}, nil
	})
}

// StartOrchestration starts orchestration to process the deletion on
// the delivery side.
func (s *DeleteManagedResourceWorkflowSpec) StartOrchestration() Activity[FulfillmentID, struct{}] {
	return NewActivity("mr-start-delete-orchestration", func(ctx context.Context, id FulfillmentID) (struct{}, error) {
		_, err := s.Orchestration.Start(ctx, id)
		if err == ErrAlreadyRunning {
			return struct{}{}, nil
		}
		return struct{}{}, err
	})
}

// StartCleanup starts the background
// [DeleteManagedResourceCleanupWorkflow]. Managed resources have no
// peer deployment row, so the cleanup workflow only deletes the
// fulfillment row after orchestration signals completion.
func (s *DeleteManagedResourceWorkflowSpec) StartCleanup() Activity[DeleteManagedResourceCleanupInput, struct{}] {
	return NewActivity("mr-start-delete-cleanup", func(ctx context.Context, input DeleteManagedResourceCleanupInput) (struct{}, error) {
		_, err := s.Cleanup.Start(ctx, input)
		if errors.Is(err, ErrAlreadyRunning) {
			return struct{}{}, nil
		}
		return struct{}{}, err
	})
}

// Run is the workflow body: mutate to deleting, start the background
// cleanup workflow, then start orchestration to process the removal.
func (s *DeleteManagedResourceWorkflowSpec) Run(record Record, input DeleteManagedResourceInput) (ManagedResourceView, error) {
	view, err := RunActivity(record, s.MutateToDeleting(), input)
	if err != nil {
		return ManagedResourceView{}, fmt.Errorf("mutate to deleting: %w", err)
	}

	if _, err := RunActivity(record, s.StartCleanup(), DeleteManagedResourceCleanupInput{
		FulfillmentID: view.Fulfillment.ID,
	}); err != nil {
		return ManagedResourceView{}, fmt.Errorf("start cleanup: %w", err)
	}

	if _, err := RunActivity(record, s.StartOrchestration(), view.Fulfillment.ID); err != nil {
		return ManagedResourceView{}, fmt.Errorf("start orchestration: %w", err)
	}

	return view, nil
}
