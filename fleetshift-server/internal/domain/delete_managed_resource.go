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
	// Auth is persisted so background remove/retry passes use delete-time
	// auth rather than stale create-time auth.
	Auth DeliveryAuth
}

// DeleteManagedResourceWorkflowSpec transitions the derived fulfillment
// to [FulfillmentStateDeleting], bumps its generation, starts a
// background [DeleteManagedResourceCleanupWorkflow], and runs a
// convergence loop to guarantee orchestration picks up the new state.
// The managed resource row remains visible in DELETING until cleanup
// completes.
//
// Pass this spec to [Registry.RegisterDeleteManagedResource] to obtain
// a [DeleteManagedResourceWorkflow] that can start instances.
type DeleteManagedResourceWorkflowSpec struct {
	Store         Store
	Orchestration OrchestrationWorkflow
	Cleanup       DeleteManagedResourceCleanupWorkflow
}

func (s *DeleteManagedResourceWorkflowSpec) Name() string { return "delete-managed-resource" }

// MutateToDeleting transitions the fulfillment to deleting, stores the
// delete request auth for later background remove/retry passes, and
// bumps its generation inside a serialized write transaction.
func (s *DeleteManagedResourceWorkflowSpec) MutateToDeleting() Activity[DeleteManagedResourceInput, managedResourceMutationResult] {
	return NewActivity("mr-mutate-to-deleting", func(ctx context.Context, in DeleteManagedResourceInput) (managedResourceMutationResult, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return managedResourceMutationResult{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		mr, err := tx.ManagedResources().GetInstance(ctx, in.ResourceType, in.Name)
		if err != nil {
			return managedResourceMutationResult{}, err
		}

		intent, err := tx.ManagedResources().GetIntent(ctx, in.ResourceType, in.Name, mr.CurrentVersion)
		if err != nil {
			return managedResourceMutationResult{}, fmt.Errorf("get intent: %w", err)
		}

		f, err := tx.Fulfillments().Get(ctx, mr.FulfillmentID)
		if err != nil {
			return managedResourceMutationResult{}, err
		}

		// Delete retries read auth from fulfillment state, not the RPC context.
		f.Auth = in.Auth
		f.State = FulfillmentStateDeleting
		f.BumpGeneration()
		if err := tx.Fulfillments().Update(ctx, f); err != nil {
			return managedResourceMutationResult{}, fmt.Errorf("update fulfillment: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return managedResourceMutationResult{}, fmt.Errorf("commit: %w", err)
		}

		return managedResourceMutationResult{
			View: ManagedResourceView{
				ManagedResource: *mr,
				Intent:          intent,
				Fulfillment:     *f,
			},
			FulfillmentID: mr.FulfillmentID,
			MyGen:         f.Generation,
		}, nil
	})
}

// LoadFulfillment reads the current fulfillment state for convergence
// checks.
func (s *DeleteManagedResourceWorkflowSpec) LoadFulfillment() Activity[FulfillmentID, *Fulfillment] {
	return NewActivity("mr-load-fulfillment-for-delete", func(ctx context.Context, id FulfillmentID) (*Fulfillment, error) {
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
// [DeleteManagedResourceCleanupWorkflow]. Managed resources have no
// peer deployment row, so the cleanup workflow deletes the managed
// resource row and fulfillment row after orchestration signals
// completion.
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
// cleanup workflow, then run the convergence loop to ensure
// orchestration picks up the new state. Returns the DELETING snapshot
// immediately; the actual row deletion happens asynchronously in the
// cleanup workflow.
func (s *DeleteManagedResourceWorkflowSpec) Run(record Record, input DeleteManagedResourceInput) (ManagedResourceView, error) {
	mr, err := RunActivity(record, s.MutateToDeleting(), input)
	if err != nil {
		return ManagedResourceView{}, fmt.Errorf("mutate to deleting: %w", err)
	}

	if _, err := RunActivity(record, s.StartCleanup(), DeleteManagedResourceCleanupInput{
		ResourceType:  input.ResourceType,
		Name:          input.Name,
		FulfillmentID: mr.FulfillmentID,
	}); err != nil {
		return ManagedResourceView{}, fmt.Errorf("start cleanup: %w", err)
	}

	if err := convergenceLoop(record, s.Orchestration, s.LoadFulfillment(), mr.FulfillmentID, mr.MyGen, true); err != nil {
		return ManagedResourceView{}, err
	}

	return mr.View, nil
}
