package domain

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ResumeManagedResourceInput carries the minimal durable payload needed
// to resume a paused managed resource. It intentionally excludes
// transport-only state (full AuthorizationContext, request metadata).
type ResumeManagedResourceInput struct {
	ResourceType  ResourceType
	Name          ResourceName
	Auth          DeliveryAuth // fresh caller credentials for the resumed resource
	UserSignature []byte       // ECDSA-P256-SHA256 re-signing material; empty for unsigned
	ValidUntil    time.Time    // client-supplied attestation expiry; zero for unsigned
}

// ResumeManagedResourceWorkflowSpec transitions a
// [FulfillmentStatePausedAuth] managed resource fulfillment back to
// active by updating auth/provenance, bumping its generation, and
// running a convergence loop.
//
// Pass this spec to [Registry.RegisterResumeManagedResource] to obtain
// a [ResumeManagedResourceWorkflow] that can start instances.
type ResumeManagedResourceWorkflowSpec struct {
	Store         Store
	Orchestration OrchestrationWorkflow
	ProvenanceSvc *ProvenanceService
}

func (s *ResumeManagedResourceWorkflowSpec) Name() string { return "resume-managed-resource" }

// MutateToResumed updates the fulfillment with fresh auth/provenance
// and bumps its generation inside a serialized write transaction.
// Provenance is built against the next generation using the current
// intent spec.
func (s *ResumeManagedResourceWorkflowSpec) MutateToResumed() Activity[ResumeManagedResourceInput, managedResourceMutationResult] {
	return NewActivity("mr-mutate-to-resumed", func(ctx context.Context, in ResumeManagedResourceInput) (managedResourceMutationResult, error) {
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

		var prov *Provenance
		if f.Provenance != nil || len(in.UserSignature) > 0 {
			nextGen := f.Generation + 1
			prov, err = s.ProvenanceSvc.BuildManagedResourceProvenance(
				ctx, tx.SignerEnrollments(), in.Auth.Caller,
				in.ResourceType, in.Name, intent.Spec,
				nextGen, in.UserSignature, in.ValidUntil,
			)
			if err != nil {
				return managedResourceMutationResult{}, fmt.Errorf("build provenance: %w", err)
			}
		}

		if err := f.Resume(in.Auth, prov); err != nil {
			return managedResourceMutationResult{}, err
		}

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
func (s *ResumeManagedResourceWorkflowSpec) LoadFulfillment() Activity[FulfillmentID, *Fulfillment] {
	return NewActivity("mr-load-fulfillment-for-resume", func(ctx context.Context, id FulfillmentID) (*Fulfillment, error) {
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

// Run is the workflow body: mutate, then run the convergence-start
// loop.
func (s *ResumeManagedResourceWorkflowSpec) Run(record Record, input ResumeManagedResourceInput) (ManagedResourceView, error) {
	mr, err := RunActivity(record, s.MutateToResumed(), input)
	if err != nil {
		return ManagedResourceView{}, fmt.Errorf("mutate to resumed: %w", err)
	}

	if err := convergenceLoop(record, s.Orchestration, s.LoadFulfillment(), mr.FulfillmentID, mr.MyGen, false); err != nil {
		return ManagedResourceView{}, err
	}

	return mr.View, nil
}
