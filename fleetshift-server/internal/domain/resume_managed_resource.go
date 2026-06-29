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
	ResourceType       ResourceType
	Name               ResourceName
	Auth               DeliveryAuth // fresh caller credentials for the resumed resource
	UserSignature      []byte       // ECDSA-P256-SHA256 re-signing material; empty for unsigned
	ValidUntil         time.Time    // client-supplied attestation expiry; zero for unsigned
	Etag               Etag         // optimistic concurrency token; empty means skip check
	ExpectedGeneration Generation   // client-supplied next generation; zero means skip check (unsigned legacy)
}

// ResumeManagedResourceWorkflowSpec transitions a paused managed
// resource fulfillment back to active reconciliation by updating
// auth/provenance, bumping its generation, and running a convergence
// loop.
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

		fullName := in.ResourceType.FullName(in.Name)
		er, err := tx.ExtensionResources().Get(ctx, fullName)
		if err != nil {
			return managedResourceMutationResult{}, err
		}
		managed := er.Managed()
		if managed == nil {
			return managedResourceMutationResult{}, fmt.Errorf(
				"%w: extension resource %s has no managed state",
				ErrInvalidArgument, fullName)
		}

		intent, err := tx.ExtensionResources().GetIntent(ctx, er.UID(), managed.CurrentVersion())
		if err != nil {
			return managedResourceMutationResult{}, fmt.Errorf("get intent: %w", err)
		}

		f, err := tx.Fulfillments().Get(ctx, managed.FulfillmentID())
		if err != nil {
			return managedResourceMutationResult{}, err
		}

		// Etag check: construct the extension resource view and compare
		// against the client's token.
		currentView := ExtensionResourceView{
			Resource:    *er,
			Intent:      &intent,
			Fulfillment: f,
		}
		if in.Etag != "" && in.Etag != currentView.Etag() {
			return managedResourceMutationResult{}, TerminalError(fmt.Errorf(
				"%w: etag mismatch (client sent %q, current is %q)",
				ErrStaleGeneration, in.Etag, currentView.Etag()))
		}

		nextGen := f.Generation() + 1

		// Expected-generation check: if supplied, it must match the
		// next generation the server is about to produce.
		if in.ExpectedGeneration != 0 && in.ExpectedGeneration != nextGen {
			return managedResourceMutationResult{}, TerminalError(fmt.Errorf(
				"%w: expected_generation mismatch (client sent %d, server will produce %d)",
				ErrStaleGeneration, in.ExpectedGeneration, nextGen))
		}

		// Signed resumes must supply expected_generation so the server
		// can bind it into provenance without inferring it.
		if len(in.UserSignature) > 0 && in.ExpectedGeneration == 0 {
			return managedResourceMutationResult{}, TerminalError(fmt.Errorf(
				"%w: expected_generation is required when user_signature is present",
				ErrInvalidArgument))
		}

		var prov *Provenance
		if f.Provenance() != nil || len(in.UserSignature) > 0 {
			provenanceGen := in.ExpectedGeneration
			if provenanceGen == 0 {
				provenanceGen = nextGen
			}
			prov, err = s.ProvenanceSvc.BuildManagedResourceProvenance(
				ctx, tx.SignerEnrollments(), in.Auth.Caller,
				in.ResourceType, in.Name, intent.Spec,
				provenanceGen, in.UserSignature, in.ValidUntil,
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
			View: ExtensionResourceView{
				Resource:    *er,
				Intent:      &intent,
				Fulfillment: f,
			},
			FulfillmentID: managed.FulfillmentID(),
			MyGen:         f.Generation(),
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
func (s *ResumeManagedResourceWorkflowSpec) Run(record Record, input ResumeManagedResourceInput) (ExtensionResourceView, error) {
	mr, err := RunActivity(record, s.MutateToResumed(), input)
	if err != nil {
		return ExtensionResourceView{}, fmt.Errorf("mutate to resumed: %w", err)
	}

	if err := convergenceLoop(record, s.Orchestration, s.LoadFulfillment(), mr.FulfillmentID, mr.MyGen, false); err != nil {
		return ExtensionResourceView{}, err
	}

	return mr.View, nil
}
