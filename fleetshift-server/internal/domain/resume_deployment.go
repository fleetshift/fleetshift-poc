package domain

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ResumeDeploymentInput carries the minimal durable payload needed to
// resume a paused deployment. It intentionally excludes transport-only
// state (full AuthorizationContext, request metadata, peer addresses).
type ResumeDeploymentInput struct {
	ID            DeploymentID
	Auth          DeliveryAuth // fresh caller credentials for the resumed deployment
	UserSignature []byte       // ECDSA-P256-SHA256 re-signing material; empty for unsigned
	ValidUntil    time.Time    // client-supplied attestation expiry; zero for unsigned
}

// ProvenanceBuilder constructs [Provenance] for a mutation that
// requires re-signing. Implementations live in the application layer
// and wrap key resolution and signature verification.
type ProvenanceBuilder interface {
	BuildProvenance(
		ctx context.Context,
		enrollments SignerEnrollmentRepository,
		caller *SubjectClaims,
		id DeploymentID,
		ms ManifestStrategySpec,
		ps PlacementStrategySpec,
		generation Generation,
		userSig []byte,
		validUntil time.Time,
	) (*Provenance, error)
}

// ResumeDeploymentWorkflowSpec transitions a [FulfillmentStatePausedAuth]
// fulfillment back to active by updating auth/provenance, bumping its
// generation, and running a convergence loop.
//
// Pass this spec to [Registry.RegisterResumeDeployment] to obtain a
// [ResumeDeploymentWorkflow] that can start instances.
type ResumeDeploymentWorkflowSpec struct {
	Store             Store
	Orchestration     OrchestrationWorkflow
	ProvenanceBuilder ProvenanceBuilder // nil when signing is not configured
}

func (s *ResumeDeploymentWorkflowSpec) Name() string { return "resume-deployment" }

// MutateToResumed updates the fulfillment with fresh auth/provenance
// and bumps its generation inside a serialized write transaction.
// The provenance is built against the actual next generation seen in
// the write transaction, not a pre-read snapshot.
func (s *ResumeDeploymentWorkflowSpec) MutateToResumed() Activity[ResumeDeploymentInput, MutationResult] {
	return NewActivity("mutate-to-resumed", func(ctx context.Context, in ResumeDeploymentInput) (MutationResult, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return MutationResult{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		dep, err := tx.Deployments().Get(ctx, in.ID)
		if err != nil {
			return MutationResult{}, err
		}

		f, err := tx.Fulfillments().Get(ctx, dep.FulfillmentID)
		if err != nil {
			return MutationResult{}, err
		}

		if f.State != FulfillmentStatePausedAuth {
			return MutationResult{}, fmt.Errorf("%w: deployment %q is in state %q, not paused_auth",
				ErrInvalidArgument, in.ID, f.State)
		}

		f.Auth = in.Auth

		hadProvenance := f.Provenance != nil
		if hadProvenance || len(in.UserSignature) > 0 {
			if hadProvenance && len(in.UserSignature) == 0 {
				return MutationResult{}, fmt.Errorf(
					"%w: deployment %q has provenance; re-signing is required to resume",
					ErrInvalidArgument, in.ID)
			}
			if s.ProvenanceBuilder == nil {
				return MutationResult{}, fmt.Errorf(
					"%w: signing not configured but deployment %q requires provenance",
					ErrInvalidArgument, in.ID)
			}
			nextGen := f.Generation + 1
			prov, err := s.ProvenanceBuilder.BuildProvenance(
				ctx, tx.SignerEnrollments(), in.Auth.Caller,
				dep.ID, f.ManifestStrategy, f.PlacementStrategy,
				nextGen, in.UserSignature, in.ValidUntil,
			)
			if err != nil {
				return MutationResult{}, fmt.Errorf("build provenance: %w", err)
			}
			f.Provenance = prov
		}

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
func (s *ResumeDeploymentWorkflowSpec) LoadFulfillment() Activity[FulfillmentID, *Fulfillment] {
	return NewActivity("load-fulfillment-for-resume", func(ctx context.Context, id FulfillmentID) (*Fulfillment, error) {
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

// Run is the workflow body: mutate, then run the convergence-start loop.
func (s *ResumeDeploymentWorkflowSpec) Run(record Record, input ResumeDeploymentInput) (DeploymentView, error) {
	mr, err := RunActivity(record, s.MutateToResumed(), input)
	if err != nil {
		return DeploymentView{}, fmt.Errorf("mutate to resumed: %w", err)
	}

	if err := convergenceLoop(record, s.Orchestration, s.LoadFulfillment(), mr.FulfillmentID, mr.MyGen, false); err != nil {
		return DeploymentView{}, err
	}

	return mr.View, nil
}
