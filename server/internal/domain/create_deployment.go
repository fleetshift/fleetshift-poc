package domain

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreateDeploymentInput is the specification for creating a new deployment.
type CreateDeploymentInput struct {
	Name              ResourceName
	ManifestStrategy  ManifestStrategySpec
	PlacementStrategy PlacementStrategySpec
	RolloutStrategy   *RolloutStrategySpec
	Auth              DeliveryAuth
	Provenance        *Provenance // set by the service layer after signature verification
	UserSignature     []byte      // ECDSA-P256-SHA256 signature; empty for unsigned deployments
	ValidUntil        time.Time   // client-supplied attestation expiry; zero for unsigned
}

// CreateDeploymentWorkflowSpec is a short-lived parent workflow that
// persists a new deployment + fulfillment and starts the orchestration
// workflow. Both steps are durable: on crash the engine replays from
// the last completed step.
//
// Pass this spec to [Registry.RegisterCreateDeployment] to obtain a
// [CreateDeploymentWorkflow] that can start instances.
type CreateDeploymentWorkflowSpec struct {
	Store         Store
	Orchestration OrchestrationWorkflow
	Now           func() time.Time
}

func (s *CreateDeploymentWorkflowSpec) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *CreateDeploymentWorkflowSpec) Name() string { return "create-deployment" }

// PersistDeployment creates both the fulfillment and thin deployment
// records in a single transaction.
func (s *CreateDeploymentWorkflowSpec) PersistDeployment() Activity[CreateDeploymentInput, DeploymentView] {
	return NewActivity("persist-deployment", func(ctx context.Context, in CreateDeploymentInput) (DeploymentView, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return DeploymentView{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		now := s.now()
		uid := NewDeploymentUID()
		fID := FulfillmentID(uuid.New().String())

		f := NewFulfillment(fID, in.Auth, in.Provenance, nil, now)
		f.AdvanceManifestStrategy(in.ManifestStrategy, now)
		f.AdvancePlacementStrategy(in.PlacementStrategy, now)
		f.AdvanceRolloutStrategy(in.RolloutStrategy, now)

		if err := tx.Fulfillments().Create(ctx, f); err != nil {
			return DeploymentView{}, err
		}

		dep := NewDeployment(in.Name, uid, fID, now)
		if err := tx.Deployments().Create(ctx, dep); err != nil {
			return DeploymentView{}, err
		}
		if err := tx.Commit(); err != nil {
			return DeploymentView{}, fmt.Errorf("commit: %w", err)
		}
		return DeploymentView{Deployment: dep, Fulfillment: *f}, nil
	})
}

// StartOrchestration returns an activity that durably starts the
// orchestration workflow for a fulfillment. The start is wrapped in
// an activity so it survives replay without re-executing.
func (s *CreateDeploymentWorkflowSpec) StartOrchestration() Activity[FulfillmentID, struct{}] {
	return NewActivity("start-orchestration", func(ctx context.Context, id FulfillmentID) (struct{}, error) {
		_, err := s.Orchestration.Start(ctx, id)
		return struct{}{}, err
	})
}

// Run is the workflow body: persist the deployment + fulfillment, then
// start orchestration as a durable activity.
func (s *CreateDeploymentWorkflowSpec) Run(record Record, input CreateDeploymentInput) (DeploymentView, error) {
	view, err := RunActivity(record, s.PersistDeployment(), input)
	if err != nil {
		return DeploymentView{}, fmt.Errorf("persist deployment: %w", err)
	}

	if _, err := RunActivity(record, s.StartOrchestration(), view.Fulfillment.ID()); err != nil {
		return DeploymentView{}, fmt.Errorf("start orchestration: %w", err)
	}

	return view, nil
}
