package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreateManagedResourceInput carries all the fields needed to create a
// managed resource instance. The application service pre-validates the
// spec against the registered JSON Schema before starting this workflow.
type CreateManagedResourceInput struct {
	ResourceType ResourceType
	Name         ResourceName
	Spec         json.RawMessage
	TypeDef      ManagedResourceTypeDef
	Provenance   *Provenance
}

// CreateManagedResourceWorkflowSpec persists a managed resource
// (HEAD + intent v1 + derived fulfillment) and starts orchestration.
// Follows the same structural pattern as [CreateDeploymentWorkflowSpec].
type CreateManagedResourceWorkflowSpec struct {
	Store         Store
	Orchestration OrchestrationWorkflow
	Now           func() time.Time
}

func (s *CreateManagedResourceWorkflowSpec) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func (s *CreateManagedResourceWorkflowSpec) Name() string { return "create-managed-resource" }

// PersistManagedResource creates the managed resource aggregate (with
// its initial intent) and derived fulfillment in a single transaction.
func (s *CreateManagedResourceWorkflowSpec) PersistManagedResource() Activity[CreateManagedResourceInput, ManagedResourceView] {
	return NewActivity("persist-managed-resource", func(ctx context.Context, in CreateManagedResourceInput) (ManagedResourceView, error) {
		tx, err := s.Store.Begin(ctx)
		if err != nil {
			return ManagedResourceView{}, fmt.Errorf("begin tx: %w", err)
		}
		defer tx.Rollback()

		now := s.now()
		fID := FulfillmentID(uuid.New().String())

		mr := ManagedResource{
			ResourceType:  in.ResourceType,
			Name:          in.Name,
			UID:           uuid.New().String(),
			FulfillmentID: fID,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		intent := mr.RecordIntent(in.Spec, now)

		ms, ps, rs := in.TypeDef.Relation.DeriveStrategies(intent)

		var attestRef *AttestationRef
		if in.Provenance != nil {
			rt := in.ResourceType
			attestRef = &AttestationRef{RelationRef: &rt}
		}

		f := Fulfillment{
			ID:             fID,
			State:          FulfillmentStateCreating,
			Provenance:     in.Provenance,
			AttestationRef: attestRef,
			Generation:     0,
			CreatedAt:      now,
			UpdatedAt:      now,
		}
		f.AdvanceManifestStrategy(ms, now)
		f.AdvancePlacementStrategy(ps, now)
		f.AdvanceRolloutStrategy(rs, now)

		if err := tx.Fulfillments().Create(ctx, &f); err != nil {
			return ManagedResourceView{}, fmt.Errorf("create fulfillment: %w", err)
		}

		if err := tx.ManagedResources().CreateInstance(ctx, &mr); err != nil {
			return ManagedResourceView{}, fmt.Errorf("create instance: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return ManagedResourceView{}, fmt.Errorf("commit: %w", err)
		}

		return ManagedResourceView{
			ManagedResource: mr,
			Intent:          intent,
			Fulfillment:     f,
		}, nil
	})
}

// StartOrchestration starts the orchestration workflow for the derived
// fulfillment.
func (s *CreateManagedResourceWorkflowSpec) StartOrchestration() Activity[FulfillmentID, struct{}] {
	return NewActivity("start-mr-orchestration", func(ctx context.Context, id FulfillmentID) (struct{}, error) {
		_, err := s.Orchestration.Start(ctx, id)
		return struct{}{}, err
	})
}

// Run is the workflow body: persist everything, then start orchestration.
func (s *CreateManagedResourceWorkflowSpec) Run(record Record, input CreateManagedResourceInput) (ManagedResourceView, error) {
	view, err := RunActivity(record, s.PersistManagedResource(), input)
	if err != nil {
		return ManagedResourceView{}, fmt.Errorf("persist managed resource: %w", err)
	}

	if _, err := RunActivity(record, s.StartOrchestration(), view.Fulfillment.ID); err != nil {
		return ManagedResourceView{}, fmt.Errorf("start orchestration: %w", err)
	}

	return view, nil
}
