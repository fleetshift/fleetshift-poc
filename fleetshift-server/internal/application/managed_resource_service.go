package application

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ManagedResourceService manages the lifecycle of managed resource
// instances: create, read, list, and delete.
type ManagedResourceService struct {
	Store          domain.Store
	SchemaCompiler domain.SchemaCompiler
	CreateWF       domain.CreateManagedResourceWorkflow
	DeleteWF       domain.DeleteManagedResourceWorkflow
}

// CreateManagedResourceInput carries the fields needed to create a
// managed resource instance.
type CreateManagedResourceInput struct {
	ResourceType  domain.ResourceType
	Name          domain.ResourceName
	Spec          json.RawMessage
	Provenance    *domain.Provenance
}

// Create validates the spec against the registered schema, applies the
// fulfillment relation, and starts the create workflow.
func (s *ManagedResourceService) Create(ctx context.Context, in CreateManagedResourceInput) (domain.ManagedResourceView, error) {
	if in.ResourceType == "" {
		return domain.ManagedResourceView{}, fmt.Errorf("%w: resource type is required", domain.ErrInvalidArgument)
	}
	if in.Name == "" {
		return domain.ManagedResourceView{}, fmt.Errorf("%w: name is required", domain.ErrInvalidArgument)
	}
	if len(in.Spec) == 0 {
		return domain.ManagedResourceView{}, fmt.Errorf("%w: spec is required", domain.ErrInvalidArgument)
	}

	// Look up the type definition to get the relation and schema.
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	typeDef, err := tx.ManagedResources().GetType(ctx, in.ResourceType)
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("lookup type %q: %w", in.ResourceType, err)
	}
	_ = tx.Commit()

	// Validate spec against the registered schema.
	if typeDef.SpecSchema != nil && s.SchemaCompiler != nil {
		schema, err := s.SchemaCompiler.Compile(*typeDef.SpecSchema)
		if err != nil {
			return domain.ManagedResourceView{}, fmt.Errorf("compile schema for %q: %w", in.ResourceType, err)
		}
		if err := schema.Validate(in.Spec); err != nil {
			return domain.ManagedResourceView{}, fmt.Errorf("%w: %v", domain.ErrInvalidArgument, err)
		}
	}

	// Use caller-provided provenance if set. When nil, the fulfillment
	// is created without provenance — attestation assembly at delivery
	// time will be skipped. Full provenance (with user signature) will
	// be implemented once the signing flow is wired for managed resources.
	var prov *domain.Provenance
	if in.Provenance != nil {
		prov = in.Provenance
	}

	exec, err := s.CreateWF.Start(ctx, domain.CreateManagedResourceInput{
		ResourceType: in.ResourceType,
		Name:         in.Name,
		Spec:         in.Spec,
		TypeDef:      typeDef,
		Provenance:   prov,
	})
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("start create workflow: %w", err)
	}

	return exec.AwaitResult(ctx)
}

// Get retrieves a managed resource view by type and name.
func (s *ManagedResourceService) Get(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (domain.ManagedResourceView, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	view, err := tx.ManagedResources().GetView(ctx, rt, name)
	if err != nil {
		return domain.ManagedResourceView{}, err
	}
	return view, tx.Commit()
}

// List returns all managed resource views for a given type.
func (s *ManagedResourceService) List(ctx context.Context, rt domain.ResourceType) ([]domain.ManagedResourceView, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	views, err := tx.ManagedResources().ListViewsByType(ctx, rt)
	if err != nil {
		return nil, err
	}
	return views, tx.Commit()
}

// Delete starts the delete workflow for a managed resource.
func (s *ManagedResourceService) Delete(ctx context.Context, rt domain.ResourceType, name domain.ResourceName) (domain.ManagedResourceView, error) {
	exec, err := s.DeleteWF.Start(ctx, domain.DeleteManagedResourceInput{
		ResourceType: rt,
		Name:         name,
	})
	if err != nil {
		return domain.ManagedResourceView{}, fmt.Errorf("start delete workflow: %w", err)
	}

	return exec.AwaitResult(ctx)
}
