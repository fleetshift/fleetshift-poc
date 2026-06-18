package application

import (
	"context"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ManagedResourceTypeService manages the lifecycle of managed resource
// type definitions. These are metadata records registered by addons to
// declare ownership of a resource type and its fulfillment relation.
type ManagedResourceTypeService struct {
	Store domain.Store
}

// CreateTypeInput carries the fields needed to register a new managed
// resource type.
type CreateTypeInput struct {
	ResourceType   domain.ResourceType
	Relation       domain.FulfillmentRelation
	Signature      domain.Signature
	APIServiceName domain.ServiceName
	APIVersion     domain.APIVersion
	CollectionID   domain.CollectionID
}

// Create registers a new managed resource type.
func (s *ManagedResourceTypeService) Create(ctx context.Context, in CreateTypeInput) (domain.ManagedResourceTypeDef, error) {
	if in.ResourceType == "" {
		return domain.ManagedResourceTypeDef{}, fmt.Errorf("%w: resource type is required", domain.ErrInvalidArgument)
	}
	if in.Relation == nil {
		return domain.ManagedResourceTypeDef{}, fmt.Errorf("%w: relation is required", domain.ErrInvalidArgument)
	}

	now := time.Now().UTC()
	def := domain.ManagedResourceTypeDef{
		ResourceType:   in.ResourceType,
		Relation:       in.Relation,
		Signature:      in.Signature,
		APIServiceName: in.APIServiceName,
		APIVersion:     in.APIVersion,
		CollectionID:   in.CollectionID,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return domain.ManagedResourceTypeDef{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := tx.ManagedResources().CreateType(ctx, def); err != nil {
		return domain.ManagedResourceTypeDef{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.ManagedResourceTypeDef{}, fmt.Errorf("commit: %w", err)
	}
	return def, nil
}

// Get retrieves a managed resource type definition by resource type.
func (s *ManagedResourceTypeService) Get(ctx context.Context, rt domain.ResourceType) (domain.ManagedResourceTypeDef, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.ManagedResourceTypeDef{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	def, err := tx.ManagedResources().GetType(ctx, rt)
	if err != nil {
		return domain.ManagedResourceTypeDef{}, err
	}
	return def, tx.Commit()
}

// List returns all registered managed resource type definitions.
func (s *ManagedResourceTypeService) List(ctx context.Context) ([]domain.ManagedResourceTypeDef, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	defs, err := tx.ManagedResources().ListTypes(ctx)
	if err != nil {
		return nil, err
	}
	return defs, tx.Commit()
}

// Delete removes a managed resource type definition.
func (s *ManagedResourceTypeService) Delete(ctx context.Context, rt domain.ResourceType) error {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := tx.ManagedResources().DeleteType(ctx, rt); err != nil {
		return err
	}
	return tx.Commit()
}
