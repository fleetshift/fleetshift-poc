package application

import (
	"context"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ExtensionResourceTypeService manages the lifecycle of extension
// resource type definitions. These are metadata records registered by
// addons to declare an extension resource type, optionally with
// management metadata (fulfillment relation and attestation signature).
//
// For now, all registered types carry management metadata; the
// management field is modeled as optional to establish the boundary for
// future inventory-only types.
type ExtensionResourceTypeService struct {
	store domain.Store
	now   func() time.Time
}

// ExtensionResourceTypeServiceOption configures an
// [ExtensionResourceTypeService].
type ExtensionResourceTypeServiceOption func(*ExtensionResourceTypeService)

// WithExtensionResourceTypeClock overrides the wall-clock used for
// timestamps (e.g. CreatedAt / UpdatedAt on type definitions).
// Defaults to [time.Now]. A nil fn is treated as a no-op to prevent
// nil-dereference panics at runtime.
func WithExtensionResourceTypeClock(fn func() time.Time) ExtensionResourceTypeServiceOption {
	return func(s *ExtensionResourceTypeService) {
		if fn != nil {
			s.now = fn
		}
	}
}

// NewExtensionResourceTypeService creates a service with the given
// store and options.
func NewExtensionResourceTypeService(store domain.Store, opts ...ExtensionResourceTypeServiceOption) *ExtensionResourceTypeService {
	s := &ExtensionResourceTypeService{
		store: store,
		now:   time.Now,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// Store returns the underlying store. This accessor exists so that
// dependents (e.g. [AddonManager]) can share the store without
// coupling to the service's internal layout.
func (s *ExtensionResourceTypeService) Store() domain.Store { return s.store }

// CreateExtensionTypeInput carries the fields needed to register a new
// extension resource type. The API service name is derived from the
// [domain.ResourceType]'s service component per AIP-123.
type CreateExtensionTypeInput struct {
	ResourceType domain.ResourceType
	APIVersion   domain.APIVersion
	CollectionID domain.CollectionID

	// Management is optional. When present, the type's instances are
	// managed resources with a fulfillment relation and attestation.
	// Enforcement of when management is required is left to callers
	// (e.g. AddonManager), not here, because the type service should
	// support inventory-only types.
	Management *CreateExtensionTypeManagementInput

	// Inventory is optional. When present, the type's instances support
	// inventory reporting (condition/observation lifecycle).
	Inventory *CreateExtensionTypeInventoryInput
}

// CreateExtensionTypeManagementInput carries management-specific fields
// for type registration.
type CreateExtensionTypeManagementInput struct {
	Relation  domain.FulfillmentRelation
	Signature domain.Signature
}

// CreateExtensionTypeInventoryInput is a marker input that opts a type
// into inventory reporting. It carries no fields today but exists as a
// struct so future inventory-specific registration parameters have a
// natural home.
type CreateExtensionTypeInventoryInput struct{}

// Create registers a new extension resource type.
func (s *ExtensionResourceTypeService) Create(ctx context.Context, in CreateExtensionTypeInput) (domain.ExtensionResourceType, error) {
	now := s.now()

	var opts []domain.ExtensionResourceTypeOption
	if in.Management != nil {
		if in.Management.Relation == nil {
			return domain.ExtensionResourceType{}, fmt.Errorf("%w: management relation is required", domain.ErrInvalidArgument)
		}
		opts = append(opts, domain.WithManagement(in.Management.Relation, in.Management.Signature))
	}
	if in.Inventory != nil {
		opts = append(opts, domain.WithInventory())
	}

	def := domain.NewExtensionResourceType(
		in.ResourceType,
		in.APIVersion,
		in.CollectionID,
		now,
		opts...,
	)

	tx, err := s.store.Begin(ctx)
	if err != nil {
		return domain.ExtensionResourceType{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := tx.ExtensionResources().CreateType(ctx, def); err != nil {
		return domain.ExtensionResourceType{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.ExtensionResourceType{}, fmt.Errorf("commit: %w", err)
	}
	return def, nil
}

// Get retrieves an extension resource type definition by resource type.
func (s *ExtensionResourceTypeService) Get(ctx context.Context, rt domain.ResourceType) (domain.ExtensionResourceType, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return domain.ExtensionResourceType{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	def, err := tx.ExtensionResources().GetType(ctx, rt)
	if err != nil {
		return domain.ExtensionResourceType{}, err
	}
	return def, tx.Commit()
}

// List returns all registered extension resource type definitions.
func (s *ExtensionResourceTypeService) List(ctx context.Context) ([]domain.ExtensionResourceType, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	defs, err := tx.ExtensionResources().ListTypes(ctx)
	if err != nil {
		return nil, err
	}
	return defs, tx.Commit()
}

// Delete removes an extension resource type definition.
func (s *ExtensionResourceTypeService) Delete(ctx context.Context, rt domain.ResourceType) error {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := tx.ExtensionResources().DeleteType(ctx, rt); err != nil {
		return err
	}
	return tx.Commit()
}
