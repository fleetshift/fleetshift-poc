package application

import (
	"context"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// PlatformResourceService manages the lifecycle of platform resource
// identities: create, get, and list. This is the thin application-layer
// entry point used by platform API handlers.
type PlatformResourceService struct {
	store domain.Store
	now   func() time.Time
}

// PlatformResourceServiceOption configures a [PlatformResourceService].
type PlatformResourceServiceOption func(*PlatformResourceService)

// WithPlatformResourceClock overrides the wall-clock used for
// timestamps (e.g. creation time). Defaults to [time.Now].
func WithPlatformResourceClock(fn func() time.Time) PlatformResourceServiceOption {
	return func(s *PlatformResourceService) { s.now = fn }
}

// NewPlatformResourceService creates a service with the given store
// and options.
func NewPlatformResourceService(store domain.Store, opts ...PlatformResourceServiceOption) *PlatformResourceService {
	s := &PlatformResourceService{
		store: store,
		now:   time.Now,
	}
	for _, o := range opts {
		o(s)
	}
	return s
}

// CreatePlatformResourceInput carries the fields needed to create or
// claim a platform resource identity.
type CreatePlatformResourceInput struct {
	Name   domain.ResourceName
	Labels map[string]string
}

// Create opens a read-write transaction, creates a new platform
// resource identity, and commits. The repository's unique constraint
// on resource_name surfaces [domain.ErrAlreadyExists] if the name is
// already taken (per AIP-133: Create must not silently update an
// existing resource).
func (s *PlatformResourceService) Create(ctx context.Context, in CreatePlatformResourceInput) (*domain.PlatformResource, error) {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	now := s.now()
	uid := domain.NewPlatformResourceUID()
	pr := domain.NewPlatformResource(uid, in.Name, in.Labels, now)

	if err := tx.ResourceIdentities().Create(ctx, pr); err != nil {
		return nil, fmt.Errorf("create: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return pr, nil
}

// Get retrieves a platform resource by its resource name.
func (s *PlatformResourceService) Get(ctx context.Context, name domain.ResourceName) (*domain.PlatformResource, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	pr, err := tx.ResourceIdentities().GetByName(ctx, name)
	if err != nil {
		return nil, err
	}
	return pr, tx.Commit()
}

// List returns all platform resources in a collection.
func (s *PlatformResourceService) List(ctx context.Context, collection domain.CollectionName) ([]*domain.PlatformResource, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	all, err := tx.ResourceIdentities().ListByCollection(ctx, collection)
	if err != nil {
		return nil, err
	}
	return all, tx.Commit()
}
