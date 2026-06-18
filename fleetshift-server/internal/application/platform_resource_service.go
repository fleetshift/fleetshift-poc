package application

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// PlatformResourceService manages the lifecycle of platform resource
// identities: create, get, list, and delete. This is the thin
// application-layer entry point used by platform API handlers.
type PlatformResourceService struct {
	store domain.Store
	now   func() time.Time
}

// PlatformResourceServiceOption configures a [PlatformResourceService].
type PlatformResourceServiceOption func(*PlatformResourceService)

// WithPlatformResourceClock overrides the wall-clock used for
// timestamps (e.g. creation and deletion times). Defaults to
// [time.Now].
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
	CollectionID domain.CollectionID
	ID           string
	Labels       map[string]string
}

// Create opens a read-write transaction, creates a new platform
// resource identity, and commits. The repository's unique constraint
// on relative_name surfaces [domain.ErrAlreadyExists] if the name is
// already taken (per AIP-133: Create must not silently update an
// existing resource).
func (s *PlatformResourceService) Create(ctx context.Context, in CreatePlatformResourceInput) (*domain.PlatformResource, error) {
	if in.CollectionID == "" {
		return nil, fmt.Errorf("%w: collection ID is required", domain.ErrInvalidArgument)
	}
	if in.ID == "" {
		return nil, fmt.Errorf("%w: resource ID is required", domain.ErrInvalidArgument)
	}

	name, err := domain.NewRelativeResourceName(in.CollectionID, in.ID)
	if err != nil {
		return nil, err
	}

	tx, err := s.store.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	now := s.now()
	uid := domain.PlatformResourceUID(uuid.New().String())
	pr := domain.NewPlatformResource(uid, in.CollectionID, name, in.Labels, now)

	if err := tx.ResourceIdentities().Create(ctx, pr); err != nil {
		return nil, fmt.Errorf("create: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return pr, nil
}

// Get retrieves a platform resource by its collection and ID.
func (s *PlatformResourceService) Get(ctx context.Context, collection domain.CollectionID, id string) (*domain.PlatformResource, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	name, err := domain.NewRelativeResourceName(collection, id)
	if err != nil {
		return nil, err
	}

	pr, err := tx.ResourceIdentities().GetByName(ctx, name)
	if err != nil {
		return nil, err
	}
	return pr, tx.Commit()
}

// List returns all active (non-deleted) platform resources in a collection.
func (s *PlatformResourceService) List(ctx context.Context, collection domain.CollectionID) ([]*domain.PlatformResource, error) {
	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	all, err := tx.ResourceIdentities().ListByCollection(ctx, collection)
	if err != nil {
		return nil, err
	}

	active := make([]*domain.PlatformResource, 0, len(all))
	for _, r := range all {
		if r.DeletedAt() == nil {
			active = append(active, r)
		}
	}
	return active, tx.Commit()
}

// Delete soft-deletes a platform resource by its collection and ID.
func (s *PlatformResourceService) Delete(ctx context.Context, collection domain.CollectionID, id string) (*domain.PlatformResource, error) {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	name, err := domain.NewRelativeResourceName(collection, id)
	if err != nil {
		return nil, err
	}

	pr, err := tx.ResourceIdentities().GetByName(ctx, name)
	if err != nil {
		return nil, err
	}

	now := s.now()
	if err := pr.SoftDelete(now); err != nil {
		return nil, err
	}

	if err := tx.ResourceIdentities().Update(ctx, pr); err != nil {
		return nil, fmt.Errorf("update: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return pr, nil
}
