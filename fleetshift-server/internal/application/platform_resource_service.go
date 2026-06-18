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
	Store domain.Store
}

// CreatePlatformResourceInput carries the fields needed to create or
// claim a platform resource identity.
type CreatePlatformResourceInput struct {
	CollectionID domain.CollectionID
	ID           string
	Labels       map[string]string
}

// Create opens a read-write transaction, calls ClaimOrGetIdentity to
// idempotently create or retrieve the platform resource identity, and
// commits. If the resource already existed, labels are updated to the
// provided values.
func (s *PlatformResourceService) Create(ctx context.Context, in CreatePlatformResourceInput) (*domain.PlatformResource, error) {
	if in.CollectionID == "" {
		return nil, fmt.Errorf("%w: collection ID is required", domain.ErrInvalidArgument)
	}
	if in.ID == "" {
		return nil, fmt.Errorf("%w: resource ID is required", domain.ErrInvalidArgument)
	}

	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	uid := domain.PlatformResourceUID(uuid.New().String())
	pr, err := domain.ClaimOrGetIdentity(ctx, tx.ResourceIdentities(), uid, in.CollectionID, in.ID, in.Labels, now)
	if err != nil {
		return nil, fmt.Errorf("claim identity: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}
	return pr, nil
}

// Get retrieves a platform resource by its collection and ID.
func (s *PlatformResourceService) Get(ctx context.Context, collection domain.CollectionID, id string) (*domain.PlatformResource, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
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
	tx, err := s.Store.BeginReadOnly(ctx)
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
	tx, err := s.Store.Begin(ctx)
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

	now := time.Now().UTC()
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
