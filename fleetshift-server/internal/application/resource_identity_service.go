package application

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ResourceIdentityService manages canonical platform resource
// identities, representations, aliases, and relationships. It is
// protocol-agnostic and delegates persistence to
// [domain.ResourceIdentityRepository] via a transactional [domain.Store].
type ResourceIdentityService struct {
	Store domain.Store
	Now   func() time.Time
	NewID func() domain.PlatformResourceUID
}

func (s *ResourceIdentityService) now() time.Time {
	if s.Now != nil {
		return s.Now()
	}
	return time.Now().UTC()
}

func (s *ResourceIdentityService) newID() domain.PlatformResourceUID {
	if s.NewID != nil {
		return s.NewID()
	}
	return domain.PlatformResourceUID(uuid.New().String())
}

// ---------------------------------------------------------------------------
// Input types
// ---------------------------------------------------------------------------

// ClaimPlatformResourceInput is the input for [ResourceIdentityService.ClaimOrGet].
type ClaimPlatformResourceInput struct {
	CollectionID domain.CollectionID
	RelativeName domain.RelativeResourceName
	Labels       map[string]string
	Aliases      []domain.Alias
}

// AttachRepresentationInput is the input for
// [ResourceIdentityService.AttachRepresentation].
type AttachRepresentationInput struct {
	PlatformUID  domain.PlatformResourceUID
	ServiceName  domain.ServiceName
	Version      domain.APIVersion
	CollectionID domain.CollectionID
	RelativeName domain.RelativeResourceName
	Roles        []domain.RepresentationRole
	Labels       map[string]string
}

// SetPlatformResourceLabelsInput is the input for
// [ResourceIdentityService.SetLabels].
type SetPlatformResourceLabelsInput struct {
	PlatformUID domain.PlatformResourceUID
	Labels      map[string]string
}

// AddRelationshipInput is the input for
// [ResourceIdentityService.AddRelationship].
type AddRelationshipInput struct {
	SourceUID     domain.PlatformResourceUID
	Type          domain.RelationshipType
	TargetUID     domain.PlatformResourceUID
	SourceService domain.ServiceName
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

// ClaimOrGet creates a new platform resource or returns the existing
// one if one already exists with the same relative name. Alias
// attachment is atomic: if any alias conflicts, the entire operation
// rolls back (no partially created platform resource).
func (s *ResourceIdentityService) ClaimOrGet(ctx context.Context, in ClaimPlatformResourceInput) (domain.PlatformResourceView, error) {
	if err := domain.ValidateCollectionID(in.CollectionID); err != nil {
		return domain.PlatformResourceView{}, err
	}
	if err := domain.ValidateRelativeResourceName(in.RelativeName); err != nil {
		return domain.PlatformResourceView{}, err
	}
	if in.RelativeName.CollectionID() != in.CollectionID {
		return domain.PlatformResourceView{}, fmt.Errorf("collection_id %q does not match relative_name %q: %w", in.CollectionID, in.RelativeName, domain.ErrInvalidArgument)
	}

	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return domain.PlatformResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	repo := tx.ResourceIdentities()

	var r *domain.PlatformResource
	existing, err := repo.GetPlatformResourceByName(ctx, in.RelativeName)
	if err == nil {
		r = existing
	} else if !errors.Is(err, domain.ErrNotFound) {
		return domain.PlatformResourceView{}, fmt.Errorf("check existing: %w", err)
	} else {
		now := s.now()
		r = domain.NewPlatformResource(s.newID(), in.CollectionID, in.RelativeName, in.Labels, now)
		if err := repo.CreatePlatformResource(ctx, r); err != nil {
			return domain.PlatformResourceView{}, err
		}
	}

	now := s.now()
	for _, alias := range in.Aliases {
		if err := domain.ValidateAlias(alias); err != nil {
			return domain.PlatformResourceView{}, err
		}
		if err := repo.PutAlias(ctx, r.UID(), alias, now); err != nil {
			return domain.PlatformResourceView{}, err
		}
	}

	view, err := s.assembleView(ctx, repo, r)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}
	return view, tx.Commit()
}

// GetByUID retrieves a platform resource view by its UID.
func (s *ResourceIdentityService) GetByUID(ctx context.Context, uid domain.PlatformResourceUID) (domain.PlatformResourceView, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.PlatformResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	repo := tx.ResourceIdentities()
	r, err := repo.GetPlatformResourceByUID(ctx, uid)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}
	view, err := s.assembleView(ctx, repo, r)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}
	return view, tx.Commit()
}

// GetByRelativeName retrieves a platform resource view by its relative
// name.
func (s *ResourceIdentityService) GetByRelativeName(ctx context.Context, name domain.RelativeResourceName) (domain.PlatformResourceView, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return domain.PlatformResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	repo := tx.ResourceIdentities()
	r, err := repo.GetPlatformResourceByName(ctx, name)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}
	view, err := s.assembleView(ctx, repo, r)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}
	return view, tx.Commit()
}

// AttachRepresentation attaches or updates an extension representation
// on a platform resource. Returns ErrAlreadyExists if the same
// service/collection/relative_name is already attached to a different
// platform UID.
func (s *ResourceIdentityService) AttachRepresentation(ctx context.Context, in AttachRepresentationInput) (domain.PlatformResourceView, error) {
	if err := domain.ValidateServiceName(in.ServiceName); err != nil {
		return domain.PlatformResourceView{}, err
	}
	if err := domain.ValidateAPIVersion(in.Version); err != nil {
		return domain.PlatformResourceView{}, err
	}
	if err := domain.ValidateCollectionID(in.CollectionID); err != nil {
		return domain.PlatformResourceView{}, err
	}
	if err := domain.ValidateRelativeResourceName(in.RelativeName); err != nil {
		return domain.PlatformResourceView{}, err
	}
	if in.RelativeName.CollectionID() != in.CollectionID {
		return domain.PlatformResourceView{}, fmt.Errorf("collection_id %q does not match relative_name %q: %w", in.CollectionID, in.RelativeName, domain.ErrInvalidArgument)
	}
	if err := domain.ValidateRepresentationRoles(in.Roles); err != nil {
		return domain.PlatformResourceView{}, err
	}

	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return domain.PlatformResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	repo := tx.ResourceIdentities()

	r, err := repo.GetPlatformResourceByUID(ctx, in.PlatformUID)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}

	now := s.now()
	rep := domain.ResourceRepresentation{
		PlatformUID:  in.PlatformUID,
		ServiceName:  in.ServiceName,
		Version:      in.Version,
		CollectionID: in.CollectionID,
		RelativeName: in.RelativeName,
		Roles:        in.Roles,
		Labels:       in.Labels,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := repo.PutRepresentation(ctx, rep); err != nil {
		return domain.PlatformResourceView{}, err
	}

	view, err := s.assembleView(ctx, repo, r)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}
	return view, tx.Commit()
}

// SetLabels replaces the platform labels on a resource and returns the
// updated view with recomputed effective labels.
func (s *ResourceIdentityService) SetLabels(ctx context.Context, in SetPlatformResourceLabelsInput) (domain.PlatformResourceView, error) {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return domain.PlatformResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	repo := tx.ResourceIdentities()

	r, err := repo.GetPlatformResourceByUID(ctx, in.PlatformUID)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}

	r.SetLabels(in.Labels, s.now())
	if err := repo.UpdatePlatformResourceLabels(ctx, r); err != nil {
		return domain.PlatformResourceView{}, err
	}

	view, err := s.assembleView(ctx, repo, r)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}
	return view, tx.Commit()
}

// AddAliases attaches one or more aliases to a platform resource.
func (s *ResourceIdentityService) AddAliases(ctx context.Context, uid domain.PlatformResourceUID, aliases []domain.Alias) (domain.PlatformResourceView, error) {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return domain.PlatformResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	repo := tx.ResourceIdentities()

	r, err := repo.GetPlatformResourceByUID(ctx, uid)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}

	now := s.now()
	for _, alias := range aliases {
		if err := domain.ValidateAlias(alias); err != nil {
			return domain.PlatformResourceView{}, err
		}
		if err := repo.PutAlias(ctx, uid, alias, now); err != nil {
			return domain.PlatformResourceView{}, err
		}
	}

	view, err := s.assembleView(ctx, repo, r)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}
	return view, tx.Commit()
}

// TombstoneRepresentation marks a representation as deleted without
// removing the platform resource.
func (s *ResourceIdentityService) TombstoneRepresentation(ctx context.Context, service domain.ServiceName, collection domain.CollectionID, name domain.RelativeResourceName) error {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := tx.ResourceIdentities().TombstoneRepresentation(ctx, service, collection, name, s.now()); err != nil {
		return err
	}
	return tx.Commit()
}

// AddRelationship records a typed edge from one platform resource to
// another.
func (s *ResourceIdentityService) AddRelationship(ctx context.Context, in AddRelationshipInput) (domain.PlatformResourceView, error) {
	if err := domain.ValidateRelationshipType(in.Type); err != nil {
		return domain.PlatformResourceView{}, err
	}

	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return domain.PlatformResourceView{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	repo := tx.ResourceIdentities()

	r, err := repo.GetPlatformResourceByUID(ctx, in.SourceUID)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}

	if _, err := repo.GetPlatformResourceByUID(ctx, in.TargetUID); err != nil {
		return domain.PlatformResourceView{}, fmt.Errorf("target resource: %w", err)
	}

	rel := domain.ResourceRelationship{
		SourceUID:     in.SourceUID,
		Type:          in.Type,
		TargetUID:     in.TargetUID,
		SourceService: in.SourceService,
		CreatedAt:     s.now(),
	}
	if err := repo.PutRelationship(ctx, rel); err != nil {
		return domain.PlatformResourceView{}, err
	}

	view, err := s.assembleView(ctx, repo, r)
	if err != nil {
		return domain.PlatformResourceView{}, err
	}
	return view, tx.Commit()
}

// ---------------------------------------------------------------------------
// View assembly
// ---------------------------------------------------------------------------

func (s *ResourceIdentityService) assembleView(ctx context.Context, repo domain.ResourceIdentityRepository, r *domain.PlatformResource) (domain.PlatformResourceView, error) {
	reps, err := repo.ListRepresentationsByPlatformUID(ctx, r.UID())
	if err != nil {
		return domain.PlatformResourceView{}, fmt.Errorf("list representations: %w", err)
	}
	aliases, err := repo.ListAliasesByPlatformUID(ctx, r.UID())
	if err != nil {
		return domain.PlatformResourceView{}, fmt.Errorf("list aliases: %w", err)
	}
	rels, err := repo.ListRelationshipsBySourceUID(ctx, r.UID())
	if err != nil {
		return domain.PlatformResourceView{}, fmt.Errorf("list relationships: %w", err)
	}

	return domain.PlatformResourceView{
		Resource:        r,
		Representations: reps,
		Aliases:         aliases,
		Relationships:   rels,
		EffectiveLabels: domain.EffectiveLabels(r.Labels(), reps),
	}, nil
}
