package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// WholePlatformScope is the only QueryResources scope accepted in v0.
// It is the AIP-159 whole-platform wildcard ("-").
const WholePlatformScope = "-"

// QueryResourcesInput is the application-layer input for
// [ResourceQueryService.QueryResources].
type QueryResourcesInput struct {
	// Scope is a resource-hierarchy path segment sequence. v0 accepts
	// only [WholePlatformScope].
	Scope string

	// Filter is a CEL expression passed through to
	// [domain.QueryRepository.QueryResources]. Empty matches all
	// activated resource types in scope when the store's
	// [domain.QuerySchemaProvider] is set. Activation scoping and
	// inactive-type rejection live in the repository (see
	// [domain.ResolveQueryResourceTypeScope]).
	Filter string

	// PageSize is passed through to the repository. Non-positive values
	// fall back to the repository default; oversized values are clamped.
	PageSize int32

	// PageToken resumes a previous QueryResources call.
	PageToken string

	// OrderBy is passed through to the repository. Empty selects the
	// default stable order; the only other supported value is
	// "resource_type,name".
	OrderBy string
}

// ResourceQueryService exposes fleet-wide queries over managed
// extension resources via [domain.QueryRepository].
type ResourceQueryService struct {
	store domain.Store
}

// NewResourceQueryService creates a [ResourceQueryService] backed by
// store. Activation scoping uses the store's [domain.QuerySchemaProvider]
// (threaded into [domain.QueryRepository]); see
// [domain.ResolveQueryResourceTypeScope].
func NewResourceQueryService(store domain.Store) *ResourceQueryService {
	return &ResourceQueryService{store: store}
}

// QueryResources returns one page of extension resource query results
// for the given scope. In v0, scope must be [WholePlatformScope]; other
// values return [domain.ErrInvalidArgument].
//
// Type scoping is owned by the query repository via its
// [domain.QuerySchemaProvider]: empty filters are limited to activated
// types, and named resourceType constraints must refer to activated
// types.
func (s *ResourceQueryService) QueryResources(ctx context.Context, in QueryResourcesInput) (domain.QueryResourcesPage, error) {
	if in.Scope != WholePlatformScope {
		return domain.QueryResourcesPage{}, fmt.Errorf(
			"%w: scope %q is not supported; v0 accepts only %q (whole platform)",
			domain.ErrInvalidArgument, in.Scope, WholePlatformScope)
	}

	tx, err := s.store.BeginReadOnly(ctx)
	if err != nil {
		return domain.QueryResourcesPage{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	page, err := tx.Queries().QueryResources(ctx, domain.QueryResourcesRequest{
		Filter:    in.Filter,
		PageSize:  in.PageSize,
		PageToken: in.PageToken,
		OrderBy:   in.OrderBy,
	})
	if err != nil {
		return domain.QueryResourcesPage{}, err
	}
	return page, tx.Commit()
}
