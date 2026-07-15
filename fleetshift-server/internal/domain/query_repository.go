package domain

import "context"

// QueryResourceKind discriminates the two resource surfaces a
// [QueryRepository] result row can come from.
//
// The current Postgres and SQLite implementations return only
// extension rows ([QueryResourceKindExtension]).
// [QueryResourceKindPlatform] is kept for a future platform-aggregate
// query implementation and is not emitted today.
type QueryResourceKind string

const (
	// QueryResourceKindPlatform marks a result row backed by
	// [ResourceIdentityRepository]'s platform resource read model
	// (physical or virtual -- see [PlatformResource]'s doc).
	//
	// Reserved for a future platform-aggregate query implementation;
	// the current QueryRepository does not emit platform rows.
	QueryResourceKindPlatform QueryResourceKind = "platform"
	// QueryResourceKindExtension marks a result row backed by
	// [ExtensionResourceRepository.GetView]'s extension resource
	// read model.
	QueryResourceKindExtension QueryResourceKind = "extension"
)

// QueryResourcesRequest is the input to [QueryRepository.QueryResources].
type QueryResourcesRequest struct {
	// Filter is a CEL expression evaluated against the query result
	// envelope (see each backend's field resolver for the supported
	// field set). Empty matches every activated extension type when
	// the repository's [QuerySchemaProvider] is set; with a nil
	// provider there is no activation scope.
	//
	// Supported CEL fields are envelope name, envelope resourceType,
	// and fields under resource by their canonical ProtoJSON names
	// (labels, intentVersion, pauseReason, localLabels, conditions,
	// observation, localUpdateTime, indexUpdateTime, generation,
	// state, and guarded spec/observation paths). Top-level identity
	// components (service_name, collection_name, resource_id, and
	// similar) are not filter fields; use name / resourceType instead.
	// Other resource.* paths are rejected as unsupported. Map keys are
	// exact and case-sensitive; message fields have no proto-name
	// aliases.
	//
	// Filter matching is case-sensitive over the ProtoJSON response
	// spelling (names, labels, resource.state enum names such as
	// "ACTIVE"). ProtoJSON int64 fields (intentVersion, generation)
	// compare as decimal strings. JSON extracts preserve JSON value
	// types — a JSON string never matches a numeric/boolean literal,
	// and incompatible != is true for present values (missing paths
	// remain non-matches). Known string fields follow the same
	// heterogeneous equality rules. Timestamp fields are ProtoJSON
	// strings: direct comparison uses the canonical ProtoJSON
	// spelling, while chronological / instant comparisons use
	// timestamp() on both sides (for example
	// timestamp(resource.localUpdateTime) <
	// timestamp("2026-06-01T12:00:00Z")). timestamp() may be applied
	// to any string-valued path.
	//
	// When a [QuerySchemaProvider] is configured, results are limited
	// to types it lists (see [ResolveQueryResourceTypeScope]). Named
	// top-level resourceType == / in constraints must refer to
	// activated types or the call fails with [ErrInvalidArgument].
	Filter string

	// PageSize caps the number of rows returned. Non-positive values
	// fall back to the repository's default page size; oversized
	// values are clamped to the repository's max.
	PageSize int32

	// PageToken resumes a previous QueryResources call. Empty starts
	// from the first page.
	PageToken string

	// OrderBy selects a supported deterministic ordering. Leave empty
	// for the default order (collection_name, resource_id,
	// service_name, type_name). The only other supported value is
	// "resource_type,name". Arbitrary expressions return
	// [ErrInvalidArgument].
	OrderBy string
}

// QueryResourcesPage is one page of [QueryResourceResult]s, in the
// order the repository applied them.
type QueryResourcesPage struct {
	Resources []QueryResourceResult
	// NextPageToken is non-empty when more rows exist past this page.
	NextPageToken string
}

// QueryResourceResult is one row of a [QueryRepository.QueryResources]
// result. The current implementation returns only extension resource
// read models; platform aggregate rows are reserved for a future
// implementation. See [QueryResourceResult.Extension].
type QueryResourceResult struct {
	// Kind discriminates which of Platform/Extension is populated.
	// It is implementation metadata for callers that find the
	// discriminator convenient; it is not part of the public
	// QueryResources response shape, and CEL filters must not select
	// on it. Prefer resourceType for type selection. The current
	// implementation always sets Kind to [QueryResourceKindExtension].
	Kind QueryResourceKind

	// Name is the envelope name used by CEL filters: the canonical
	// full resource name "//{service_name}/{collection_name}/{resource_id}".
	Name string

	// ResourceType is the stable type identity used by CEL filters:
	// "{service_name}/{type_name}".
	ResourceType ResourceType

	// ServiceName, CollectionName, and ResourceID are implementation
	// metadata populated for ordering, page tokens, and callers that
	// already consume the current DTO. They are not public
	// QueryResources envelope CEL fields.
	ServiceName ServiceName
	// APIVersion is retained on the DTO for compatibility but is not
	// populated by the current extension-only implementations
	// (QueryResources no longer joins extension_resource_types for
	// the page window). Callers that need the type's API version
	// should read it from the type catalog / schema provider.
	APIVersion     APIVersion
	CollectionName CollectionName
	ResourceID     ResourceID

	// Platform is always nil in the current implementation. Kept for
	// a future platform-aggregate query surface.
	Platform *PlatformResource
	// Extension is always populated in the current implementation.
	// Its shape matches [ExtensionResourceRepository.GetView].
	Extension *ExtensionResourceView
}

// QueryRepository is a read model repository over extension resources.
// Unlike [ExtensionResourceRepository], it is not an aggregate
// repository: it owns no aggregate of its own, never mutates state,
// and has no Create/Update/Delete. QueryResources projects existing
// extension aggregate state -- the same rows
// [ExtensionResourceRepository.GetView] already exposes -- into a
// filterable, paginated result set.
//
// Platform aggregate search is intentionally not implemented yet;
// restoring it via the previous platform_rows CTE union is deferred
// until the platform aggregate model is settled.
type QueryRepository interface {
	// QueryResources returns one page of extension resource read
	// models matching req.Filter, applying keyset pagination via
	// req.PageToken/req.PageSize. Implementations execute one data
	// query per page; they do not hydrate results with a per-row
	// follow-up read.
	QueryResources(ctx context.Context, req QueryResourcesRequest) (QueryResourcesPage, error)
}
