package domain

import "context"

// QueryResourceKind discriminates the two resource surfaces a
// [QueryRepository] result row can come from.
type QueryResourceKind string

const (
	// QueryResourceKindPlatform marks a result row backed by
	// [ResourceIdentityRepository]'s platform resource read model
	// (physical or virtual -- see [PlatformResource]'s doc).
	QueryResourceKindPlatform QueryResourceKind = "platform"
	// QueryResourceKindExtension marks a result row backed by
	// [ExtensionResourceRepository.GetView]'s extension resource
	// read model.
	QueryResourceKindExtension QueryResourceKind = "extension"
)

// QueryResourcesRequest is the input to [QueryRepository.QueryResources].
type QueryResourcesRequest struct {
	// Filter is a CEL expression evaluated against the query result
	// envelope (see the Postgres implementation's field resolver for
	// the supported field set). Empty matches every row.
	Filter string

	// PageSize caps the number of rows returned. Non-positive values
	// fall back to the repository's default page size; oversized
	// values are clamped to the repository's max.
	PageSize int32

	// PageToken resumes a previous QueryResources call. Empty starts
	// from the first page.
	PageToken string

	// OrderBy is reserved for future sortable-field support. Leave
	// empty for the default deterministic ordering. For the POC, a
	// non-empty value returns [ErrUnimplemented].
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
// result: either a platform resource or an extension resource read
// model, never both. See [QueryResourceResult.Platform] and
// [QueryResourceResult.Extension].
type QueryResourceResult struct {
	// Kind is just a discriminator for which of Platform/Extension is
	// populated, if a caller finds that convenient. It is not itself
	// a stable resource identity; CEL filters that need to select a
	// resource kind or type should prefer resource_type or
	// service_name.
	Kind QueryResourceKind

	// Name is the envelope name used by CEL filters. Platform:
	// "//fleetshift.io/{resource_name}". Extension:
	// "//{service_name}/{resource_name}".
	Name string

	// ResourceType is the stable type identity used by CEL filters.
	// For extension resources this is "{service_name}/{type_name}".
	// Platform resources leave this empty in the first POC -- there
	// is no platform schema metadata yet to derive a stable platform
	// type identity from (see
	// docs/design/architecture/resource_indexing.md's open
	// questions); CollectionName is available in the meantime.
	ResourceType ResourceType

	ServiceName    ServiceName
	APIVersion     APIVersion
	CollectionName CollectionName
	ResourceID     ResourceID

	// Platform is populated for platform rows (Kind ==
	// [QueryResourceKindPlatform]); nil for extension rows. Its shape
	// matches [ResourceIdentityRepository.GetByName].
	Platform *PlatformResource
	// Extension is populated for extension rows (Kind ==
	// [QueryResourceKindExtension]); nil for platform rows. Its shape
	// matches [ExtensionResourceRepository.GetView].
	Extension *ExtensionResourceView
}

// QueryRepository is a read model repository over the platform and
// extension resource surfaces. Unlike [ExtensionResourceRepository]
// and [ResourceIdentityRepository], it is not an aggregate
// repository: it owns no aggregate of its own, never mutates state,
// and has no Create/Update/Delete. QueryResources projects existing
// aggregate state -- the same rows [ExtensionResourceRepository.GetView]
// and [ResourceIdentityRepository.GetByName] already expose -- into a
// single filterable, paginated result set spanning both resource
// surfaces. See this repository's implementation(s) for the supported
// CEL filter subset.
type QueryRepository interface {
	// QueryResources returns one page of platform and/or extension
	// resource read models matching req.Filter, applying keyset
	// pagination via req.PageToken/req.PageSize. Implementations
	// execute one data query per page; they do not hydrate results
	// with a per-row follow-up read.
	QueryResources(ctx context.Context, req QueryResourcesRequest) (QueryResourcesPage, error)
}
