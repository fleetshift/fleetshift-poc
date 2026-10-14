// Package dynamicapi provides shared infrastructure for building and
// registering dynamic gRPC + HTTP services at runtime. It contains the
// dynamic service mux, file registry, compiler, reflection helpers, and
// proto field/HTTP utilities used by both the extension (managed resource)
// and platform resource transport packages.
package dynamicapi

// CollectionConfig describes the identity and naming of a resource
// collection. This is the shared vocabulary between extension and
// platform APIs that participate in the same identity domain — the
// CollectionID binds them to the same platform resource type.
//
// The relative resource name ({CollectionID}/{id}, e.g. "clusters/foo") is
// identity-equivalent across all APIs that share a CollectionID. This is
// how extension resources unify under a single platform identity.
//
// See docs/design/architecture/resource_identity_and_api.md for the full
// two-layer API model and identity semantics.
//
// # Current scope
//
// The resource hierarchy is currently flat: resource names are
// {CollectionID}/{leaf_id} with no parent segments. Workspace and tenant
// scoping will introduce parent collections in the future.
type CollectionConfig struct {
	// Version is the HTTP API version segment (e.g. "v1").
	Version string

	// CollectionID is the AIP collection identifier that establishes
	// platform identity domain membership (e.g. "clusters").
	CollectionID string

	// Singular is the PascalCase singular resource name used in RPC
	// and message names (e.g. "Cluster").
	Singular string

	// Plural is the PascalCase plural resource name used in List RPC
	// and message names (e.g. "Clusters").
	Plural string
}
