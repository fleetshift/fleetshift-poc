package managedresource

import (
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

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

// ResourceTypeConfig describes an extension service for a platform resource
// type. This is the input to the dynamic service builder — it carries
// everything needed to build and register a typed gRPC + HTTP service at
// runtime without compile-time Go stubs.
//
// Each config describes an extension representation of a platform-level
// resource type. The addon does not define a resource type in isolation;
// it implements a platform resource type through its own typed API. Multiple
// extensions can model the same platform resource type (e.g. one manages
// clusters, another inventories them), each under its own APIServiceName but
// sharing the same [CollectionConfig.CollectionID].
type ResourceTypeConfig struct {
	CollectionConfig

	// ResourceType is the addon-scoped domain identifier used for
	// internal dispatch (e.g. "kind.fleetshift.io/Cluster"). Per
	// AIP-123, this follows the {ServiceName}/{Type} pattern. It
	// identifies the addon's specific implementation of the resource
	// type — it is NOT the platform identity. Two addons modeling
	// the same platform resource type will have different
	// ResourceType values but the same CollectionID.
	ResourceType domain.ResourceType

	// APIServiceName is the versionless AIP-122 service name that
	// differentiates this extension's API surface from other extensions
	// and from the platform's canonical service. It appears in full
	// resource names (e.g. "//kind.fleetshift.io/clusters/foo") and
	// HTTP path prefixes. The addon chooses this; the platform imposes
	// no convention beyond uniqueness.
	APIServiceName string

	// ProtoPackage is the versioned proto package for the generated
	// service (e.g. "kind.fleetshift.v1"). Combined with Singular to
	// form the gRPC service name ({ProtoPackage}.{Singular}Service).
	// Each extension has its own package, avoiding service name
	// collisions even when multiple extensions model the same
	// CollectionID.
	ProtoPackage string

	// SpecMessage is the fully-qualified name of the addon spec message
	// (e.g. "addons.kind.v1.KindClusterSpec").
	SpecMessage protoreflect.FullName

	// SpecDescriptor is the pre-resolved spec message descriptor.
	// If set, SpecMessage is used only for identification; the descriptor
	// is used directly without consulting the global registry.
	SpecDescriptor protoreflect.MessageDescriptor
}

// GRPCServiceName returns the fully-qualified gRPC service name
// (e.g. "kind.fleetshift.v1.ClusterService"). This is extension-specific —
// multiple extensions modeling the same platform resource type each have
// distinct gRPC service names because they use different ProtoPackages.
func (c *ResourceTypeConfig) GRPCServiceName() string {
	return c.ProtoPackage + "." + c.Singular + "Service"
}

// ResourceMessageName returns the resource message name (e.g. "Cluster").
func (c *ResourceTypeConfig) ResourceMessageName() string {
	return c.Singular
}

// CanonicalHTTPPrefix returns the extension-specific HTTP route prefix
// (e.g. "/apis/kind.fleetshift.io/v1/clusters"). The APIServiceName
// segment differentiates this extension's routes from both other
// extensions and the platform's own canonical routes (which would be at
// "/apis/fleetshift.io/v1/clusters" for the same identity domain).
func (c *ResourceTypeConfig) CanonicalHTTPPrefix() string {
	return "/apis/" + c.APIServiceName + "/" + c.Version + "/" + c.CollectionID
}

// Collection returns the relative resource name collection prefix
// (e.g. "clusters/"). This prefix is identity-equivalent across all
// extensions sharing the same CollectionID — "clusters/foo" refers to
// the same platform resource regardless of which extension service
// produced it.
func (c *ResourceTypeConfig) Collection() string {
	return c.CollectionID + "/"
}

// PlatformResourceConfig describes a platform-canonical resource
// service. Unlike [ResourceTypeConfig] (which is extension-specific),
// the platform config uses the fixed service name "fleetshift.io",
// proto package "fleetshift.v1", and platform API version
// [PlatformAPIVersion]. It only needs the [CollectionConfig] to know
// the collection shape.
type PlatformResourceConfig struct {
	CollectionConfig
}

// PlatformProtoPackage is the fixed proto package for all
// platform-canonical resource services.
const PlatformProtoPackage = "fleetshift.v1"

// PlatformAPIVersion is the fixed HTTP API version for the
// platform-canonical API surface. Extension schemas version their own
// APIs independently; they do not choose the platform API version.
const PlatformAPIVersion = "v1"

// PlatformServiceName is the fixed AIP-122 service name for the
// platform API surface.
const PlatformServiceName = "fleetshift.io"

// GRPCServiceName returns the fully-qualified gRPC service name for
// the platform resource (e.g. "fleetshift.v1.PlatformClusterService").
func (c *PlatformResourceConfig) GRPCServiceName() string {
	return PlatformProtoPackage + ".Platform" + c.Singular + "Service"
}

// HTTPPrefix returns the platform-canonical HTTP route prefix
// (e.g. "/apis/fleetshift.io/v1/clusters").
func (c *PlatformResourceConfig) HTTPPrefix() string {
	return "/apis/" + PlatformServiceName + "/" + c.Version + "/" + c.CollectionID
}

// Collection returns the relative resource name collection prefix
// (e.g. "clusters/").
func (c *PlatformResourceConfig) Collection() string {
	return c.CollectionID + "/"
}
