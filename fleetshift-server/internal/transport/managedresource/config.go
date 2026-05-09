package managedresource

import (
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ResourceTypeConfig describes a managed resource type for dynamic service
// registration. This is the input to the dynamic service builder — it
// carries everything needed to build and register a typed gRPC + HTTP
// service at runtime without compile-time Go stubs.
type ResourceTypeConfig struct {
	// ResourceType is the domain identifier (e.g. "clusters").
	ResourceType domain.ResourceType

	// Singular is the singular resource name in PascalCase (e.g. "Cluster",
	// "KindCluster"). Used directly in RPC and message names like
	// Create{Singular}, Get{Singular}Request, etc.
	Singular string

	// Plural is the plural resource name in PascalCase (e.g. "Clusters",
	// "KindClusters"). Used directly in the List RPC and message names
	// (List{Plural}, List{Plural}Request). The lowerCamelCase collection
	// identifier for HTTP paths and proto field names is derived via
	// [CollectionID].
	Plural string

	// ProtoPackage is the proto package for the generated service
	// (e.g. "fleetshift.v1").
	ProtoPackage string

	// SpecMessage is the fully-qualified name of the addon spec message
	// (e.g. "addons.cluster_mgmt.v1.ClusterSpec").
	SpecMessage protoreflect.FullName

	// SpecDescriptor is the pre-resolved spec message descriptor.
	// If set, SpecMessage is used only for identification; the descriptor
	// is used directly without consulting the global registry.
	SpecDescriptor protoreflect.MessageDescriptor
}

// ServiceName returns the gRPC service name (e.g. "fleetshift.v1.ClusterService").
func (c *ResourceTypeConfig) ServiceName() string {
	return string(c.ProtoPackage) + "." + c.Singular + "Service"
}

// ResourceMessageName returns the resource message name (e.g. "Cluster").
func (c *ResourceTypeConfig) ResourceMessageName() string {
	return c.Singular
}

// CollectionID returns the lowerCamelCase collection identifier derived
// from Plural, per AIP-122 (e.g. "KindClusters" -> "kindClusters").
// Used for HTTP path segments, proto field names, and resource name
// prefixes.
func (c *ResourceTypeConfig) CollectionID() string {
	return strings.ToLower(c.Plural[:1]) + c.Plural[1:]
}

// Collection returns the resource name collection prefix
// (e.g. "kindClusters/").
func (c *ResourceTypeConfig) Collection() string {
	return c.CollectionID() + "/"
}
