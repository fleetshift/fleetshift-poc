package managedresource

import (
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ResourceTypeConfig describes a managed resource type for dynamic service
// registration. This is the input to the dynamic service builder — it
// carries everything needed to build and register a typed gRPC + HTTP
// service at runtime without compile-time Go stubs.
type ResourceTypeConfig struct {
	// ResourceType is the domain identifier (e.g. "api.kind.cluster").
	ResourceType domain.ResourceType

	// APIServiceName is the versionless AIP service name used in full
	// resource names and HTTP prefixes (e.g. "kind.fleetshift.io").
	APIServiceName string

	// Version is the HTTP API version segment (e.g. "v1").
	Version string

	// CollectionID is the AIP collection identifier used in resource
	// names, HTTP paths, and proto field names (e.g. "clusters").
	CollectionID string

	// Singular is the PascalCase singular resource name used in RPC
	// and message names like Create{Singular}, Get{Singular}Request
	// (e.g. "Cluster").
	Singular string

	// Plural is the PascalCase plural resource name used in List RPC
	// and message names (e.g. "Clusters").
	Plural string

	// ProtoPackage is the versioned proto package for the generated
	// service (e.g. "kind.fleetshift.v1").
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
// (e.g. "kind.fleetshift.v1.ClusterService").
func (c *ResourceTypeConfig) GRPCServiceName() string {
	return c.ProtoPackage + "." + c.Singular + "Service"
}

// ResourceMessageName returns the resource message name (e.g. "Cluster").
func (c *ResourceTypeConfig) ResourceMessageName() string {
	return c.Singular
}

// CanonicalHTTPPrefix returns the canonical HTTP route prefix
// (e.g. "/apis/kind.fleetshift.io/v1/clusters").
func (c *ResourceTypeConfig) CanonicalHTTPPrefix() string {
	return "/apis/" + c.APIServiceName + "/" + c.Version + "/" + c.CollectionID
}

// Collection returns the resource name collection prefix
// (e.g. "clusters/").
func (c *ResourceTypeConfig) Collection() string {
	return c.CollectionID + "/"
}
