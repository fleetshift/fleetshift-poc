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
	// ResourceType is the domain identifier (e.g. "clusters").
	ResourceType domain.ResourceType

	// Singular is the singular resource name in PascalCase (e.g. "Cluster").
	Singular string

	// Plural is the plural collection name (e.g. "clusters").
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

// Collection returns the URL path collection prefix (e.g. "clusters/").
func (c *ResourceTypeConfig) Collection() string {
	return c.Plural + "/"
}
