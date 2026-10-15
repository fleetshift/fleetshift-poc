// Package platformresource provides the transport layer for
// platform-canonical resource APIs. Unlike extension APIs (which are
// addon-specific and use addon-chosen service names), the platform API
// uses the fixed service name "resource-manager.open-cluster-management.io", proto package
// "ocm.resource_manager.v1alpha", and platform API version. It only needs a
// [dynamicapi.CollectionConfig] to know the collection shape.
package platformresource

import (
	"github.com/stolostron/ocm-resource-manager/server/internal/transport/dynamicapi"
)

// ProtoPackage is the fixed proto package for all platform-canonical
// resource services.
const ProtoPackage = "ocm.resource_manager.v1alpha"

// APIVersion is the fixed HTTP API version for the platform-canonical
// API surface. Extension schemas version their own APIs independently;
// they do not choose the platform API version.
const APIVersion = "v1alpha"

// ServiceName is the fixed AIP-122 service name for the platform API
// surface.
const ServiceName = "resource-manager.open-cluster-management.io"

// Config describes a platform-canonical resource service. Unlike
// extension [extensionresource.ResourceTypeConfig] (which is
// extension-specific), the platform config uses the fixed service name
// "resource-manager.open-cluster-management.io", proto package "ocm.resource_manager.v1alpha", and platform API
// version [APIVersion]. It only needs the [dynamicapi.CollectionConfig]
// to know the collection shape.
type Config struct {
	dynamicapi.CollectionConfig
}

// GRPCServiceName returns the fully-qualified gRPC service name for
// the platform resource (e.g. "ocm.resource_manager.v1alpha.PlatformClusterService").
func (c *Config) GRPCServiceName() string {
	return ProtoPackage + ".Platform" + c.Singular + "Service"
}

// HTTPPrefix returns the platform-canonical HTTP route prefix
// (e.g. "/apis/resource-manager.open-cluster-management.io/v1alpha/clusters").
func (c *Config) HTTPPrefix() string {
	return "/apis/" + ServiceName + "/" + c.Version + "/" + c.CollectionID
}

// Collection returns the relative resource name collection prefix
// (e.g. "clusters/").
func (c *Config) Collection() string {
	return c.CollectionID + "/"
}
