package kubernetes

import "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"

// AddonID is the [domain.AddonID] for the Kubernetes addon. It also
// doubles as the service-name prefix every resource type the addon
// owns must start with, per the platform's addon-ownership rule.
const AddonID domain.AddonID = "kubernetes.fleetshift.io"

// Descriptor returns the addon descriptor for the generic Kubernetes
// agent. It declares a delivery capability for Kubernetes
// targets using token-passthrough delivery (no fleetlet), and an
// inventory capability for the generic watched-object resource type.
func Descriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   AddonID,
		Name: "Kubernetes Agent",
		Capabilities: []domain.Capability{
			domain.DeliveryCapability{TargetType: TargetType},
			domain.InventoryResourceCapability{ResourceType: ObjectResourceType},
		},
	}
}

// InventorySchema returns the extension resource schema for the generic
// Kubernetes object inventory type. Every watched Kubernetes object,
// regardless of kind, is reported under this one schema, so it carries
// no management section: there is no per-object spec to fulfill, only
// observed state. That also means it has no proto files to compile --
// inventory-only schemas are registered as type definitions but never
// passed to schema activation, so there is no dynamic API surface that
// would need one.
func InventorySchema() domain.ExtensionResourceSchema {
	return domain.ExtensionResourceSchema{
		ResourceType: ObjectResourceType,
		ProtoPackage: "kubernetes.fleetshift.v1",
		Version:      "v1",
		CollectionID: string(ObjectCollectionID),
		Singular:     "Object",
		Plural:       "Objects",
		Inventory:    &domain.InventorySchema{},
	}
}
