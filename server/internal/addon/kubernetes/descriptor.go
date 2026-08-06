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
// regardless of kind, is reported under this one schema. It has no
// management section (no per-object spec to fulfill, only observed
// state) and no ProtoFiles to compile. Connect still registers a type
// definition with Inventory set and Management nil, and activates the
// schema so QueryResources can include the type in its activation scope.
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
