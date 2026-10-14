// Package scripted implements a test-only delivery add-on whose behavior
// is declared entirely in each resource's own spec. It is registered
// only when --test-features is enabled.
//
// The add-on provides one resource type (ScriptedResource) with one
// delivery target (scripted-local). Tests create resources through the
// normal public API; the spec's behavior section controls
// acknowledgement/completion latency and outcome sequences for both
// delivery and removal, making test scenarios deterministic and
// reproducible without an imperative control API.
package scripted

import (
	_ "embed"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Fixed D31 identifiers. These are locked by the design decision and
// must not change without a reviewed schema version bump.
const (
	// AddonID is the add-on identifier registered with the platform.
	AddonID domain.AddonID = "scripted.fleetshift.io"

	// TargetType is the delivery target type for scripted delivery.
	TargetType domain.TargetType = "scripted"

	// TargetID is the sole static target for local scripted delivery.
	TargetID domain.TargetID = "scripted-local"

	// ResourceType is the managed resource type for scripted resources.
	ResourceType domain.ResourceType = "scripted.fleetshift.io/ScriptedResource"

	// CollectionID is the REST collection name for scripted resources.
	CollectionID = "scriptedResources"

	// ManagedManifestType is the manifest type used for managed
	// scripted resource delivery.
	ManagedManifestType domain.ManifestType = "managed.api.scripted.resource"

	// CapabilityLabel is the environment-manifest capability label
	// advertised when the scripted add-on is active.
	CapabilityLabel = "scripted-delivery"

	// ProtoPackage is the protobuf package name for the spec schema.
	ProtoPackage = "addons.scripted.v1"

	// SpecMessage is the fully qualified protobuf message name for the
	// resource spec.
	SpecMessage = "addons.scripted.v1.ScriptedResourceSpec"
)

const specProtoPath = "addons/scripted/v1/scripted_resource_spec.proto"

//go:embed scripted_resource_spec.proto
var scriptedResourceSpecProto string

// Descriptor returns the add-on descriptor for the scripted delivery
// provider. It declares a delivery capability, a managed resource
// capability, and an inventory capability, all for the single
// ScriptedResource type.
func Descriptor() domain.AddonDescriptor {
	return domain.AddonDescriptor{
		ID:   AddonID,
		Name: "Scripted Delivery Provider",
		Capabilities: []domain.Capability{
			domain.DeliveryCapability{TargetType: TargetType},
			domain.ManagedResourceCapability{ResourceType: ResourceType},
			domain.InventoryResourceCapability{ResourceType: ResourceType},
		},
	}
}

// Schema returns the extension resource schema for the scripted
// resource type. It carries the embedded proto definition and
// fulfillment relation that the platform uses to compile the dynamic
// API surface and route fulfillments to the scripted delivery agent.
func Schema() domain.ExtensionResourceSchema {
	return domain.ExtensionResourceSchema{
		ResourceType: ResourceType,
		ProtoPackage: ProtoPackage,
		Version:      "v1",
		CollectionID: CollectionID,
		Singular:     "ScriptedResource",
		Plural:       "ScriptedResources",
		ProtoFiles: map[string]string{
			specProtoPath: scriptedResourceSpecProto,
		},
		EntryFile: specProtoPath,
		Management: &domain.ManagementSchema{
			SpecMessage: SpecMessage,
			Relation:    domain.NewRegisteredSelfTarget(TargetID, ManagedManifestType),
		},
		Inventory: &domain.InventorySchema{},
	}
}
