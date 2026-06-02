package domain

import (
	"time"

	// External import in domain justified: the domain IS by definition coupled to protobuf.
	"google.golang.org/protobuf/reflect/protoreflect"
)

// AddonID uniquely identifies an addon within the platform.
type AddonID string

// AddonState represents the lifecycle phase of an addon.
type AddonState int

const (
	// AddonStateDefined means the addon descriptor has been loaded into
	// the catalog but no authorization or trust configuration exists yet.
	AddonStateDefined AddonState = iota

	// AddonStateEnabled means an admin has authorized the addon and
	// configured its trust policy. Capability expectations are recorded
	// but no schemas have been provided and no runtime is active.
	AddonStateEnabled

	// AddonStateConnected means a workload has connected, provided
	// schemas, and activated runtime capabilities (delivery agents, etc.).
	AddonStateConnected
)

// Addon is the persisted record for an addon. It doubles as the trust
// policy entry once signing is implemented.
type Addon struct {
	ID           AddonID
	Name         string
	State        AddonState
	Capabilities []Capability
	EnabledAt    time.Time
	ConnectedAt  *time.Time
}

// AddonDescriptor is the value object loaded from the catalog. It
// describes what the addon ships and what capabilities it declares.
// Descriptors are lightweight — they carry authorization-relevant
// metadata, not schemas.
type AddonDescriptor struct {
	ID           AddonID
	Name         string
	Capabilities []Capability
}

// Capability is a sealed interface for extension point declarations.
// Each variant corresponds to one extension point type.
type Capability interface {
	addonCapability()

	// CapabilityType returns a human-readable discriminator for logging
	// and error messages.
	CapabilityType() string
}

// ManagedResourceCapability declares that the addon will provide a
// managed resource type. The full schema and fulfillment relation come
// from the workload at connect time via [ManagedResourceSchema].
type ManagedResourceCapability struct {
	ResourceType ResourceType
}

func (ManagedResourceCapability) addonCapability()       {}
func (ManagedResourceCapability) CapabilityType() string { return "managed_resource" }

// DeliveryCapability declares that the addon provides a
// [DeliveryAgent] for the given target type.
type DeliveryCapability struct {
	TargetType TargetType
}

func (DeliveryCapability) addonCapability()       {}
func (DeliveryCapability) CapabilityType() string { return "delivery" }

// ManagedResourceSchema is provided by the workload at connect time.
// It carries the full schema and fulfillment relation that the platform
// validates against the declared [ManagedResourceCapability].
//
// Proto definitions are provided inline as content, not as file paths.
// This parallels how an application carries its DB migration SQL — the
// workload owns and transmits its schema. The compiler combines inline
// sources with a built-in resolver for well-known imports.
type ManagedResourceSchema struct {
	ResourceType ResourceType
	// Singular is the singular resource name in PascalCase (e.g. "KindCluster").
	Singular string

	// Plural is the plural resource name in PascalCase (e.g. "KindClusters").
	// The lowerCamelCase collection identifier for HTTP paths and proto field
	// names (e.g. "kindClusters") is derived automatically by the transport
	// layer via [managedresource.ResourceTypeConfig.CollectionID].
	Plural string

	// ProtoFiles maps virtual filenames to proto source content.
	// The compiler resolves imports within this map first, then
	// falls back to well-known types (google/protobuf/*, buf.validate/*).
	ProtoFiles map[string]string

	// EntryFile is the proto file the compiler starts from. Required
	// for multi-file schemas; for single-file schemas the compiler
	// infers it automatically.
	EntryFile string

	SpecMessage protoreflect.FullName

	Relation FulfillmentRelation
}
