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
// from the workload at connect time via [ExtensionResourceSchema].
type ManagedResourceCapability struct {
	ResourceType ResourceType
}

func (ManagedResourceCapability) addonCapability()       {}
func (ManagedResourceCapability) CapabilityType() string { return "managed_resource" }

// InventoryResourceCapability declares that the addon will report
// inventory for the given resource type. The addon provides an
// [InventorySchema] section inside its [ExtensionResourceSchema] at
// connect time.
type InventoryResourceCapability struct {
	ResourceType ResourceType
}

func (InventoryResourceCapability) addonCapability()       {}
func (InventoryResourceCapability) CapabilityType() string { return "inventory_resource" }

// DeliveryCapability declares that the addon provides a
// [DeliveryAgent] for the given target type.
type DeliveryCapability struct {
	TargetType TargetType
}

func (DeliveryCapability) addonCapability()       {}
func (DeliveryCapability) CapabilityType() string { return "delivery" }

// ExtensionResourceSchema is provided by the workload at connect time.
// It carries the shared resource identity fields and optional
// capability-specific sections ([ManagementSchema], [InventorySchema]).
// The platform validates each section against the addon's declared
// capabilities: Management requires a [ManagedResourceCapability],
// Inventory requires an [InventoryResourceCapability].
//
// Proto definitions are provided inline as content, not as file paths.
// This parallels how an application carries its DB migration SQL — the
// workload owns and transmits its schema. The compiler combines inline
// sources with a built-in resolver for well-known imports.
type ExtensionResourceSchema struct {
	ResourceType ResourceType

	// ProtoPackage is the versioned proto package used by gRPC
	// (e.g. "kind.fleetshift.v1").
	ProtoPackage string

	// Version is the extension HTTP API version segment (e.g. "v1").
	Version string

	// CollectionID is the AIP collection identifier used in resource
	// names and HTTP paths (e.g. "clusters").
	CollectionID string

	// Singular is the PascalCase singular resource name used in RPC and
	// message names (e.g. "Cluster").
	Singular string

	// Plural is the PascalCase plural resource name used in List RPC
	// and response message names (e.g. "Clusters").
	Plural string

	// ProtoFiles maps virtual filenames to proto source content.
	// The compiler resolves imports within this map first, then
	// falls back to well-known types (google/protobuf/*, buf.validate/*).
	ProtoFiles map[string]string

	// EntryFile is the proto file the compiler starts from. Required
	// for multi-file schemas; for single-file schemas the compiler
	// infers it automatically.
	EntryFile string

	// Management carries the management-specific fields. When non-nil,
	// the addon must declare a [ManagedResourceCapability] for this
	// resource type.
	Management *ManagementSchema

	// Inventory carries inventory-specific fields. When non-nil, the
	// addon must declare an [InventoryResourceCapability] for this
	// resource type.
	Inventory *InventorySchema
}

// ManagementSchema carries the management-specific fields extracted
// from the schema. These are only relevant for managed resource types.
type ManagementSchema struct {
	SpecMessage protoreflect.FullName
	Relation    FulfillmentRelation
}

// InventorySchema is a capability marker for inventory-reportable
// extension resource types. It carries no fields today but exists as a
// struct to support future inventory-specific schema configuration.
type InventorySchema struct{}
