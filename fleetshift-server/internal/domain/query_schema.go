package domain

// External import in domain justified: the domain IS by definition
// coupled to protobuf (see addon.go's ManagementSchema for the
// existing precedent).
import (
	"context"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// QuerySchemaProvider gives [QueryRepository] implementations
// optional protobuf schema knowledge for type-specific CEL field
// validation -- see the QueryRepository POC plan's "Schema Provider"
// section and querysql's field resolver (the only current caller).
//
// It is intentionally narrow and transport-agnostic: it knows nothing
// about internal/transport's dynamic gRPC/HTTP registration, only the
// descriptors that registration already produces. A concrete
// implementation that *does* know how to populate it from schema
// activation lives in a higher layer -- see
// internal/application's QuerySchemaCatalog, wired beside
// managedresource.DynamicSchemaActivator -- so this interface itself
// stays free of that dependency direction.
//
// Absence of a schema is an explicit, first-class outcome (the bool
// return), not an error. The POC's field resolver treats "no schema
// registered for this type" and "schema registered but its
// descriptor is nil" both as "validate this path structurally only"
// (any well-formed dotted JSON path is accepted), rather than failing
// closed -- most resource types have no descriptor activated through
// this path yet (see the plan's "Important gap" on inventory-only
// activation). Only a resolvable schema *with* a descriptor triggers
// field-name validation.
//
// The plan's interface sketch also included platform resource schema
// lookup (GetPlatformQuerySchema); it is omitted here because
// platform resources have no schema story yet in this POC at all
// (see the plan's "Open Questions" on platform resource_type) --
// adding an always-empty method would be speculative surface with no
// caller.
type QuerySchemaProvider interface {
	// GetResourceQuerySchema returns rt's query schema, if known.
	GetResourceQuerySchema(ctx context.Context, rt ResourceType) (ResourceQuerySchema, bool, error)

	// ListResourceQuerySchemas returns every currently known resource
	// query schema. The POC's compiler does not call this (it only
	// ever looks up one type at a time, guarded by a resource_type
	// filter conjunct); kept for parity with the plan's sketch and for
	// future admin/debug surfaces.
	ListResourceQuerySchemas(ctx context.Context) ([]ResourceQuerySchema, error)
}

// ResourceQuerySchema is what a [QuerySchemaProvider] knows about one
// extension resource type's shape, for query-time field validation.
type ResourceQuerySchema struct {
	ResourceType   ResourceType
	ServiceName    ServiceName
	TypeName       string
	APIVersion     APIVersion
	CollectionName CollectionName

	// SpecDescriptor describes the managed resource's intent spec
	// message, when the type is managed and its schema has been
	// activated (see managedresource.DynamicSchemaActivator.Activate,
	// which already compiles this exact descriptor). Query-time field
	// resolution walks it to validate resource.spec.<path> segments;
	// nil means "validate this type's spec paths structurally only".
	SpecDescriptor protoreflect.MessageDescriptor

	// InventoryObservationDescriptor is reserved for future inventory
	// schema activation -- DynamicSchemaActivator does not yet support
	// activating inventory-only capabilities (see the plan's
	// "Important gap" section) -- so this is always nil today.
	InventoryObservationDescriptor protoreflect.MessageDescriptor
}
