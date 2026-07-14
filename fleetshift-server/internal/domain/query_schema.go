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
// validation. The postgres/sqlite query field resolvers are the
// current callers (they implement infrastructure/querysql.FieldResolver).
//
// It is intentionally narrow and transport-agnostic: it knows nothing
// about internal/transport's dynamic gRPC/HTTP registration, only the
// descriptors that registration already produces. A concrete
// implementation that *does* know how to populate it from schema
// activation lives in a higher layer -- see
// transport/extensionresource's ActiveResourceRegistry, which is also the
// activator's state store -- so this interface itself
// stays free of that dependency direction.
//
// Absence of a schema is an explicit, first-class outcome (the bool
// return), not an error. Field resolvers treat "no schema registered
// for this type" and "schema registered but its descriptor is nil"
// both as "validate this path structurally only" (any well-formed
// path is accepted with exact segment keys preserved — no case
// conversion), rather than failing closed -- types without a
// SpecDescriptor (including inventory-only activations) validate
// structurally only. Only a resolvable schema *with* a descriptor
// triggers JSON-name field validation (ByJSONName only; no
// proto-name aliases).
//
// Platform resource schema lookup is omitted: platform resources have
// no query schema story yet, and QueryResources does not emit
// platform rows. Adding an always-empty method would be speculative
// surface with no caller.
type QuerySchemaProvider interface {
	// GetResourceQuerySchema returns rt's query schema, if known.
	GetResourceQuerySchema(ctx context.Context, rt ResourceType) (ResourceQuerySchema, bool, error)

	// ListResourceQuerySchemas returns every currently known resource
	// query schema. [QueryRepository] uses this to scope QueryResources
	// to activated types (see [ResolveQueryResourceTypeScope]). The
	// query compiler still looks up one type at a time via
	// GetResourceQuerySchema when validating guarded
	// resource.spec.*/resource.observation.* paths.
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
	// activated (see extensionresource.DynamicSchemaActivator.Activate,
	// which already compiles this exact descriptor). Query-time field
	// resolution walks it to validate resource.spec.<path> segments
	// against canonical ProtoJSON field names (FieldDescriptor.JSONName);
	// nil means "validate this type's spec paths structurally only"
	// with exact keys preserved.
	SpecDescriptor protoreflect.MessageDescriptor

	// InventoryObservationDescriptor describes a typed observation
	// message when InventorySchema carries one. Nil while observation
	// is google.protobuf.Struct MVP (including inventory-only
	// activations that still register query schemas).
	InventoryObservationDescriptor protoreflect.MessageDescriptor
}
