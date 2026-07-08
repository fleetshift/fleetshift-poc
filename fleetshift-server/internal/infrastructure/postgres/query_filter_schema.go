package postgres

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres/querysql"
)

// validateSpecPath checks names -- the parsed resource.spec.<path>
// segments -- against r's schema provider, when one is configured and
// has a descriptor registered for ctx's guarded resource type, and
// returns the path to actually use for JSON key extraction (see
// jsonTextField). Callers must only reach this once
// ctx.GuardedResourceType is known to be non-nil. See
// [domain.QuerySchemaProvider]'s doc for why "no provider", "no
// schema for this type", and "schema with no descriptor" all fall
// back to returning names unchanged (structural validation only)
// rather than an error: most resource types have no descriptor
// activated through this path yet.
func (r queryFieldResolver) validateSpecPath(ctx querysql.ResolveContext, names []string) ([]string, error) {
	if r.SchemaProvider == nil {
		return names, nil
	}
	rt := *ctx.GuardedResourceType
	schema, ok, err := r.SchemaProvider.GetResourceQuerySchema(ctx.Context, rt)
	if err != nil {
		return nil, fmt.Errorf("filter: look up query schema for %q: %w", rt, err)
	}
	if !ok || schema.SpecDescriptor == nil {
		return names, nil
	}
	return validateDescriptorPath(schema.SpecDescriptor, "resource.spec", names)
}

// validateObservationPath is validateSpecPath's counterpart for
// resource.inventory.observation.<path>. InventoryObservationDescriptor
// is always nil today (see its doc), so this always returns names
// unchanged in practice; it exists so activating inventory schemas
// later does not require touching the resolver.
func (r queryFieldResolver) validateObservationPath(ctx querysql.ResolveContext, names []string) ([]string, error) {
	if r.SchemaProvider == nil {
		return names, nil
	}
	rt := *ctx.GuardedResourceType
	schema, ok, err := r.SchemaProvider.GetResourceQuerySchema(ctx.Context, rt)
	if err != nil {
		return nil, fmt.Errorf("filter: look up query schema for %q: %w", rt, err)
	}
	if !ok || schema.InventoryObservationDescriptor == nil {
		return names, nil
	}
	return validateDescriptorPath(schema.InventoryObservationDescriptor, "resource.inventory.observation", names)
}

// validateDescriptorPath walks desc field-by-field through names,
// descending into nested message fields for all but the last segment,
// and returns the path rewritten to each field's JSON name. It
// matches a segment against both the proto field name and its JSON
// name, since CEL filter authors are more likely to know a spec field
// by its JSON/camelCase name than its proto_name -- but the stored
// spec JSON itself (see registrar.go's protojson.Marshal, which uses
// protojson's default MarshalOptions) always keys on the JSON name.
// If jsonTextField used the segment text the filter author actually
// typed, a proto_name match here (e.g. resource.spec.api_server_port)
// would validate successfully but then extract the wrong JSON key
// (api_server_port instead of the stored apiServerPort), silently
// matching nothing. Returning the resolved JSON names lets the two
// spellings behave identically.
func validateDescriptorPath(desc protoreflect.MessageDescriptor, root string, names []string) ([]string, error) {
	cur := desc
	resolved := make([]string, len(names))
	for i, name := range names {
		fd := cur.Fields().ByName(protoreflect.Name(name))
		if fd == nil {
			fd = cur.Fields().ByJSONName(name)
		}
		if fd == nil {
			return nil, fmt.Errorf("filter: %w: %s has no field %q (message %s)",
				domain.ErrInvalidArgument, joinDotted(root, names[:i]), name, cur.FullName())
		}
		resolved[i] = fd.JSONName()
		if i == len(names)-1 {
			return resolved, nil
		}
		if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
			return nil, fmt.Errorf("filter: %w: %s is not a message field, cannot select nested field %q",
				domain.ErrInvalidArgument, joinDotted(root, names[:i+1]), names[i+1])
		}
		cur = fd.Message()
	}
	return resolved, nil
}

// joinDotted joins root with names using ".", skipping the separator
// when names is empty.
func joinDotted(root string, names []string) string {
	s := root
	for _, n := range names {
		s += "." + n
	}
	return s
}
