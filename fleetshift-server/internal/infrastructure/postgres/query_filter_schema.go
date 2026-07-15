package postgres

import (
	"fmt"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// validateSpecPath checks names -- the parsed resource.spec.<path>
// segments -- against r's schema provider when a top-level
// resourceType == guard is present and a descriptor is registered
// for that type. Without a guard (or without a descriptor), names are
// returned unchanged for structural JSON extraction: schema validation
// is optional, not a prerequisite for querying spec paths.
//
// When a descriptor is present, segments are matched against canonical
// JSON field names only (see validateDescriptorPath). Without one,
// every segment is preserved exactly -- no case conversion or
// proto-name/JSON-name aliasing.
func (r queryFieldResolver) validateSpecPath(ctx querysql.ResolveContext, names []string) ([]string, error) {
	if r.SchemaProvider == nil || ctx.GuardedResourceType == nil {
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
// resource.observation.<path>. InventoryObservationDescriptor is
// often nil, so this usually returns names unchanged; when a guard
// and descriptor are both present, field names are validated.
func (r queryFieldResolver) validateObservationPath(ctx querysql.ResolveContext, names []string) ([]string, error) {
	if r.SchemaProvider == nil || ctx.GuardedResourceType == nil {
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
	return validateDescriptorPath(schema.InventoryObservationDescriptor, "resource.observation", names)
}

const (
	structFullName protoreflect.FullName = "google.protobuf.Struct"
)

// protoJSONTerminalMessages are well-known types whose ProtoJSON
// encoding is a scalar (or otherwise non-message) value, so filter
// paths must not traverse into their protobuf fields (e.g.
// Timestamp.seconds).
var protoJSONTerminalMessages = map[protoreflect.FullName]bool{
	"google.protobuf.Timestamp":   true,
	"google.protobuf.Duration":    true,
	"google.protobuf.FieldMask":   true,
	"google.protobuf.BoolValue":   true,
	"google.protobuf.BytesValue":  true,
	"google.protobuf.DoubleValue": true,
	"google.protobuf.FloatValue":  true,
	"google.protobuf.Int32Value":  true,
	"google.protobuf.Int64Value":  true,
	"google.protobuf.StringValue": true,
	"google.protobuf.UInt32Value": true,
	"google.protobuf.UInt64Value": true,
	"google.protobuf.Empty":       true,
}

// validateDescriptorPath walks desc through names against the
// canonical ProtoJSON-shaped view of the message:
//
//   - At a message node, a segment must match FieldDescriptor.JSONName
//     exactly (ByJSONName only). The physical JSON key is that same
//     JSON name -- input is never case-converted or aliased to a
//     proto field name.
//   - Crossing a string-keyed map consumes the next segment as an
//     exact literal map key, then resumes message validation when the
//     map value is a message.
//   - google.protobuf.Struct (and other open tails after a Struct
//     field) treat remaining segments as exact literal keys.
//   - Well-known types with special ProtoJSON encodings (Timestamp,
//     Duration, wrappers, …) are terminal: nested field selection is
//     rejected.
//   - Repeated/list traversal and non-string map keys fail closed.
//   - Terminal selection of a repeated or map field is still allowed.
//
// Select and string-index syntax are already flattened to the same
// raw segments by querysql; this function never distinguishes them.
func validateDescriptorPath(desc protoreflect.MessageDescriptor, root string, names []string) ([]string, error) {
	cur := desc
	resolved := make([]string, len(names))
	for i := 0; i < len(names); {
		if protoJSONTerminalMessages[cur.FullName()] {
			return nil, fmt.Errorf("filter: %w: %s is a ProtoJSON scalar (%s), cannot select nested field %q",
				domain.ErrInvalidArgument, joinDotted(root, names[:i]), cur.FullName(), names[i])
		}
		name := names[i]
		fd := cur.Fields().ByJSONName(name)
		if fd == nil {
			return nil, fmt.Errorf("filter: %w: %s has no field %q (message %s)",
				domain.ErrInvalidArgument, joinDotted(root, names[:i]), name, cur.FullName())
		}
		resolved[i] = fd.JSONName()
		if i == len(names)-1 {
			return resolved, nil
		}

		if fd.IsList() {
			return nil, fmt.Errorf("filter: %w: %s is a repeated field, cannot select nested field %q",
				domain.ErrInvalidArgument, joinDotted(root, names[:i+1]), names[i+1])
		}

		if fd.IsMap() {
			if fd.MapKey().Kind() != protoreflect.StringKind {
				return nil, fmt.Errorf("filter: %w: %s has unsupported map key type %s",
					domain.ErrInvalidArgument, joinDotted(root, names[:i+1]), fd.MapKey().Kind())
			}
			i++
			resolved[i] = names[i] // exact literal map key
			if i == len(names)-1 {
				return resolved, nil
			}
			mv := fd.MapValue()
			if mv.Kind() != protoreflect.MessageKind && mv.Kind() != protoreflect.GroupKind {
				return nil, fmt.Errorf("filter: %w: %s map values are not messages, cannot select nested field %q",
					domain.ErrInvalidArgument, joinDotted(root, names[:i]), names[i+1])
			}
			if mv.Message().FullName() == structFullName {
				return appendLiteralTail(resolved, names, i+1), nil
			}
			cur = mv.Message()
			i++
			continue
		}

		if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
			return nil, fmt.Errorf("filter: %w: %s is not a message field, cannot select nested field %q",
				domain.ErrInvalidArgument, joinDotted(root, names[:i+1]), names[i+1])
		}
		if fd.Message().FullName() == structFullName {
			return appendLiteralTail(resolved, names, i+1), nil
		}
		cur = fd.Message()
		i++
	}
	return resolved, nil
}

// appendLiteralTail copies names[from:] into resolved as exact keys
// (open map / Struct tails). resolved must already hold the prefix
// through names[from-1].
func appendLiteralTail(resolved, names []string, from int) []string {
	for j := from; j < len(names); j++ {
		resolved[j] = names[j]
	}
	return resolved
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
