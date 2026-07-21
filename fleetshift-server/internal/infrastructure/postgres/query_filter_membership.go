package postgres

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// ResolveMembership implements [querysql.FieldResolver].
func (r queryFieldResolver) ResolveMembership(path querysql.FieldPath, key string, kind querysql.ContainerKind, ctx querysql.ResolveContext) (string, error) {
	if kind == querysql.ContainerKindScalar {
		return "", fmt.Errorf("filter: %w: membership requires a container field", domain.ErrInvalidArgument)
	}
	segs := path.Segments
	if len(segs) == 0 {
		return "", fmt.Errorf("filter: %w: empty field path", domain.ErrInvalidArgument)
	}

	// "spec" in resource → has(resource.spec), including proto3 default omission.
	if len(segs) == 1 && segs[0] == "resource" {
		return r.ResolvePresence(querysql.FieldPath{Segments: []string{"resource", key}}, ctx)
	}
	if len(segs) == 1 {
		return "", fmt.Errorf("filter: %w: membership requires a container field", domain.ErrInvalidArgument)
	}
	if segs[0] != "resource" {
		return "", fmt.Errorf("filter: %w: unsupported field expression", domain.ErrInvalidArgument)
	}

	effective := kind
	if effective == querysql.ContainerKindUnknown {
		var err error
		effective, err = r.containerKindOf(segs[1:], ctx)
		if err != nil {
			return "", err
		}
	}
	if effective == querysql.ContainerKindScalar {
		return "", fmt.Errorf("filter: %w: membership requires a container field", domain.ErrInvalidArgument)
	}

	// Object-key membership agrees with ResolvePresence(path+key) for
	// known object/map paths and for forced Object kind.
	if effective == querysql.ContainerKindObject {
		full := append(append([]string(nil), segs...), key)
		return r.ResolvePresence(querysql.FieldPath{Segments: full}, ctx)
	}

	container, err := r.jsonbContainerExpr(segs[1:], ctx)
	if err != nil {
		return "", err
	}
	keyPh := ctx.Bind(key)

	switch effective {
	case querysql.ContainerKindList:
		return jsonbListStringMembership(container, keyPh), nil
	case querysql.ContainerKindUnknown:
		return jsonbDynamicMembership(container, keyPh), nil
	default:
		return "", fmt.Errorf("filter: %w: unsupported container kind", domain.ErrInvalidArgument)
	}
}

// containerKindOf classifies resource.* for membership. Known maps and
// condition objects are Object; known scalars are Scalar; open JSON
// under spec/observation is Unknown unless a schema descriptor proves
// object/map or list.
func (r queryFieldResolver) containerKindOf(resourceSegs []string, ctx querysql.ResolveContext) (querysql.ContainerKind, error) {
	if len(resourceSegs) == 0 {
		return querysql.ContainerKindUnknown, fmt.Errorf("filter: %w: unsupported field \"resource\"", domain.ErrInvalidArgument)
	}
	head, rest := resourceSegs[0], resourceSegs[1:]
	switch head {
	case "labels", "localLabels":
		if len(rest) == 0 {
			return querysql.ContainerKindObject, nil
		}
		// Map values are strings (scalars), not containers.
		return querysql.ContainerKindScalar, nil
	case "conditions":
		switch len(rest) {
		case 0:
			return querysql.ContainerKindObject, nil
		case 1:
			// Condition entry is a JSON object.
			return querysql.ContainerKindObject, nil
		default:
			return querysql.ContainerKindScalar, nil
		}
	case "spec":
		if len(rest) == 0 {
			return querysql.ContainerKindObject, nil
		}
		return r.schemaContainerKind(ctx, true, rest)
	case "observation":
		if len(rest) == 0 {
			return querysql.ContainerKindObject, nil
		}
		return r.schemaContainerKind(ctx, false, rest)
	case "name", "uid", "intentVersion", "state", "pauseReason", "generation",
		"localUpdateTime", "indexUpdateTime":
		if len(rest) == 0 {
			return querysql.ContainerKindScalar, nil
		}
	}
	return querysql.ContainerKindUnknown, fmt.Errorf("filter: %w: unsupported field \"resource.%s\"", domain.ErrInvalidArgument, strings.Join(resourceSegs, "."))
}

func (r queryFieldResolver) schemaContainerKind(ctx querysql.ResolveContext, isSpec bool, names []string) (querysql.ContainerKind, error) {
	var (
		validated []string
		err       error
		desc      protoreflect.MessageDescriptor
	)
	if isSpec {
		validated, err = r.validateSpecPath(ctx, names)
	} else {
		validated, err = r.validateObservationPath(ctx, names)
	}
	if err != nil {
		return querysql.ContainerKindUnknown, err
	}
	if r.SchemaProvider == nil || ctx.GuardedResourceType == nil {
		return querysql.ContainerKindUnknown, nil
	}
	schema, ok, err := r.SchemaProvider.GetResourceQuerySchema(ctx.Context, *ctx.GuardedResourceType)
	if err != nil {
		return querysql.ContainerKindUnknown, err
	}
	if !ok {
		return querysql.ContainerKindUnknown, nil
	}
	if isSpec {
		desc = schema.SpecDescriptor
	} else {
		desc = schema.InventoryObservationDescriptor
	}
	if desc == nil {
		return querysql.ContainerKindUnknown, nil
	}
	return descriptorContainerKind(desc, validated)
}

// descriptorContainerKind walks to the field named by names and returns
// Object for message/map/Struct, List for repeated, Scalar otherwise.
// When the walk cannot classify (open Struct tail mid-path), returns Unknown.
func descriptorContainerKind(desc protoreflect.MessageDescriptor, names []string) (querysql.ContainerKind, error) {
	if len(names) == 0 {
		return querysql.ContainerKindObject, nil
	}
	cur := desc
	for i := 0; i < len(names); i++ {
		name := names[i]
		if cur == nil {
			return querysql.ContainerKindUnknown, nil
		}
		if cur.FullName() == structFullName {
			// Remaining path is open JSON.
			return querysql.ContainerKindUnknown, nil
		}
		fd := cur.Fields().ByJSONName(name)
		if fd == nil {
			return querysql.ContainerKindUnknown, fmt.Errorf("filter: %w: unknown field %q", domain.ErrInvalidArgument, name)
		}
		last := i == len(names)-1
		if fd.IsList() {
			if !last {
				return querysql.ContainerKindUnknown, fmt.Errorf("filter: %w: cannot traverse list field %q", domain.ErrInvalidArgument, name)
			}
			return querysql.ContainerKindList, nil
		}
		if fd.IsMap() {
			if last {
				return querysql.ContainerKindObject, nil
			}
			// Consume the map key segment and continue when the value
			// is a message; open/scalar map values leave Unknown.
			if i+1 >= len(names) {
				return querysql.ContainerKindUnknown, nil
			}
			if fd.MapKey().Kind() != protoreflect.StringKind {
				return querysql.ContainerKindUnknown, fmt.Errorf("filter: %w: non-string map keys are not supported", domain.ErrInvalidArgument)
			}
			mv := fd.MapValue()
			keyIdx := i + 1
			if keyIdx == len(names)-1 {
				switch mv.Kind() {
				case protoreflect.MessageKind, protoreflect.GroupKind:
					if protoJSONTerminalMessages[mv.Message().FullName()] {
						return querysql.ContainerKindScalar, nil
					}
					return querysql.ContainerKindObject, nil
				default:
					return querysql.ContainerKindScalar, nil
				}
			}
			if mv.Kind() != protoreflect.MessageKind && mv.Kind() != protoreflect.GroupKind {
				return querysql.ContainerKindUnknown, fmt.Errorf("filter: %w: cannot traverse non-message map value", domain.ErrInvalidArgument)
			}
			cur = mv.Message()
			i = keyIdx // next i++ advances past the map key
			continue
		}
		if last {
			switch fd.Kind() {
			case protoreflect.MessageKind, protoreflect.GroupKind:
				if protoJSONTerminalMessages[fd.Message().FullName()] {
					return querysql.ContainerKindScalar, nil
				}
				return querysql.ContainerKindObject, nil
			default:
				return querysql.ContainerKindScalar, nil
			}
		}
		if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
			return querysql.ContainerKindScalar, nil
		}
		cur = fd.Message()
	}
	return querysql.ContainerKindUnknown, nil
}

func (r queryFieldResolver) jsonbContainerExpr(resourceSegs []string, ctx querysql.ResolveContext) (string, error) {
	if len(resourceSegs) == 0 {
		return "", fmt.Errorf("filter: %w: unsupported field \"resource\"", domain.ErrInvalidArgument)
	}
	head, rest := resourceSegs[0], resourceSegs[1:]
	switch head {
	case "labels":
		if len(rest) != 0 {
			return "", fmt.Errorf("filter: %w: unsupported nested path under map field", domain.ErrInvalidArgument)
		}
		return "er.labels", nil
	case "localLabels":
		if len(rest) != 0 {
			return "", fmt.Errorf("filter: %w: unsupported nested path under map field", domain.ErrInvalidArgument)
		}
		return "inv.labels", nil
	case "conditions":
		switch len(rest) {
		case 0:
			return "inv.conditions", nil
		case 1:
			return fmt.Sprintf("inv.conditions -> %s", ctx.Bind(rest[0])), nil
		default:
			return "", fmt.Errorf("filter: %w: unsupported field \"resource.conditions.%s\"", domain.ErrInvalidArgument, strings.Join(rest, "."))
		}
	case "spec":
		names, err := r.validateSpecPath(ctx, rest)
		if err != nil {
			return "", err
		}
		return chainedJSONB("ri.spec", names, ctx.Bind), nil
	case "observation":
		names, err := r.validateObservationPath(ctx, rest)
		if err != nil {
			return "", err
		}
		return chainedJSONB("inv.observation", names, ctx.Bind), nil
	default:
		return "", fmt.Errorf("filter: %w: unsupported field \"resource.%s\"", domain.ErrInvalidArgument, strings.Join(resourceSegs, "."))
	}
}

func chainedJSONB(column string, names []string, bind func(any) string) string {
	var sb strings.Builder
	sb.WriteString(column)
	for _, n := range names {
		fmt.Fprintf(&sb, " -> %s", bind(n))
	}
	return sb.String()
}

func jsonbListStringMembership(container, keyPlaceholder string) string {
	// NULL container → SQL NULL (three-valued); non-array → NULL.
	// Array string membership uses @> (contains the JSON string scalar)
	// rather than jsonb_array_elements so each candidate pays one jsonb
	// operator instead of expanding the array element-by-element.
	return fmt.Sprintf(
		`(CASE
			WHEN %s IS NULL THEN NULL
			WHEN jsonb_typeof(%s) <> 'array' THEN NULL
			ELSE %s @> to_jsonb(%s::text)
		END)`,
		container, container, container, keyPlaceholder,
	)
}

func jsonbDynamicMembership(container, keyPlaceholder string) string {
	return fmt.Sprintf(
		`(CASE jsonb_typeof(%s)
			WHEN 'object' THEN %s ? %s
			WHEN 'array' THEN %s @> to_jsonb(%s::text)
			ELSE NULL
		END)`,
		container, container, keyPlaceholder, container, keyPlaceholder,
	)
}
