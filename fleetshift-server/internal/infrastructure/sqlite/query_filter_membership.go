package sqlite

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

	if effective == querysql.ContainerKindObject {
		full := append(append([]string(nil), segs...), key)
		return r.ResolvePresence(querysql.FieldPath{Segments: full}, ctx)
	}

	ref, err := r.jsonContainerRef(segs[1:], ctx)
	if err != nil {
		return "", err
	}
	keyPh := ctx.Bind(key)

	switch effective {
	case querysql.ContainerKindList:
		return jsonListStringMembership(ref, keyPh), nil
	case querysql.ContainerKindUnknown:
		return jsonDynamicMembership(ref, keyPh), nil
	default:
		return "", fmt.Errorf("filter: %w: unsupported container kind", domain.ErrInvalidArgument)
	}
}

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
		return querysql.ContainerKindScalar, nil
	case "conditions":
		switch len(rest) {
		case 0, 1:
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
			i = keyIdx
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

// jsonRef addresses a JSON value as column + optional path expression
// so json_type/json_each can inspect nested documents without
// extracting scalars into bare SQL text (which is not valid JSON).
type jsonRef struct {
	column string
	path   string // empty means the whole column; otherwise a SQL expr
}

func (r queryFieldResolver) jsonContainerRef(resourceSegs []string, ctx querysql.ResolveContext) (jsonRef, error) {
	if len(resourceSegs) == 0 {
		return jsonRef{}, fmt.Errorf("filter: %w: unsupported field \"resource\"", domain.ErrInvalidArgument)
	}
	head, rest := resourceSegs[0], resourceSegs[1:]
	switch head {
	case "labels":
		if len(rest) != 0 {
			return jsonRef{}, fmt.Errorf("filter: %w: unsupported nested path under map field", domain.ErrInvalidArgument)
		}
		return jsonRef{column: "er.labels"}, nil
	case "localLabels":
		if len(rest) != 0 {
			return jsonRef{}, fmt.Errorf("filter: %w: unsupported nested path under map field", domain.ErrInvalidArgument)
		}
		return jsonRef{column: "inv.labels"}, nil
	case "conditions":
		switch len(rest) {
		case 0:
			return jsonRef{column: "inv.conditions"}, nil
		case 1:
			return jsonRef{column: "inv.conditions", path: jsonQuotedPathExpr(ctx.Bind(rest[0]))}, nil
		default:
			return jsonRef{}, fmt.Errorf("filter: %w: unsupported field \"resource.conditions.%s\"", domain.ErrInvalidArgument, strings.Join(rest, "."))
		}
	case "spec":
		names, err := r.validateSpecPath(ctx, rest)
		if err != nil {
			return jsonRef{}, err
		}
		if len(names) == 0 {
			return jsonRef{column: "ri.spec"}, nil
		}
		return jsonRef{column: "ri.spec", path: jsonQuotedPathChain(names, ctx.Bind)}, nil
	case "observation":
		names, err := r.validateObservationPath(ctx, rest)
		if err != nil {
			return jsonRef{}, err
		}
		if len(names) == 0 {
			return jsonRef{column: "inv.observation"}, nil
		}
		return jsonRef{column: "inv.observation", path: jsonQuotedPathChain(names, ctx.Bind)}, nil
	default:
		return jsonRef{}, fmt.Errorf("filter: %w: unsupported field \"resource.%s\"", domain.ErrInvalidArgument, strings.Join(resourceSegs, "."))
	}
}

func (r jsonRef) typeExpr() string {
	if r.path == "" {
		return fmt.Sprintf("json_type(%s)", r.column)
	}
	return fmt.Sprintf("json_type(%s, %s)", r.column, r.path)
}

func (r jsonRef) eachExpr() string {
	if r.path == "" {
		return fmt.Sprintf("json_each(%s)", r.column)
	}
	return fmt.Sprintf("json_each(%s, %s)", r.column, r.path)
}

func (r jsonRef) nullCheck() string {
	if r.path == "" {
		return r.column + " IS NULL"
	}
	// Missing nested path → json_type IS NULL.
	return fmt.Sprintf("json_type(%s, %s) IS NULL", r.column, r.path)
}

func jsonListStringMembership(ref jsonRef, keyPlaceholder string) string {
	return fmt.Sprintf(
		`(CASE
			WHEN %s THEN NULL
			WHEN %s <> 'array' THEN NULL
			ELSE EXISTS (
				SELECT 1 FROM %s AS e
				WHERE e.type = 'text' AND e.value = %s
			)
		END)`,
		ref.nullCheck(), ref.typeExpr(), ref.eachExpr(), keyPlaceholder,
	)
}

func jsonDynamicMembership(ref jsonRef, keyPlaceholder string) string {
	keyPath := jsonQuotedPathExpr(keyPlaceholder)
	var objectPresent string
	if ref.path == "" {
		objectPresent = fmt.Sprintf("json_type(%s, %s) IS NOT NULL", ref.column, keyPath)
	} else {
		// Append ."key" onto the container path expression.
		objectPresent = fmt.Sprintf(
			`json_type(%s, %s || '."' || replace(replace(%s, '\', '\\'), '"', '\"') || '"') IS NOT NULL`,
			ref.column, ref.path, keyPlaceholder,
		)
	}
	return fmt.Sprintf(
		`(CASE %s
			WHEN 'object' THEN %s
			WHEN 'array' THEN EXISTS (
				SELECT 1 FROM %s AS e
				WHERE e.type = 'text' AND e.value = %s
			)
			ELSE NULL
		END)`,
		ref.typeExpr(), objectPresent, ref.eachExpr(), keyPlaceholder,
	)
}
