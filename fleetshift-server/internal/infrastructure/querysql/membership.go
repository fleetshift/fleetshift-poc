package querysql

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ContainerKind classifies a container-membership RHS path for SQL
// lowering. Dialects receive only List or Unknown via
// [JSONMembershipTarget]; Object membership is lowered through
// [FieldResolver.ResolvePresence], and Scalar is rejected in
// querysql before any dialect hook runs.
type ContainerKind int

const (
	// ContainerKindUnknown means the runtime JSON shape decides:
	// object → key membership, array → string-value membership,
	// missing/null/scalar → SQL NULL (CEL unknown/error).
	ContainerKindUnknown ContainerKind = iota
	// ContainerKindObject means object-key membership (including a
	// JSON-null value for that key).
	ContainerKindObject
	// ContainerKindList means string-value membership in a JSON array.
	ContainerKindList
	// ContainerKindScalar is not a container; membership fails closed
	// with [domain.ErrInvalidArgument].
	ContainerKindScalar
)

// JSONMembershipRoot identifies which ProtoJSON JSON column a list /
// unknown container membership targets.
type JSONMembershipRoot int

const (
	// JSONMembershipRootSpec is resource.spec (and nested paths).
	JSONMembershipRootSpec JSONMembershipRoot = iota + 1
	// JSONMembershipRootObservation is resource.observation (and nested paths).
	JSONMembershipRootObservation
)

// JSONMembershipTarget is what a dialect needs to render list or
// runtime-dispatch container membership SQL. Kind is restricted by
// contract to [ContainerKindList] or [ContainerKindUnknown]; Object
// and Scalar must not reach [FieldResolver.ResolveJSONMembership].
type JSONMembershipTarget struct {
	Root JSONMembershipRoot
	// Path is the schema-validated (or structurally accepted) relative
	// JSON path under Root — empty for the root itself.
	Path []string
	Kind ContainerKind
}

// ResourceContainerClass is the dialect-neutral result of classifying
// a `"key" in resource.<…>` RHS path.
type ResourceContainerClass struct {
	Kind ContainerKind

	// UnderSpec / UnderObservation mark a container under
	// resource.spec / resource.observation (including the root). At
	// most one is set. When set, Path is the schema-validated (or
	// passthrough) segments under that root — empty for the root
	// itself. For List/Unknown, Path is copied into
	// [JSONMembershipTarget] for dialect JSON extraction. Object
	// membership still goes through ResolvePresence(path+key), which
	// validates the appended member name (a second schema walk that
	// reuses the per-compilation schema lookup).
	UnderSpec        bool
	UnderObservation bool
	Path             []string
}

// classifyResourceContainer classifies resource.<segs> for container
// membership using ctx's guard and schema provider.
func classifyResourceContainer(segs []string, ctx ResolveContext) (ResourceContainerClass, error) {
	if len(segs) == 0 {
		return ResourceContainerClass{}, fmt.Errorf("filter: %w: unsupported field \"resource\"", domain.ErrInvalidArgument)
	}
	head, rest := segs[0], segs[1:]
	switch head {
	case "labels", "localLabels":
		if len(rest) == 0 {
			return ResourceContainerClass{Kind: ContainerKindObject}, nil
		}
		// Map values are strings (scalars), not containers.
		return ResourceContainerClass{Kind: ContainerKindScalar}, nil
	case "conditions":
		switch len(rest) {
		case 0, 1:
			return ResourceContainerClass{Kind: ContainerKindObject}, nil
		default:
			return ResourceContainerClass{Kind: ContainerKindScalar}, nil
		}
	case "spec":
		if len(rest) == 0 {
			return ResourceContainerClass{Kind: ContainerKindObject, UnderSpec: true, Path: nil}, nil
		}
		schema, ok, err := ctx.lookupSchema()
		if err != nil {
			return ResourceContainerClass{}, err
		}
		if !ok {
			return ResourceContainerClass{Kind: ContainerKindUnknown, UnderSpec: true, Path: rest}, nil
		}
		kind, path, err := classifyDescriptorContainer(schema.SpecDescriptor, "resource.spec", rest)
		if err != nil {
			return ResourceContainerClass{}, err
		}
		return ResourceContainerClass{Kind: kind, UnderSpec: true, Path: path}, nil
	case "observation":
		if len(rest) == 0 {
			return ResourceContainerClass{Kind: ContainerKindObject, UnderObservation: true, Path: nil}, nil
		}
		schema, ok, err := ctx.lookupSchema()
		if err != nil {
			return ResourceContainerClass{}, err
		}
		if !ok {
			return ResourceContainerClass{Kind: ContainerKindUnknown, UnderObservation: true, Path: rest}, nil
		}
		kind, path, err := classifyDescriptorContainer(schema.InventoryObservationDescriptor, "resource.observation", rest)
		if err != nil {
			return ResourceContainerClass{}, err
		}
		return ResourceContainerClass{Kind: kind, UnderObservation: true, Path: path}, nil
	case "name", "uid", "intentVersion", "state", "pauseReason", "generation",
		"localUpdateTime", "indexUpdateTime":
		if len(rest) == 0 {
			return ResourceContainerClass{Kind: ContainerKindScalar}, nil
		}
	}
	return ResourceContainerClass{}, fmt.Errorf("filter: %w: unsupported field \"resource.%s\"", domain.ErrInvalidArgument, strings.Join(segs, "."))
}

// classifyDescriptorContainer validates names under root when desc is
// non-nil and classifies the terminal container kind. A nil desc keeps
// the path structurally (Kind Unknown).
func classifyDescriptorContainer(
	desc protoreflect.MessageDescriptor,
	root string,
	names []string,
) (ContainerKind, []string, error) {
	if desc == nil {
		return ContainerKindUnknown, names, nil
	}
	validated, err := ValidateDescriptorPath(desc, root, names)
	if err != nil {
		return ContainerKindUnknown, nil, err
	}
	kind, err := DescriptorContainerKind(desc, validated)
	return kind, validated, err
}

// DescriptorContainerKind walks to the field named by names and returns
// Object for message/map/Struct, List for repeated string, Scalar
// otherwise. Repeated non-string fields fail closed (list-membership
// SQL only matches JSON string elements). When the walk cannot
// classify (open Struct tail mid-path), returns Unknown.
func DescriptorContainerKind(desc protoreflect.MessageDescriptor, names []string) (ContainerKind, error) {
	if len(names) == 0 {
		return ContainerKindObject, nil
	}
	cur := desc
	for i := 0; i < len(names); i++ {
		name := names[i]
		if cur == nil {
			return ContainerKindUnknown, nil
		}
		if cur.FullName() == structFullName {
			// Remaining path is open JSON.
			return ContainerKindUnknown, nil
		}
		fd := cur.Fields().ByJSONName(name)
		if fd == nil {
			return ContainerKindUnknown, fmt.Errorf("filter: %w: unknown field %q", domain.ErrInvalidArgument, name)
		}
		last := i == len(names)-1
		if fd.IsList() {
			if !last {
				return ContainerKindUnknown, fmt.Errorf("filter: %w: cannot traverse list field %q", domain.ErrInvalidArgument, name)
			}
			// Container membership SQL compares a string literal against
			// JSON string elements only (see docs/query-resources-cel-filters.md).
			if fd.Kind() != protoreflect.StringKind {
				return ContainerKindUnknown, fmt.Errorf("filter: %w: non-string list elements are not supported", domain.ErrInvalidArgument)
			}
			return ContainerKindList, nil
		}
		if fd.IsMap() {
			if last {
				return ContainerKindObject, nil
			}
			// Consume the map key segment and continue when the value
			// is a message; open/scalar map values leave Unknown.
			if i+1 >= len(names) {
				return ContainerKindUnknown, nil
			}
			if fd.MapKey().Kind() != protoreflect.StringKind {
				return ContainerKindUnknown, fmt.Errorf("filter: %w: non-string map keys are not supported", domain.ErrInvalidArgument)
			}
			mv := fd.MapValue()
			keyIdx := i + 1
			if keyIdx == len(names)-1 {
				switch mv.Kind() {
				case protoreflect.MessageKind, protoreflect.GroupKind:
					if protoJSONTerminalMessages[mv.Message().FullName()] {
						return ContainerKindScalar, nil
					}
					return ContainerKindObject, nil
				default:
					return ContainerKindScalar, nil
				}
			}
			if mv.Kind() != protoreflect.MessageKind && mv.Kind() != protoreflect.GroupKind {
				return ContainerKindUnknown, fmt.Errorf("filter: %w: cannot traverse non-message map value", domain.ErrInvalidArgument)
			}
			cur = mv.Message()
			i = keyIdx // next i++ advances past the map key
			continue
		}
		if last {
			switch fd.Kind() {
			case protoreflect.MessageKind, protoreflect.GroupKind:
				if protoJSONTerminalMessages[fd.Message().FullName()] {
					return ContainerKindScalar, nil
				}
				return ContainerKindObject, nil
			default:
				return ContainerKindScalar, nil
			}
		}
		if fd.Kind() != protoreflect.MessageKind && fd.Kind() != protoreflect.GroupKind {
			return ContainerKindScalar, nil
		}
		cur = fd.Message()
	}
	return ContainerKindUnknown, nil
}

// resolveContainerMembership owns `"key" in <path>` orchestration:
// resource-root and known-object membership become ResolvePresence;
// scalars fail closed; list/unknown containers call
// ResolveJSONMembership with a dialect-only target.
func resolveContainerMembership(fields FieldResolver, path FieldPath, key string, ctx ResolveContext) (string, error) {
	segs := path.Segments
	if len(segs) == 0 {
		return "", fmt.Errorf("filter: %w: empty field path", domain.ErrInvalidArgument)
	}

	// "spec" in resource → has(resource.spec), including proto3 default omission.
	if len(segs) == 1 && segs[0] == "resource" {
		return fields.ResolvePresence(FieldPath{Segments: []string{"resource", key}}, ctx)
	}
	if len(segs) == 1 {
		return "", fmt.Errorf("filter: %w: membership requires a container field", domain.ErrInvalidArgument)
	}
	if segs[0] != "resource" {
		return "", fmt.Errorf("filter: %w: unsupported field expression", domain.ErrInvalidArgument)
	}

	class, err := ctx.ClassifyResourceContainer(segs[1:])
	if err != nil {
		return "", err
	}
	if class.Kind == ContainerKindScalar {
		return "", fmt.Errorf("filter: %w: membership requires a container field", domain.ErrInvalidArgument)
	}

	// Object-key membership agrees with ResolvePresence(path+key).
	if class.Kind == ContainerKindObject {
		full := append(append([]string(nil), segs...), key)
		return fields.ResolvePresence(FieldPath{Segments: full}, ctx)
	}

	var root JSONMembershipRoot
	switch {
	case class.UnderSpec:
		root = JSONMembershipRootSpec
	case class.UnderObservation:
		root = JSONMembershipRootObservation
	default:
		return "", fmt.Errorf("filter: %w: unsupported container field", domain.ErrInvalidArgument)
	}
	if class.Kind != ContainerKindList && class.Kind != ContainerKindUnknown {
		return "", fmt.Errorf("filter: %w: unsupported container kind", domain.ErrInvalidArgument)
	}
	return fields.ResolveJSONMembership(JSONMembershipTarget{
		Root: root,
		Path: class.Path,
		Kind: class.Kind,
	}, key, ctx)
}
