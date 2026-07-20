package postgres

import (
	"fmt"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// zeroProtoJSONTimestamp is the writer's ProtoJSON spelling of
// time.Time{}. Condition lastTransitionTime presence must treat this
// as absent — inventory JSON always stores the key, while projection
// omits zero timestamps.
const zeroProtoJSONTimestamp = "0001-01-01T00:00:00Z"

// ResolvePresence implements [querysql.FieldResolver].
func (r queryFieldResolver) ResolvePresence(path querysql.FieldPath, ctx querysql.ResolveContext) (string, error) {
	segs := path.Segments
	if len(segs) == 0 {
		return "", fmt.Errorf("filter: %w: empty field path", domain.ErrInvalidArgument)
	}
	if len(segs) == 1 {
		return "", fmt.Errorf("filter: %w: has() requires a field select (envelope fields are always present)", domain.ErrInvalidArgument)
	}
	if segs[0] != "resource" {
		return "", fmt.Errorf("filter: %w: unsupported field expression", domain.ErrInvalidArgument)
	}
	return r.resolveResourcePresence(segs[1:], ctx)
}

func (r queryFieldResolver) resolveResourcePresence(segs []string, ctx querysql.ResolveContext) (string, error) {
	if len(segs) == 0 {
		return "", fmt.Errorf("filter: %w: unsupported field \"resource\"", domain.ErrInvalidArgument)
	}
	head, rest := segs[0], segs[1:]
	switch head {
	case "name", "uid":
		if len(rest) == 0 {
			// Always projected on every extension row.
			return "TRUE", nil
		}
	case "spec":
		if len(rest) == 0 {
			return "ri.spec IS NOT NULL", nil
		}
		names, err := r.validateSpecPath(ctx, rest)
		if err != nil {
			return "", err
		}
		return jsonbPathExists("ri.spec", names, ctx.Bind), nil
	case "observation":
		if len(rest) == 0 {
			return "inv.observation IS NOT NULL", nil
		}
		names, err := r.validateObservationPath(ctx, rest)
		if err != nil {
			return "", err
		}
		return jsonbPathExists("inv.observation", names, ctx.Bind), nil
	case "labels":
		return mapPresence("er.labels", rest, ctx.Bind)
	case "localLabels":
		return mapPresence("inv.labels", rest, ctx.Bind)
	case "conditions":
		return conditionPresence(rest, ctx.Bind)
	case "pauseReason":
		if len(rest) == 0 {
			return "(f.pause_reason IS NOT NULL AND f.pause_reason <> '')", nil
		}
	case "intentVersion":
		if len(rest) == 0 {
			return "(erm.current_version IS NOT NULL AND erm.current_version <> 0)", nil
		}
	case "state":
		if len(rest) == 0 {
			// Fulfillment join present with a concrete stored state.
			return "(f.state IS NOT NULL AND f.state <> '')", nil
		}
	case "generation":
		if len(rest) == 0 {
			return "(f.generation IS NOT NULL AND f.generation <> 0)", nil
		}
	case "localUpdateTime":
		if len(rest) == 0 {
			return "inv.observed_at IS NOT NULL", nil
		}
	case "indexUpdateTime":
		if len(rest) == 0 {
			return "inv.updated_at IS NOT NULL", nil
		}
	}
	return "", fmt.Errorf("filter: %w: unsupported field \"resource.%s\"", domain.ErrInvalidArgument, strings.Join(segs, "."))
}

// mapPresence handles resource.labels / localLabels / conditions
// container and key presence. Empty projected maps are omitted
// (ProtoJSON), so the container itself requires a non-empty object.
func mapPresence(column string, rest []string, bind func(any) string) (string, error) {
	switch len(rest) {
	case 0:
		return fmt.Sprintf("(%s IS NOT NULL AND %s <> '{}'::jsonb)", column, column), nil
	case 1:
		return fmt.Sprintf("(%s IS NOT NULL AND %s ? %s)", column, column, bind(rest[0])), nil
	default:
		return "", fmt.Errorf("filter: %w: unsupported nested path under map field", domain.ErrInvalidArgument)
	}
}

func conditionPresence(rest []string, bind func(any) string) (string, error) {
	switch len(rest) {
	case 0:
		return mapPresence("inv.conditions", nil, bind)
	case 1:
		return fmt.Sprintf("(inv.conditions IS NOT NULL AND inv.conditions ? %s)", bind(rest[0])), nil
	case 2:
		if !conditionSubfields[rest[1]] {
			return "", fmt.Errorf("filter: %w: unsupported condition subfield %q", domain.ErrInvalidArgument, rest[1])
		}
		key := bind(rest[0])
		switch rest[1] {
		case "status":
			// Status is always projected when the condition entry exists.
			return fmt.Sprintf(
				"(inv.conditions ? %s AND COALESCE(inv.conditions -> %s ->> 'status', '') <> '')",
				key, key,
			), nil
		case "reason", "message":
			// Empty strings are stored but omitted from ProtoJSON.
			return fmt.Sprintf(
				"(COALESCE(inv.conditions -> %s ->> %s, '') <> '')",
				key, bind(rest[1]),
			), nil
		case "lastTransitionTime":
			// Writer always stores the key; projection omits zero time.
			return fmt.Sprintf(
				"(COALESCE(inv.conditions -> %s ->> 'lastTransitionTime', '') <> '' AND "+
					"inv.conditions -> %s ->> 'lastTransitionTime' <> %s)",
				key, key, bind(zeroProtoJSONTimestamp),
			), nil
		}
	}
	return "", fmt.Errorf("filter: %w: unsupported field \"resource.conditions.%s\"", domain.ErrInvalidArgument, strings.Join(rest, "."))
}

// jsonbPathExists is true when the nested JSON path exists, including
// a JSON-null leaf ((column #> path) IS NOT NULL).
func jsonbPathExists(column string, names []string, bind func(any) string) string {
	if len(names) == 0 {
		return column + " IS NOT NULL"
	}
	parts := make([]string, len(names))
	for i, n := range names {
		parts[i] = bind(n)
	}
	// Build ARRAY[$1,$2,...]::text[] for #>.
	return fmt.Sprintf("(%s #> ARRAY[%s]::text[]) IS NOT NULL", column, strings.Join(parts, ", "))
}
