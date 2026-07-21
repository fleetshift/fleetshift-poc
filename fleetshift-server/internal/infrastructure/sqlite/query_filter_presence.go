package sqlite

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
			return "TRUE", nil
		}
	case "spec":
		if len(rest) == 0 {
			return "ri.spec IS NOT NULL", nil
		}
		names, err := ctx.ValidateSpecPath(rest)
		if err != nil {
			return "", err
		}
		return jsonPathExists("ri.spec", names, ctx.Bind), nil
	case "observation":
		if len(rest) == 0 {
			return "inv.observation IS NOT NULL", nil
		}
		names, err := ctx.ValidateObservationPath(rest)
		if err != nil {
			return "", err
		}
		return jsonPathExists("inv.observation", names, ctx.Bind), nil
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

func mapPresence(column string, rest []string, bind func(any) string) (string, error) {
	switch len(rest) {
	case 0:
		// Empty projected maps are omitted; require at least one key.
		return fmt.Sprintf("(%s IS NOT NULL AND EXISTS (SELECT 1 FROM json_each(%s)))", column, column), nil
	case 1:
		path := jsonQuotedPathExpr(bind(rest[0]))
		// json_type is NOT NULL for present keys including JSON null;
		// guard the column so LEFT JOIN nulls negate cleanly.
		return fmt.Sprintf("(%s IS NOT NULL AND json_type(%s, %s) IS NOT NULL)", column, column, path), nil
	default:
		return "", fmt.Errorf("filter: %w: unsupported nested path under map field", domain.ErrInvalidArgument)
	}
}

func conditionPresence(rest []string, bind func(any) string) (string, error) {
	switch len(rest) {
	case 0:
		return mapPresence("inv.conditions", nil, bind)
	case 1:
		path := jsonQuotedPathExpr(bind(rest[0]))
		return fmt.Sprintf("(inv.conditions IS NOT NULL AND json_type(inv.conditions, %s) IS NOT NULL)", path), nil
	case 2:
		if !conditionSubfields[rest[1]] {
			return "", fmt.Errorf("filter: %w: unsupported condition subfield %q", domain.ErrInvalidArgument, rest[1])
		}
		keyPh := bind(rest[0])
		switch rest[1] {
		case "status":
			extract := jsonExtractKeySubfield("inv.conditions", keyPh, "status")
			return fmt.Sprintf(
				"(inv.conditions IS NOT NULL AND COALESCE(%s, '') <> '')",
				extract,
			), nil
		case "reason", "message":
			extract := jsonExtractKeySubfield("inv.conditions", keyPh, rest[1])
			return fmt.Sprintf("(COALESCE(%s, '') <> '')", extract), nil
		case "lastTransitionTime":
			extract := jsonExtractKeySubfield("inv.conditions", keyPh, "lastTransitionTime")
			return fmt.Sprintf(
				"(COALESCE(%s, '') <> '' AND %s <> %s)",
				extract, extract, bind(zeroProtoJSONTimestamp),
			), nil
		}
	}
	return "", fmt.Errorf("filter: %w: unsupported field \"resource.conditions.%s\"", domain.ErrInvalidArgument, strings.Join(rest, "."))
}

// jsonPathExists is true when the nested JSON path exists, including
// a JSON-null leaf (json_type IS NOT NULL).
func jsonPathExists(column string, names []string, bind func(any) string) string {
	if len(names) == 0 {
		return column + " IS NOT NULL"
	}
	path := jsonQuotedPathChain(names, bind)
	return fmt.Sprintf("json_type(%s, %s) IS NOT NULL", column, path)
}
