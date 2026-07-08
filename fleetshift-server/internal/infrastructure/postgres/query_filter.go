package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres/querysql"
)

// queryFieldResolver implements [querysql.FieldResolver] for this
// project's actual row shape: the all_rows/filtered_page envelope
// columns and the resource.*/resource.inventory.* fields the plan's
// "CEL Shape" section documents (see ../query_sql.go for the columns
// this mirrors). querysql's compiler owns CEL AST lowering and knows
// nothing about any of this -- see querysql's package doc for why the
// split lands here.
type queryFieldResolver struct {
	// SchemaProvider, if set, lets resource.spec.* (and, once
	// activated, resource.inventory.observation.*) paths be validated
	// against a real protobuf descriptor when the filter's top-level
	// resource_type guard resolves to a type with one registered. See
	// [domain.QuerySchemaProvider]'s doc for the absence-of-schema
	// fallback behavior when this is nil or has nothing registered for
	// the guarded type.
	SchemaProvider domain.QuerySchemaProvider
}

var _ querysql.FieldResolver = queryFieldResolver{}

// envelopeColumns are the common QueryResources envelope fields
// (see the plan's "CEL Shape" section), each a direct column on the
// all_rows/filtered_page CTEs (see ../query_sql.go). All are plain
// SQL text, so no JSON cast handling is needed for them.
var envelopeColumns = map[string]bool{
	"kind":            true,
	"name":            true,
	"platform_name":   true,
	"resource_type":   true,
	"service_name":    true,
	"api_version":     true,
	"collection_name": true,
	"resource_id":     true,
}

// conditionSubfields are the resource.inventory.conditions["Type"].*
// fields the plan documents; all are text-valued so no cast handling
// is needed for them either.
var conditionSubfields = map[string]bool{
	"status":             true,
	"reason":             true,
	"message":            true,
	"lastTransitionTime": true,
}

// identifierPattern is the character set CEL's own lexer already
// restricts field-path segments to. jsonFieldChain re-checks it
// before inlining a segment into SQL text as defense in depth, in
// case a future refactor ever starts gathering path segments some
// other way.
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Resolve implements [querysql.FieldResolver].
func (r queryFieldResolver) Resolve(path querysql.FieldPath, want querysql.TypeHint, ctx querysql.ResolveContext) (querysql.SQLExpr, error) {
	segs := path.Segments
	if len(segs) == 0 {
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: empty field path", domain.ErrInvalidArgument)
	}
	if len(segs) == 1 {
		if envelopeColumns[segs[0]] {
			return querysql.SQLExpr{SQL: segs[0]}, nil
		}
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field %q", domain.ErrInvalidArgument, segs[0])
	}
	if segs[0] != "resource" {
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field expression", domain.ErrInvalidArgument)
	}
	return r.resolveResourceField(segs[1:], want, ctx)
}

// resolveResourceField maps the segments following `resource` to a
// SQL expression. See the package doc and the plan's "CEL Shape"
// section for the field list this mirrors.
func (r queryFieldResolver) resolveResourceField(segs []string, want querysql.TypeHint, ctx querysql.ResolveContext) (querysql.SQLExpr, error) {
	if len(segs) == 0 {
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field \"resource\"", domain.ErrInvalidArgument)
	}

	head, rest := segs[0], segs[1:]
	switch head {
	case "name":
		if len(rest) == 0 {
			// Both platform and extension rows compute their
			// envelope platform_name identically as
			// collection_name || '/' || resource_id (see
			// ../query_sql.go), so resource.name can reuse that
			// column directly instead of a separate one.
			return querysql.SQLExpr{SQL: "platform_name"}, nil
		}
	case "uid":
		if len(rest) == 0 {
			return querysql.SQLExpr{SQL: "extension_uid::text"}, nil
		}
	case "labels":
		if len(rest) == 1 {
			return labelField("resource_labels", rest[0], ctx.Bind, want), nil
		}
	case "intent_version":
		if len(rest) == 0 {
			return querysql.SQLExpr{SQL: "intent_version"}, nil
		}
	case "state":
		if len(rest) == 0 {
			return querysql.SQLExpr{SQL: "fulfillment_state"}, nil
		}
	case "pause_reason":
		if len(rest) == 0 {
			return querysql.SQLExpr{SQL: "pause_reason"}, nil
		}
	case "generation":
		if len(rest) == 0 {
			return querysql.SQLExpr{SQL: "generation"}, nil
		}
	case "spec":
		if len(rest) > 0 {
			if ctx.GuardedResourceType == nil {
				return querysql.SQLExpr{}, fmt.Errorf(
					"filter: %w: resource.spec.* requires a top-level resource_type == \"...\" conjunct",
					domain.ErrInvalidArgument)
			}
			names, err := r.validateSpecPath(ctx, rest)
			if err != nil {
				return querysql.SQLExpr{}, err
			}
			return jsonTextField("spec", names, want)
		}
	case "inventory":
		return r.resolveInventoryField(rest, want, ctx)
	}
	return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field \"resource.%s\"", domain.ErrInvalidArgument, strings.Join(segs, "."))
}

func (r queryFieldResolver) resolveInventoryField(segs []string, want querysql.TypeHint, ctx querysql.ResolveContext) (querysql.SQLExpr, error) {
	if len(segs) == 0 {
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field \"resource.inventory\"", domain.ErrInvalidArgument)
	}

	head, rest := segs[0], segs[1:]
	switch head {
	case "labels":
		if len(rest) == 1 {
			return labelField("inventory_labels", rest[0], ctx.Bind, want), nil
		}
	case "conditions":
		if len(rest) == 2 && conditionSubfields[rest[1]] {
			expr := fmt.Sprintf("inventory_conditions -> %s ->> '%s'", ctx.Bind(rest[0]), rest[1])
			return castByHint(expr, want), nil
		}
	case "observation":
		if len(rest) > 0 {
			if ctx.GuardedResourceType == nil {
				return querysql.SQLExpr{}, fmt.Errorf(
					"filter: %w: resource.inventory.observation.* requires a top-level resource_type == \"...\" conjunct",
					domain.ErrInvalidArgument)
			}
			names, err := r.validateObservationPath(ctx, rest)
			if err != nil {
				return querysql.SQLExpr{}, err
			}
			return jsonTextField("inventory_observation", names, want)
		}
	}
	return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field \"resource.inventory.%s\"", domain.ErrInvalidArgument, strings.Join(segs, "."))
}

// labelField handles the common `<column> ->> <key>` shape shared by
// resource.labels[...] and resource.inventory.labels[...]. key comes
// from the filter text (a CEL map-index string literal), so it is
// bound as a SQL parameter via ctx.Bind rather than inlined -- see
// [querysql.ResolveContext.Bind]'s doc -- even though CEL's lexer
// already restricts *identifiers*, since a map-index literal can
// contain arbitrary characters (quotes, comment sequences, ...).
func labelField(column, key string, bind func(any) string, want querysql.TypeHint) querysql.SQLExpr {
	return castByHint(fmt.Sprintf("%s ->> %s", column, bind(key)), want)
}

// jsonTextField builds a chained ->/->> extraction from column
// through names -- the parsed dotted-path field names from a
// resource.spec.foo.bar/resource.inventory.observation.foo.bar
// selector -- and casts the result per want (see castByHint). names
// come from CEL field-path segments -- already restricted by CEL's
// own lexer to identifierPattern -- but this re-validates before
// inlining them into SQL text as defense in depth, per the plan's
// "SQL identifiers and JSON paths must come only from static field
// mappings or schema descriptors" rule.
func jsonTextField(column string, names []string, want querysql.TypeHint) (querysql.SQLExpr, error) {
	for _, n := range names {
		if !identifierPattern.MatchString(n) {
			return querysql.SQLExpr{}, fmt.Errorf("filter: %w: invalid field name %q", domain.ErrInvalidArgument, n)
		}
	}
	var sb strings.Builder
	sb.WriteString(column)
	for i, n := range names {
		op := "->"
		if i == len(names)-1 {
			op = "->>"
		}
		fmt.Fprintf(&sb, " %s '%s'", op, n)
	}
	return castByHint(sb.String(), want), nil
}

// castByHint wraps a JSON-text-extracted SQL expression in a cast
// matching want, so e.g. `(resource.generation as extracted via ->>)
// > 4` compares numerically rather than lexically. TypeHintString and
// TypeHintUnknown need no cast: ->> already yields text, which is
// exactly what a string comparison needs.
func castByHint(expr string, want querysql.TypeHint) querysql.SQLExpr {
	switch want {
	case querysql.TypeHintBool:
		return querysql.SQLExpr{SQL: safeJSONCast(expr, "boolean")}
	case querysql.TypeHintNumber:
		return querysql.SQLExpr{SQL: safeJSONCast(expr, "numeric")}
	default:
		return querysql.SQLExpr{SQL: expr}
	}
}

// safeJSONCast casts a JSON-text-extracted SQL expression to sqlType,
// guarded by pg_input_is_valid so a row whose value at this JSON path
// isn't actually castable evaluates to SQL NULL (i.e. doesn't match)
// instead of raising a runtime cast error.
//
// This guard is required even for fields gated by a
// resource_type == "..." conjunct (see hasResourceTypeGuard in
// querysql): standard SQL gives no evaluation-order guarantee for a
// plain WHERE-clause AND, so Postgres remains free to evaluate this
// cast against rows the guard conjunct would otherwise exclude (e.g.
// a differently-typed row whose value at the same JSON key is a
// string, not a number). It is equally required for fields with no
// type guard at all -- e.g. resource.labels[...]/
// resource.inventory.labels[...], which share their column across
// every resource type -- where there is no guard conjunct to even
// attempt to rely on.
//
// pg_input_is_valid is a Postgres 17+ builtin (this project targets
// Postgres 18 uniformly, see e.g. deploy/podman/overrides/postgres.yaml);
// it reports whether a text value is valid input for a type without
// raising an error. CASE is used rather than e.g. a boolean AND
// because Postgres does guarantee CASE only evaluates the selected
// branch for expressions that vary per row -- verified against a live
// Postgres 18 instance: a bare `(expr)::numeric` conjunct can still
// fail depending on plan shape, but `CASE WHEN pg_input_is_valid(expr,
// 'numeric') THEN (expr)::numeric END` reliably returns NULL instead
// for a non-numeric expr.
func safeJSONCast(expr, sqlType string) string {
	return fmt.Sprintf("(CASE WHEN pg_input_is_valid(%s, '%s') THEN (%s)::%s END)", expr, sqlType, expr, sqlType)
}
