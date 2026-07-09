package postgres

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// queryFieldResolver implements [querysql.FieldResolver] for
// QueryResources' extension-only row shape: the er/erm/ri/f/inv
// aliases in buildQueryResourcesSQL's filtered_page CTE. querysql's
// compiler owns CEL AST lowering and knows nothing about any of this
// -- see querysql's package doc for why the split lands here.
//
// Public CEL fields match the target QueryResources response shape:
// envelope name, envelope resource_type, and fields under resource.
// Old POC envelope aliases (platform_name, kind, service_name,
// api_version, collection_name, resource_id) and platform-only body
// fields are rejected.
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
		return r.resolveEnvelopeField(segs[0])
	}
	if segs[0] != "resource" {
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field expression", domain.ErrInvalidArgument)
	}
	return r.resolveResourceField(segs[1:], want, ctx)
}

func (r queryFieldResolver) resolveEnvelopeField(name string) (querysql.SQLExpr, error) {
	switch name {
	case "name":
		// Canonical full name. Equality / IN against well-formed
		// "//service/collection/id" literals special-case to
		// constituent-column predicates so the default-order index
		// can seek; other comparisons fall back to the expression.
		return querysql.SQLExpr{
			SQL:     "'//' || er.service_name || '/' || er.collection_name || '/' || er.resource_id",
			Compare: compareFullNameEquality,
			In:      inFullName,
		}, nil
	case "resource_type":
		// Equality / IN against well-formed "service/Type" literals
		// special-case to service_name/type_name predicates so
		// idx_extension_resources_type_query_order can participate.
		return querysql.SQLExpr{
			SQL:     "er.service_name || '/' || er.type_name",
			Compare: compareResourceTypeEquality,
			In:      inResourceType,
		}, nil
	default:
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field %q", domain.ErrInvalidArgument, name)
	}
}

// resolveResourceField maps the segments following `resource` to a
// SQL expression against the er/erm/ri/f/inv aliases.
func (r queryFieldResolver) resolveResourceField(segs []string, want querysql.TypeHint, ctx querysql.ResolveContext) (querysql.SQLExpr, error) {
	if len(segs) == 0 {
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field \"resource\"", domain.ErrInvalidArgument)
	}

	head, rest := segs[0], segs[1:]
	switch head {
	case "name":
		if len(rest) == 0 {
			// Body-level relative resource name
			// (collection_name/resource_id), not an envelope alias.
			return querysql.SQLExpr{SQL: "er.collection_name || '/' || er.resource_id"}, nil
		}
	case "uid":
		if len(rest) == 0 {
			return querysql.SQLExpr{SQL: "er.uid::text"}, nil
		}
	case "labels":
		if len(rest) == 1 {
			return labelField("er.labels", rest[0], ctx.Bind, want), nil
		}
	case "intent_version":
		if len(rest) == 0 {
			return querysql.SQLExpr{SQL: "erm.current_version"}, nil
		}
	case "state":
		if len(rest) == 0 {
			return querysql.SQLExpr{SQL: "f.state"}, nil
		}
	case "pause_reason":
		if len(rest) == 0 {
			return querysql.SQLExpr{SQL: "f.pause_reason"}, nil
		}
	case "generation":
		if len(rest) == 0 {
			return querysql.SQLExpr{SQL: "f.generation"}, nil
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
			return jsonTextField("ri.spec", names, want)
		}
	case "inventory":
		return r.resolveInventoryField(rest, want, ctx)
	case "effective_labels", "representations", "aliases", "relationships":
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field \"resource.%s\"", domain.ErrInvalidArgument, strings.Join(segs, "."))
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
			return labelField("inv.labels", rest[0], ctx.Bind, want), nil
		}
	case "conditions":
		if len(rest) == 2 && conditionSubfields[rest[1]] {
			key := rest[0]
			subfield := rest[1]
			keyPlaceholder := ctx.Bind(key)
			extract := fmt.Sprintf("inv.conditions -> %s ->> '%s'", keyPlaceholder, subfield)
			expr := castByHint(extract, want)
			expr.Compare = conditionContainmentCompare(keyPlaceholder, subfield)
			return expr, nil
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
			return jsonTextField("inv.observation", names, want)
		}
	}
	return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field \"resource.inventory.%s\"", domain.ErrInvalidArgument, strings.Join(segs, "."))
}

// labelField handles the common `<column> ->> <key>` shape shared by
// resource.labels[...] and resource.inventory.labels[...]. key comes
// from the filter text (a CEL map-index string literal), so it is
// bound as a SQL parameter via bind rather than inlined. String
// equality is rewritten to JSONB containment so the GIN indexes can
// participate; other operators keep the ->> / safe-cast path.
func labelField(column, key string, bind func(any) string, want querysql.TypeHint) querysql.SQLExpr {
	keyPlaceholder := bind(key)
	expr := castByHint(fmt.Sprintf("%s ->> %s", column, keyPlaceholder), want)
	expr.Compare = labelContainmentCompare(column, keyPlaceholder)
	return expr
}

func labelContainmentCompare(column, keyPlaceholder string) func(querysql.ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		if op != querysql.OpEqual {
			return "", false, nil
		}
		value, ok := lit.(string)
		if !ok {
			return "", false, nil
		}
		// Reuse the key placeholder already bound during Resolve so
		// equality rewrite does not double-bind the same key. Cast
		// both args to text: jsonb_build_object is polymorphic and
		// Postgres cannot infer bind-parameter types otherwise.
		return fmt.Sprintf("%s @> jsonb_build_object(%s::text, %s::text)", column, keyPlaceholder, bind(value)), true, nil
	}
}

func conditionContainmentCompare(typePlaceholder, subfield string) func(querysql.ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		if op != querysql.OpEqual {
			return "", false, nil
		}
		value, ok := lit.(string)
		if !ok {
			return "", false, nil
		}
		// Condition type key was already bound during Resolve; the
		// subfield name comes from the static whitelist and is bound
		// here as a JSON object key (not inlined). Cast all three
		// args to text for jsonb_build_object's polymorphic signature.
		return fmt.Sprintf(
			"inv.conditions @> jsonb_build_object(%s::text, jsonb_build_object(%s::text, %s::text))",
			typePlaceholder, bind(subfield), bind(value),
		), true, nil
	}
}

func compareFullNameEquality(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
	if op != querysql.OpEqual {
		return "", false, nil
	}
	s, ok := lit.(string)
	if !ok {
		return "", false, nil
	}
	full := domain.FullResourceName(s)
	service := full.ServiceName()
	name := full.ResourceName()
	if service == "" || name == "" || !strings.HasPrefix(s, "//") {
		return "", false, nil
	}
	if _, err := domain.ParseResourceName(string(name)); err != nil {
		return "", false, nil
	}
	return fmt.Sprintf(
		"(er.service_name = %s AND er.collection_name = %s AND er.resource_id = %s)",
		bind(string(service)), bind(string(name.Collection())), bind(string(name.ID())),
	), true, nil
}

func compareResourceTypeEquality(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
	if op != querysql.OpEqual {
		return "", false, nil
	}
	s, ok := lit.(string)
	if !ok {
		return "", false, nil
	}
	rt, err := domain.ParseResourceType(s)
	if err != nil {
		// Fall back to the concatenated expression so a malformed
		// literal still compares (and simply matches nothing) rather
		// than failing closed at compile time for a value that CEL
		// already accepted as a string.
		return "", false, nil
	}
	return fmt.Sprintf(
		"(er.service_name = %s AND er.type_name = %s)",
		bind(string(rt.ServiceName())), bind(rt.TypeName()),
	), true, nil
}

func inResourceType(values []any, bind func(any) string) (string, bool, error) {
	tuples := make([]string, 0, len(values))
	for _, v := range values {
		s, ok := v.(string)
		if !ok {
			return "", false, nil
		}
		rt, err := domain.ParseResourceType(s)
		if err != nil {
			// Any unparseable element keeps the generic concatenated
			// IN path so the filter still compiles.
			return "", false, nil
		}
		tuples = append(tuples, fmt.Sprintf("(%s, %s)",
			bind(string(rt.ServiceName())), bind(rt.TypeName())))
	}
	return fmt.Sprintf("(er.service_name, er.type_name) IN (%s)", strings.Join(tuples, ", ")), true, nil
}

func inFullName(values []any, bind func(any) string) (string, bool, error) {
	tuples := make([]string, 0, len(values))
	for _, v := range values {
		s, ok := v.(string)
		if !ok {
			return "", false, nil
		}
		full := domain.FullResourceName(s)
		service := full.ServiceName()
		name := full.ResourceName()
		if service == "" || name == "" || !strings.HasPrefix(s, "//") {
			return "", false, nil
		}
		if _, err := domain.ParseResourceName(string(name)); err != nil {
			return "", false, nil
		}
		tuples = append(tuples, fmt.Sprintf("(%s, %s, %s)",
			bind(string(service)), bind(string(name.Collection())), bind(string(name.ID()))))
	}
	return fmt.Sprintf("(er.service_name, er.collection_name, er.resource_id) IN (%s)", strings.Join(tuples, ", ")), true, nil
}

// jsonTextField builds a chained ->/->> extraction from column
// through names -- the parsed dotted-path field names from a
// resource.spec.foo.bar/resource.inventory.observation.foo.bar
// selector -- and casts the result per want (see castByHint).
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
// matching want, so e.g. a numeric comparison compares numerically
// rather than lexically. TypeHintString and TypeHintUnknown need no
// cast: ->> already yields text.
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
// cast against rows the guard conjunct would otherwise exclude.
//
// pg_input_is_valid is a Postgres 17+ builtin (this project targets
// Postgres 18 uniformly); CASE is used rather than a boolean AND
// because Postgres does guarantee CASE only evaluates the selected
// branch for expressions that vary per row.
func safeJSONCast(expr, sqlType string) string {
	return fmt.Sprintf("(CASE WHEN pg_input_is_valid(%s, '%s') THEN (%s)::%s END)", expr, sqlType, expr, sqlType)
}
