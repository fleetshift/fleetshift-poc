package sqlite

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
//
// SQLite differences from the Postgres resolver (postgres/query_filter.go):
//
//   - JSON text columns use json_extract / ->> rather than JSONB
//     operators; there is no GIN, so label/condition equality stays on
//     the extract path (no @> containment rewrite).
//   - Numeric/boolean casts use CAST(... AS REAL/INTEGER) guarded by
//     typeof(...) = 'text' AND a regex match, because SQLite has no
//     pg_input_is_valid and will coerce garbage to 0 rather than
//     erroring -- which would incorrectly match numeric comparisons.
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
			return querysql.SQLExpr{SQL: "er.uid"}, nil
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
			// SQLite's ->> only accepts a string literal RHS, so
			// dynamic keys use json_extract. Quote the key so values
			// like "Ready" and hyphenated names stay one path segment.
			extract := jsonExtractKeySubfield("inv.conditions", keyPlaceholder, subfield)
			return castByHint(extract, want), nil
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

// labelField handles the common json_extract(<column>, '$."<key>")
// shape shared by resource.labels[...] and
// resource.inventory.labels[...]. key comes from the filter text (a
// CEL map-index string literal), so it is bound as a SQL parameter
// via bind rather than inlined. Unlike Postgres, there is no GIN
// containment rewrite -- equality stays on the extract path.
func labelField(column, key string, bind func(any) string, want querysql.TypeHint) querysql.SQLExpr {
	keyPlaceholder := bind(key)
	return castByHint(jsonExtractKey(column, keyPlaceholder), want)
}

// jsonExtractKey builds json_extract(column, '$."<bound-key>") with
// the key quoted so hyphenated labels (e.g. node-role) are not parsed
// as JSON-path arithmetic. The key placeholder is a bound parameter;
// embedded quotes/backslashes are escaped inside SQL.
func jsonExtractKey(column, keyPlaceholder string) string {
	return fmt.Sprintf(
		`json_extract(%s, '$."' || replace(replace(%s, '\', '\\'), '"', '\"') || '"')`,
		column, keyPlaceholder,
	)
}

// jsonExtractKeySubfield is jsonExtractKey plus a whitelist-validated
// identifier subfield (e.g. conditions["Ready"].status).
func jsonExtractKeySubfield(column, keyPlaceholder, subfield string) string {
	return fmt.Sprintf(
		`json_extract(%s, '$."' || replace(replace(%s, '\', '\\'), '"', '\"') || '".%s')`,
		column, keyPlaceholder, subfield,
	)
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
// SQLite 3.38+ supports the Postgres-compatible -> / ->> operators
// for literal path segments (modernc.org/sqlite ships 3.51.x).
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
// cast: ->> / json_extract already yield text (or NULL).
func castByHint(expr string, want querysql.TypeHint) querysql.SQLExpr {
	switch want {
	case querysql.TypeHintBool:
		return querysql.SQLExpr{SQL: safeJSONBoolCast(expr)}
	case querysql.TypeHintNumber:
		return querysql.SQLExpr{SQL: safeJSONNumberCast(expr)}
	default:
		return querysql.SQLExpr{SQL: expr}
	}
}

// safeJSONNumberCast casts a JSON-extracted SQL expression to REAL,
// guarded so a row whose value at this JSON path isn't actually
// numeric evaluates to SQL NULL (i.e. doesn't match) instead of
// SQLite's permissive CAST(... AS REAL) which turns garbage into 0
// and would incorrectly satisfy comparisons like > 4.
//
// SQLite's ->> returns INTEGER/REAL for JSON numbers (unlike Postgres
// jsonb ->> which always yields text), so the integer/real typeof
// branches cover the common case. Numeric-looking TEXT (e.g. a JSON
// string "8") is accepted to match Postgres's pg_input_is_valid
// behavior; anything with non-numeric characters becomes NULL.
//
// This guard is required even for fields gated by a
// resource_type == "..." conjunct (see hasResourceTypeGuard in
// querysql): SQLite gives no evaluation-order guarantee for a plain
// WHERE-clause AND, so it remains free to evaluate this cast against
// rows the guard conjunct would otherwise exclude.
//
// CASE is used rather than a boolean AND because SQLite does
// guarantee CASE only evaluates the selected branch.
func safeJSONNumberCast(expr string) string {
	// TEXT values are accepted only when they are themselves a JSON
	// integer/real literal (json_valid + json_type). That rejects
	// prefix-cast traps like "1e", "1.2.3", and "1-2" that a GLOB of
	// numeric characters would still allow through CAST(... AS REAL),
	// matching Postgres's pg_input_is_valid('numeric') intent more
	// closely than a character-class check.
	//
	// expr may contain numbered bind placeholders (?N). Repeating it
	// is safe because QuestionParams emits reusable ?N indexes, not
	// bare positional "?".
	return fmt.Sprintf(
		`(CASE WHEN typeof(%s) IN ('integer', 'real') THEN %s WHEN typeof(%s) = 'text' AND json_valid(%s) AND json_type(%s) IN ('integer', 'real') THEN CAST((%s) AS REAL) END)`,
		expr, expr, expr, expr, expr, expr,
	)
}

// safeJSONBoolCast maps JSON booleans to INTEGER 1/0 for comparison
// against bound Go bools. SQLite's ->> returns INTEGER 0/1 for JSON
// true/false; TEXT "true"/"false" (e.g. a JSON string) is also
// accepted. Non-boolean values become NULL so they don't match.
func safeJSONBoolCast(expr string) string {
	return fmt.Sprintf(
		`(CASE WHEN typeof(%s) = 'integer' AND (%s) IN (0, 1) THEN (%s) WHEN typeof(%s) = 'text' AND lower(%s) IN ('true', 'false') THEN CASE WHEN lower(%s) = 'true' THEN 1 ELSE 0 END END)`,
		expr, expr, expr, expr, expr, expr,
	)
}
