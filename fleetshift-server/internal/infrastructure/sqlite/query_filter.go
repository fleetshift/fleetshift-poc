package sqlite

import (
	"fmt"
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
// Public CEL fields match the QueryResources ProtoJSON response
// shape: envelope name, envelope resourceType, and fields under
// resource by their canonical JSON names. Top-level identity
// components and platform-only body fields are rejected.
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
	// SchemaProvider, if set, lets resource.spec.* and
	// resource.observation.* paths be validated against a real
	// protobuf descriptor when the filter's top-level resourceType
	// guard resolves to a type with one registered. See
	// [domain.QuerySchemaProvider]'s doc for the absence-of-schema
	// fallback behavior when this is nil or has nothing registered for
	// the guarded type.
	SchemaProvider domain.QuerySchemaProvider
}

var _ querysql.FieldResolver = queryFieldResolver{}

// conditionSubfields are the resource.conditions["Type"].* fields
// supported in filters; all are text-valued so no cast handling is
// needed for them. Subfield names match the ProtoJSON spelling.
var conditionSubfields = map[string]bool{
	"status":             true,
	"reason":             true,
	"message":            true,
	"lastTransitionTime": true,
}

// Resolve implements [querysql.FieldResolver].
func (r queryFieldResolver) Resolve(path querysql.FieldPath, want querysql.TypeHint, ctx querysql.ResolveContext) (querysql.SQLExpr, error) {
	segs := path.Segments
	if len(segs) == 0 {
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: empty field path", domain.ErrInvalidArgument)
	}
	if len(segs) == 1 {
		return r.resolveEnvelopeField(segs[0], want)
	}
	if segs[0] != "resource" {
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field expression", domain.ErrInvalidArgument)
	}
	return r.resolveResourceField(segs[1:], want, ctx)
}

func (r queryFieldResolver) resolveEnvelopeField(name string, want querysql.TypeHint) (querysql.SQLExpr, error) {
	switch name {
	case "name":
		sql := "'//' || er.service_name || '/' || er.collection_name || '/' || er.resource_id"
		if want == querysql.TypeHintTimestamp {
			return knownTimestamp(sql), nil
		}
		// Canonical full name. Equality / IN against well-formed
		// "//service/collection/id" literals special-case to
		// constituent-column predicates so the default-order index
		// can seek; other comparisons fall back to the expression.
		return querysql.SQLExpr{
			SQL:     sql,
			Compare: compareFullNameEquality,
			In:      inFullName,
		}, nil
	case "resourceType":
		sql := "er.service_name || '/' || er.type_name"
		if want == querysql.TypeHintTimestamp {
			return knownTimestamp(sql), nil
		}
		// Equality / IN against well-formed "service/Type" literals
		// special-case to service_name/type_name predicates so
		// idx_extension_resources_type_query_order can participate.
		return querysql.SQLExpr{
			SQL:     sql,
			Compare: compareResourceTypeEquality,
			In:      inResourceType,
		}, nil
	default:
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field %q", domain.ErrInvalidArgument, name)
	}
}

// resolveResourceField maps the segments following `resource` to a
// SQL expression against the er/erm/ri/f/inv aliases. Field names
// match the canonical ProtoJSON response spelling only.
func (r queryFieldResolver) resolveResourceField(segs []string, want querysql.TypeHint, ctx querysql.ResolveContext) (querysql.SQLExpr, error) {
	if len(segs) == 0 {
		return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field \"resource\"", domain.ErrInvalidArgument)
	}

	head, rest := segs[0], segs[1:]
	switch head {
	case "name":
		if len(rest) == 0 {
			// Body-level relative resource name
			// (collection_name/resource_id), distinct from the
			// envelope's full name.
			return stringOrTimestamp(want, "er.collection_name || '/' || er.resource_id"), nil
		}
	case "uid":
		if len(rest) == 0 {
			return stringOrTimestamp(want, "er.uid"), nil
		}
	case "labels":
		if len(rest) == 1 {
			return labelField("er.labels", rest[0], want, ctx.Bind), nil
		}
	case "intentVersion":
		if len(rest) == 0 {
			return stringOrTimestamp(want, "CAST(erm.current_version AS TEXT)"), nil
		}
	case "state":
		if len(rest) == 0 {
			return stringOrTimestamp(want, apiStateSQL), nil
		}
	case "pauseReason":
		if len(rest) == 0 {
			return stringOrTimestamp(want, "f.pause_reason"), nil
		}
	case "generation":
		if len(rest) == 0 {
			return stringOrTimestamp(want, "CAST(f.generation AS TEXT)"), nil
		}
	case "spec":
		if len(rest) > 0 {
			names, err := r.validateSpecPath(ctx, rest)
			if err != nil {
				return querysql.SQLExpr{}, err
			}
			return jsonTextField("ri.spec", names, want, ctx.Bind), nil
		}
	case "localLabels":
		if len(rest) == 1 {
			return labelField("inv.labels", rest[0], want, ctx.Bind), nil
		}
	case "conditions":
		if len(rest) == 2 && conditionSubfields[rest[1]] {
			key := rest[0]
			subfield := rest[1]
			keyPlaceholder := ctx.Bind(key)
			// Subfield is whitelist-validated; key is bound. Quote the
			// key so hyphenated / $-prefixed names stay literal.
			extract := jsonExtractKeySubfield("inv.conditions", keyPlaceholder, subfield)
			if want == querysql.TypeHintTimestamp {
				if subfield != "lastTransitionTime" {
					// Non-timestamp subfields error at conversion time
					// per row when non-RFC3339.
					path := fmt.Sprintf(`%s || '.%s'`, jsonQuotedPathExpr(keyPlaceholder), subfield)
					return dynamicTimestamp("inv.conditions", path), nil
				}
				// timestamp() reads the fixed-width storage sibling.
				normExtract := jsonExtractKeySubfield(
					"inv.conditions", keyPlaceholder, conditionLastTransitionTimeNormJSONKey,
				)
				return canonicalTimestampText(normExtract), nil
			}
			return knownStringField(extract), nil
		}
	case "observation":
		if len(rest) > 0 {
			names, err := r.validateObservationPath(ctx, rest)
			if err != nil {
				return querysql.SQLExpr{}, err
			}
			return jsonTextField("inv.observation", names, want, ctx.Bind), nil
		}
	case "localUpdateTime":
		if len(rest) == 0 {
			if want == querysql.TypeHintTimestamp {
				return knownTimestamp("inv.observed_at"), nil
			}
			return knownTimestampString("inv.observed_at"), nil
		}
	case "indexUpdateTime":
		if len(rest) == 0 {
			if want == querysql.TypeHintTimestamp {
				return knownTimestamp("inv.updated_at"), nil
			}
			return knownTimestampString("inv.updated_at"), nil
		}
	}
	return querysql.SQLExpr{}, fmt.Errorf("filter: %w: unsupported field \"resource.%s\"", domain.ErrInvalidArgument, strings.Join(segs, "."))
}

// apiStateSQL projects fulfillments.state (lowercase storage) to the
// ProtoJSON enum name exposed on the query result.
const apiStateSQL = `(CASE f.state WHEN 'creating' THEN 'CREATING' WHEN 'active' THEN 'ACTIVE' WHEN 'deleting' THEN 'DELETING' WHEN 'failed' THEN 'FAILED' ELSE f.state END)`

func stringOrTimestamp(want querysql.TypeHint, textSQL string) querysql.SQLExpr {
	if want == querysql.TypeHintTimestamp {
		return knownTimestamp(textSQL)
	}
	return knownStringField(textSQL)
}

// labelField handles the common json_extract(<column>, '$."<key>")
// shape shared by resource.labels[...] and
// resource.localLabels[...]. Labels are known string map values.
// key comes from the filter text, so it is bound as a SQL parameter.
// timestamp() conversion uses cel_ts_norm over the extracted text.
func labelField(column, key string, want querysql.TypeHint, bind func(any) string) querysql.SQLExpr {
	keyPlaceholder := bind(key)
	extract := jsonExtractKey(column, keyPlaceholder)
	if want == querysql.TypeHintTimestamp {
		return knownTimestamp(extract)
	}
	return knownStringField(extract)
}

// jsonExtractKey builds json_extract(column, '$."<bound-key>") with
// the key quoted so hyphenated / $-prefixed labels are literal object
// keys, not JSONPath. The key placeholder is a bound parameter;
// embedded quotes/backslashes are escaped inside SQL.
func jsonExtractKey(column, keyPlaceholder string) string {
	return fmt.Sprintf("json_extract(%s, %s)", column, jsonQuotedPathExpr(keyPlaceholder))
}

// jsonExtractKeySubfield is jsonExtractKey plus a whitelist-validated
// identifier subfield (e.g. conditions["Ready"].status).
func jsonExtractKeySubfield(column, keyPlaceholder, subfield string) string {
	return fmt.Sprintf(
		`json_extract(%s, %s || '.%s')`,
		column, jsonQuotedPathExpr(keyPlaceholder), subfield,
	)
}

// jsonQuotedPathExpr builds a SQL expression that evaluates to the
// JSON path `$."<key>"` with key taken from a bound parameter,
// escaping quotes/backslashes so the key is always a literal object
// member name (including keys that begin with `$`).
func jsonQuotedPathExpr(keyPlaceholder string) string {
	return fmt.Sprintf(
		`'$."' || replace(replace(%s, '\', '\\'), '"', '\"') || '"'`,
		keyPlaceholder,
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

// jsonTextField extracts a JSON path with literal object keys (quoted
// json_extract path segments). Bound keys beginning with `$` stay
// literal member names. Comparisons use DynamicJSON so incompatible
// present types follow CEL equality (string "5" != 5 → true).
// timestamp() conversion uses DynamicTimestamp over the same path.
func jsonTextField(column string, names []string, want querysql.TypeHint, bind func(any) string) querysql.SQLExpr {
	path := jsonQuotedPathChain(names, bind)
	if want == querysql.TypeHintTimestamp {
		return dynamicTimestamp(column, path)
	}
	return dynamicJSON(column, path)
}

// jsonQuotedPathChain builds `$."a"."b"` as a SQL expression over
// bound key parameters.
func jsonQuotedPathChain(names []string, bind func(any) string) string {
	var sb strings.Builder
	sb.WriteString("'$'")
	for _, n := range names {
		ph := bind(n)
		fmt.Fprintf(&sb, ` || '."' || replace(replace(%s, '\', '\\'), '"', '\"') || '"'`, ph)
	}
	return sb.String()
}

// safeJSONNumberCast is retained for unit tests that exercise the
// historic extract-then-cast shape against a plain column.
func safeJSONNumberCast(expr string) string {
	return fmt.Sprintf(
		`(CASE WHEN typeof(%s) IN ('integer', 'real') THEN %s END)`,
		expr, expr,
	)
}

// safeJSONBoolCast is retained for unit tests.
func safeJSONBoolCast(expr string) string {
	return fmt.Sprintf(
		`(CASE WHEN typeof(%s) = 'integer' AND (%s) IN (0, 1) THEN (%s) END)`,
		expr, expr, expr,
	)
}
