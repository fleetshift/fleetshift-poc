package postgres

import (
	"fmt"
	"strings"
	"time"

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
// Schema validation for resource.spec.*/resource.observation.* is
// owned by [querysql.ResolveContext] (via the compiler's Schemas);
// this resolver only maps validated paths to Postgres JSONB SQL.
type queryFieldResolver struct{}

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
			return textTimestamp(sql), nil
		}
		// Canonical full name. Equality / IN against well-formed
		// "//service/collection/id" literals special-case to
		// constituent-column predicates so the default-order index
		// can seek; other comparisons fall back to KnownStringField
		// (COLLATE "C" for CEL lexical order).
		base := knownStringField(sql)
		return querysql.SQLExpr{
			SQL: sql,
			Compare: func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
				if sql, handled, err := compareFullNameEquality(op, lit, bind); handled || err != nil {
					return sql, handled, err
				}
				return base.Compare(op, lit, bind)
			},
			In: func(values []any, bind func(any) string) (string, bool, error) {
				if sql, handled, err := inFullName(values, bind); handled || err != nil {
					return sql, handled, err
				}
				return base.In(values, bind)
			},
			StartsWith: base.StartsWith,
		}, nil
	case "resourceType":
		sql := "er.service_name || '/' || er.type_name"
		if want == querysql.TypeHintTimestamp {
			return textTimestamp(sql), nil
		}
		// Equality / IN against well-formed "service/Type" literals
		// special-case to service_name/type_name predicates so
		// idx_extension_resources_type_query_order can participate.
		base := knownStringField(sql)
		return querysql.SQLExpr{
			SQL: sql,
			Compare: func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
				if sql, handled, err := compareResourceTypeEquality(op, lit, bind); handled || err != nil {
					return sql, handled, err
				}
				return base.Compare(op, lit, bind)
			},
			In: func(values []any, bind func(any) string) (string, bool, error) {
				if sql, handled, err := inResourceType(values, bind); handled || err != nil {
					return sql, handled, err
				}
				return base.In(values, bind)
			},
			StartsWith: base.StartsWith,
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
			return stringOrTimestamp(want, "er.uid::text"), nil
		}
	case "labels":
		if len(rest) == 1 {
			return labelField("er.labels", rest[0], want, ctx.Bind), nil
		}
	case "intentVersion":
		if len(rest) == 0 {
			return stringOrTimestamp(want, "erm.current_version::text"), nil
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
			return stringOrTimestamp(want, "f.generation::text"), nil
		}
	case "spec":
		if len(rest) > 0 {
			names, err := ctx.ValidateSpecPath(rest)
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
			jsonb := fmt.Sprintf("inv.conditions -> %s -> '%s'", keyPlaceholder, subfield)
			if want == querysql.TypeHintTimestamp {
				if subfield != "lastTransitionTime" {
					return dynamicTimestamp(jsonb), nil
				}
				// timestamp() reads the fixed-width storage sibling.
				// Equality uses GIN containment; ordered/!=/IN compare
				// the norm text directly (no cel_ts_norm).
				normKey := conditionLastTransitionTimeNormJSONKey
				text := fmt.Sprintf("inv.conditions -> %s ->> '%s'", keyPlaceholder, normKey)
				expr := canonicalTimestampText(text)
				baseCompare := expr.Compare
				ginEqual := conditionStringCompare(keyPlaceholder, normKey, text)
				expr.Compare = func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
					if op == querysql.OpEqual {
						if t, ok := lit.(time.Time); ok {
							return ginEqual(op, formatTimestampNorm(t), bind)
						}
					}
					return baseCompare(op, lit, bind)
				}
				return expr, nil
			}
			text := fmt.Sprintf("inv.conditions -> %s ->> '%s'", keyPlaceholder, subfield)
			expr := knownStringField(text)
			expr.Compare = conditionStringCompare(keyPlaceholder, subfield, text)
			return expr, nil
		}
	case "observation":
		if len(rest) > 0 {
			names, err := ctx.ValidateObservationPath(rest)
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
		return textTimestamp(textSQL)
	}
	return knownStringField(textSQL)
}

// labelField handles the common `<column> -> <key>` JSONB shape shared
// by resource.labels[...] and resource.localLabels[...]. Labels are
// known string map values. String equality is rewritten to JSONB
// containment so the GIN indexes can participate; other operators use
// KnownStringField semantics (including incompatible != → IS NOT NULL).
// timestamp() conversion uses cel_ts_norm over the extracted text.
func labelField(column, key string, want querysql.TypeHint, bind func(any) string) querysql.SQLExpr {
	keyPlaceholder := bind(key)
	text := fmt.Sprintf("(%s ->> %s)", column, keyPlaceholder)
	if want == querysql.TypeHintTimestamp {
		return textTimestamp(text)
	}
	expr := knownStringField(text)
	expr.Compare = labelStringCompare(column, keyPlaceholder, text)
	return expr
}

func labelStringCompare(column, keyPlaceholder, textSQL string) func(querysql.ComparisonOperator, any, func(any) string) (string, bool, error) {
	base := knownStringField(textSQL).Compare
	return func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		if op == querysql.OpEqual {
			if value, ok := lit.(string); ok {
				// Reuse the key placeholder already bound during Resolve.
				return fmt.Sprintf("%s @> jsonb_build_object(%s::text, %s::text)", column, keyPlaceholder, bind(value)), true, nil
			}
		}
		return base(op, lit, bind)
	}
}

func conditionStringCompare(typePlaceholder, subfield, textSQL string) func(querysql.ComparisonOperator, any, func(any) string) (string, bool, error) {
	base := knownStringField(textSQL).Compare
	return func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		if op == querysql.OpEqual {
			if value, ok := lit.(string); ok {
				return fmt.Sprintf(
					"inv.conditions @> jsonb_build_object(%s::text, jsonb_build_object(%s::text, %s::text))",
					typePlaceholder, bind(subfield), bind(value),
				), true, nil
			}
		}
		return base(op, lit, bind)
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

// jsonTextField builds a chained JSONB -> path. Comparisons use
// DynamicJSON so incompatible present types follow CEL equality
// (string "5" != 5 → true) while missing paths stay non-matches.
// timestamp() conversion uses DynamicTimestamp over the same path.
func jsonTextField(column string, names []string, want querysql.TypeHint, bind func(any) string) querysql.SQLExpr {
	var sb strings.Builder
	sb.WriteString(column)
	for _, n := range names {
		fmt.Fprintf(&sb, " -> %s", bind(n))
	}
	if want == querysql.TypeHintTimestamp {
		return dynamicTimestamp(sb.String())
	}
	return dynamicJSONB(sb.String())
}

// safeJSONCast is retained for documentation of the historic
// pg_input_is_valid approach.
func safeJSONCast(expr, sqlType string) string {
	return fmt.Sprintf("(CASE WHEN pg_input_is_valid(%s, '%s') THEN (%s)::%s END)", expr, sqlType, expr, sqlType)
}
