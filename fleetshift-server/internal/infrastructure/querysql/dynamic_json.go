package querysql

import (
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// PostgresDynamicJSONB returns an [SQLExpr] for a jsonb path that may
// hold any JSON value (spec/observation/labels/Struct). Equality and
// inequality compare typed jsonb values via to_jsonb so string "5"
// differs from number 5, JSON null is present, and a missing path
// (SQL NULL) stays a non-match for every operator.
func PostgresDynamicJSONB(jsonbExpr string) SQLExpr {
	return SQLExpr{
		SQL:        jsonbExpr,
		Compare:    postgresDynamicJSONBCompare(jsonbExpr),
		In:         postgresDynamicJSONBIn(jsonbExpr),
		StartsWith: postgresDynamicJSONBStartsWith(jsonbExpr),
	}
}

func postgresDynamicJSONBCompare(jsonbExpr string) func(ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		pgType, ok := postgresLitType(lit)
		if !ok {
			return "", false, fmt.Errorf("filter: %w: unsupported literal type %T", domain.ErrInvalidArgument, lit)
		}
		bound := bind(lit)
		toJSON := fmt.Sprintf("to_jsonb(%s::%s)", bound, pgType)
		switch op {
		case OpEqual:
			return fmt.Sprintf("%s = %s", jsonbExpr, toJSON), true, nil
		case OpNotEqual:
			// Missing path → SQL NULL → non-match.
			// Present incompatible / null / unequal → true/false via <>.
			return fmt.Sprintf("%s <> %s", jsonbExpr, toJSON), true, nil
		case OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
			sqlOp, _ := comparisonSQLOp(op)
			switch lit.(type) {
			case int64, uint64, float64:
				return fmt.Sprintf(
					`(CASE WHEN jsonb_typeof(%s) = 'number' THEN (%s)::text::numeric %s %s::numeric END)`,
					jsonbExpr, jsonbExpr, sqlOp, bound,
				), true, nil
			case string:
				// COLLATE "C" matches CEL UTF-8 code-point order.
				return fmt.Sprintf(
					`(CASE WHEN jsonb_typeof(%s) = 'string' THEN ((%s) #>> '{}') COLLATE "C" %s (%s) COLLATE "C" END)`,
					jsonbExpr, jsonbExpr, sqlOp, bound,
				), true, nil
			default:
				// bool ordered comparisons are unsupported in CEL.
				return "", false, fmt.Errorf("filter: %w: ordered comparison is not supported for this operand type", domain.ErrInvalidArgument)
			}
		default:
			return "", false, nil
		}
	}
}

func postgresDynamicJSONBIn(jsonbExpr string) func([]any, func(any) string) (string, bool, error) {
	return func(values []any, bind func(any) string) (string, bool, error) {
		if len(values) == 0 {
			return fmt.Sprintf("(%s IS NOT NULL AND FALSE)", jsonbExpr), true, nil
		}
		pgType, ok := postgresLitType(values[0])
		if !ok {
			return "", false, fmt.Errorf("filter: %w: unsupported literal type %T", domain.ErrInvalidArgument, values[0])
		}
		parts := make([]string, len(values))
		for i, v := range values {
			parts[i] = fmt.Sprintf("to_jsonb(%s::%s)", bind(v), pgType)
		}
		return fmt.Sprintf("%s IN (%s)", jsonbExpr, joinComma(parts)), true, nil
	}
}

func postgresDynamicJSONBStartsWith(jsonbExpr string) func(string, func(any) string) (string, bool, error) {
	return func(prefix string, bind func(any) string) (string, bool, error) {
		pattern := escapeLikePattern(prefix) + "%"
		return fmt.Sprintf(
			`(CASE WHEN jsonb_typeof(%s) = 'string' THEN ((%s) #>> '{}') COLLATE "C" LIKE (%s) COLLATE "C" ESCAPE '\' END)`,
			jsonbExpr, jsonbExpr, bind(pattern),
		), true, nil
	}
}

func postgresLitType(lit any) (string, bool) {
	switch lit.(type) {
	case string:
		return "text", true
	case bool:
		return "boolean", true
	case int64, uint64, float64:
		return "numeric", true
	default:
		return "", false
	}
}

// SQLiteDynamicJSON returns an [SQLExpr] for a json_extract path that
// may hold any JSON value. json_type distinguishes missing (NULL),
// present null ('null'), and typed values so != is true for
// incompatible present values and a non-match for missing paths.
func SQLiteDynamicJSON(column, path string) SQLExpr {
	typeOf := fmt.Sprintf("json_type(%s, %s)", column, path)
	extract := fmt.Sprintf("json_extract(%s, %s)", column, path)
	return SQLExpr{
		SQL:        extract,
		Compare:    sqliteDynamicJSONCompare(typeOf, extract),
		In:         sqliteDynamicJSONIn(typeOf, extract),
		StartsWith: sqliteDynamicJSONStartsWith(typeOf, extract),
	}
}

func sqliteDynamicJSONCompare(typeOf, extract string) func(ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		bound := bind(lit)
		switch op {
		case OpEqual:
			return sqliteDynamicJSONEqual(typeOf, extract, lit, bound)
		case OpNotEqual:
			return sqliteDynamicJSONNotEqual(typeOf, extract, lit, bound)
		case OpLess, OpLessEqual, OpGreater, OpGreaterEqual:
			sqlOp, _ := comparisonSQLOp(op)
			switch lit.(type) {
			case int64, uint64, float64:
				return fmt.Sprintf(
					`(CASE WHEN %s IN ('integer', 'real') THEN %s %s %s END)`,
					typeOf, extract, sqlOp, bound,
				), true, nil
			case string:
				return fmt.Sprintf(
					`(CASE WHEN %s = 'text' THEN %s %s %s END)`,
					typeOf, extract, sqlOp, bound,
				), true, nil
			default:
				return "", false, fmt.Errorf("filter: %w: ordered comparison is not supported for this operand type", domain.ErrInvalidArgument)
			}
		default:
			return "", false, nil
		}
	}
}

func sqliteDynamicJSONEqual(typeOf, extract string, lit any, bound string) (string, bool, error) {
	switch lit.(type) {
	case string:
		return fmt.Sprintf(
			`(CASE WHEN %s IS NULL THEN NULL WHEN %s = 'text' THEN %s = %s ELSE 0 END)`,
			typeOf, typeOf, extract, bound,
		), true, nil
	case bool:
		want := "false"
		if lit.(bool) {
			want = "true"
		}
		return fmt.Sprintf(
			`(CASE WHEN %s IS NULL THEN NULL WHEN %s = %q THEN 1 ELSE 0 END)`,
			typeOf, typeOf, want,
		), true, nil
	case int64, uint64, float64:
		return fmt.Sprintf(
			`(CASE WHEN %s IS NULL THEN NULL WHEN %s IN ('integer', 'real') THEN %s = %s ELSE 0 END)`,
			typeOf, typeOf, extract, bound,
		), true, nil
	default:
		return "", false, fmt.Errorf("filter: %w: unsupported literal type %T", domain.ErrInvalidArgument, lit)
	}
}

func sqliteDynamicJSONNotEqual(typeOf, extract string, lit any, bound string) (string, bool, error) {
	// Missing (json_type NULL) → SQL NULL → non-match.
	// Present compatible → normal != .
	// Present incompatible or JSON null → true (1).
	switch lit.(type) {
	case string:
		return fmt.Sprintf(
			`(CASE WHEN %s IS NULL THEN NULL WHEN %s = 'text' THEN %s != %s ELSE 1 END)`,
			typeOf, typeOf, extract, bound,
		), true, nil
	case bool:
		want := "false"
		if lit.(bool) {
			want = "true"
		}
		return fmt.Sprintf(
			`(CASE WHEN %s IS NULL THEN NULL WHEN %s = %q THEN 0 ELSE 1 END)`,
			typeOf, typeOf, want,
		), true, nil
	case int64, uint64, float64:
		return fmt.Sprintf(
			`(CASE WHEN %s IS NULL THEN NULL WHEN %s IN ('integer', 'real') THEN %s != %s ELSE 1 END)`,
			typeOf, typeOf, extract, bound,
		), true, nil
	default:
		return "", false, fmt.Errorf("filter: %w: unsupported literal type %T", domain.ErrInvalidArgument, lit)
	}
}

func sqliteDynamicJSONIn(typeOf, extract string) func([]any, func(any) string) (string, bool, error) {
	return func(values []any, bind func(any) string) (string, bool, error) {
		if len(values) == 0 {
			return fmt.Sprintf("(%s IS NOT NULL AND 0)", extract), true, nil
		}
		// Membership is a disjunction of type-preserving equalities.
		parts := make([]string, len(values))
		for i, v := range values {
			b := bind(v)
			eq, _, err := sqliteDynamicJSONEqual(typeOf, extract, v, b)
			if err != nil {
				return "", false, err
			}
			parts[i] = "(" + eq + ")"
		}
		return joinOr(parts), true, nil
	}
}

func sqliteDynamicJSONStartsWith(typeOf, extract string) func(string, func(any) string) (string, bool, error) {
	return func(prefix string, bind func(any) string) (string, bool, error) {
		pattern := escapeLikePattern(prefix) + "%"
		return fmt.Sprintf(
			`(CASE WHEN %s = 'text' THEN %s LIKE %s ESCAPE '\' END)`,
			typeOf, extract, bind(pattern),
		), true, nil
	}
}

func joinComma(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += ", "
		}
		out += p
	}
	return out
}

func joinOr(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += " OR "
		}
		out += p
	}
	return out
}
