package sqlite

import (
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// dynamicJSON returns an [querysql.SQLExpr] for a json_extract path that
// may hold any JSON value. json_type distinguishes missing (NULL),
// present null ('null'), and typed values so != is true for
// incompatible present values and a non-match for missing paths.
func dynamicJSON(column, path string) querysql.SQLExpr {
	typeOf := fmt.Sprintf("json_type(%s, %s)", column, path)
	extract := fmt.Sprintf("json_extract(%s, %s)", column, path)
	return querysql.SQLExpr{
		SQL:        extract,
		Compare:    dynamicJSONCompare(typeOf, extract),
		In:         dynamicJSONIn(typeOf, extract),
		StartsWith: dynamicJSONStartsWith(typeOf, extract),
	}
}

func dynamicJSONCompare(typeOf, extract string) func(querysql.ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		bound := bind(lit)
		switch op {
		case querysql.OpEqual:
			return dynamicJSONEqual(typeOf, extract, lit, bound)
		case querysql.OpNotEqual:
			return dynamicJSONNotEqual(typeOf, extract, lit, bound)
		case querysql.OpLess, querysql.OpLessEqual, querysql.OpGreater, querysql.OpGreaterEqual:
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

func dynamicJSONEqual(typeOf, extract string, lit any, bound string) (string, bool, error) {
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

func dynamicJSONNotEqual(typeOf, extract string, lit any, bound string) (string, bool, error) {
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

func dynamicJSONIn(typeOf, extract string) func([]any, func(any) string) (string, bool, error) {
	return func(values []any, bind func(any) string) (string, bool, error) {
		if len(values) == 0 {
			return fmt.Sprintf("(%s IS NOT NULL AND 0)", extract), true, nil
		}
		parts := make([]string, len(values))
		for i, v := range values {
			b := bind(v)
			eq, _, err := dynamicJSONEqual(typeOf, extract, v, b)
			if err != nil {
				return "", false, err
			}
			parts[i] = "(" + eq + ")"
		}
		return joinOr(parts), true, nil
	}
}

func dynamicJSONStartsWith(typeOf, extract string) func(string, func(any) string) (string, bool, error) {
	return func(prefix string, bind func(any) string) (string, bool, error) {
		pattern := escapeLikePattern(prefix) + "%"
		return fmt.Sprintf(
			`(CASE WHEN %s = 'text' THEN %s LIKE %s ESCAPE '\' END)`,
			typeOf, extract, bind(pattern),
		), true, nil
	}
}
