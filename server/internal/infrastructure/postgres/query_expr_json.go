package postgres

import (
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// dynamicJSONB returns an [querysql.SQLExpr] for a jsonb path that may
// hold any JSON value (spec/observation/labels/Struct). Equality and
// inequality compare typed jsonb values via to_jsonb so string "5"
// differs from number 5, JSON null is present, and a missing path
// (SQL NULL) stays a non-match for every operator.
func dynamicJSONB(jsonbExpr string) querysql.SQLExpr {
	return querysql.SQLExpr{
		SQL:        jsonbExpr,
		Compare:    dynamicJSONBCompare(jsonbExpr),
		In:         dynamicJSONBIn(jsonbExpr),
		StartsWith: dynamicJSONBStartsWith(jsonbExpr),
	}
}

func dynamicJSONBCompare(jsonbExpr string) func(querysql.ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		pgType, ok := postgresLitType(lit)
		if !ok {
			return "", false, fmt.Errorf("filter: %w: unsupported literal type %T", domain.ErrInvalidArgument, lit)
		}
		bound := bind(lit)
		toJSON := fmt.Sprintf("to_jsonb(%s::%s)", bound, pgType)
		switch op {
		case querysql.OpEqual:
			return fmt.Sprintf("%s = %s", jsonbExpr, toJSON), true, nil
		case querysql.OpNotEqual:
			return fmt.Sprintf("%s <> %s", jsonbExpr, toJSON), true, nil
		case querysql.OpLess, querysql.OpLessEqual, querysql.OpGreater, querysql.OpGreaterEqual:
			sqlOp, _ := comparisonSQLOp(op)
			switch lit.(type) {
			case int64, uint64, float64:
				return fmt.Sprintf(
					`(CASE WHEN jsonb_typeof(%s) = 'number' THEN (%s)::text::numeric %s %s::numeric END)`,
					jsonbExpr, jsonbExpr, sqlOp, bound,
				), true, nil
			case string:
				return fmt.Sprintf(
					`(CASE WHEN jsonb_typeof(%s) = 'string' THEN ((%s) #>> '{}') COLLATE "C" %s (%s) COLLATE "C" END)`,
					jsonbExpr, jsonbExpr, sqlOp, bound,
				), true, nil
			default:
				return "", false, fmt.Errorf("filter: %w: ordered comparison is not supported for this operand type", domain.ErrInvalidArgument)
			}
		default:
			return "", false, nil
		}
	}
}

func dynamicJSONBIn(jsonbExpr string) func([]any, func(any) string) (string, bool, error) {
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

func dynamicJSONBStartsWith(jsonbExpr string) func(string, func(any) string) (string, bool, error) {
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
