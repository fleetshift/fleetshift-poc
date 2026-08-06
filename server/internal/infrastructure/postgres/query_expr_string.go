package postgres

import (
	"fmt"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// knownStringField returns an [querysql.SQLExpr] for a ProtoJSON/CEL
// string column. Compatible string operands use normal string
// semantics with COLLATE "C" (CEL UTF-8 code-point order);
// incompatible == is false, incompatible != is "column IS NOT NULL",
// and ordered cross-type comparisons fail closed.
func knownStringField(column string) querysql.SQLExpr {
	return querysql.SQLExpr{
		SQL:        column,
		Compare:    knownStringCompare(column),
		In:         knownStringIn(column),
		StartsWith: knownStringStartsWith(column),
	}
}

const collateC = `"C"`

func collateExpr(expr string) string {
	return fmt.Sprintf("(%s) COLLATE %s", expr, collateC)
}

func knownStringCompare(column string) func(querysql.ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		s, ok := lit.(string)
		if !ok {
			switch op {
			case querysql.OpEqual:
				return "FALSE", true, nil
			case querysql.OpNotEqual:
				return fmt.Sprintf("(%s IS NOT NULL)", column), true, nil
			default:
				return "", false, fmt.Errorf("filter: %w: ordered comparison requires a string operand", domain.ErrInvalidArgument)
			}
		}
		sqlOp, ok := comparisonSQLOp(op)
		if !ok {
			return "", false, nil
		}
		return fmt.Sprintf("%s %s %s", collateExpr(column), sqlOp, collateExpr(bind(s))), true, nil
	}
}

func knownStringIn(column string) func([]any, func(any) string) (string, bool, error) {
	return func(values []any, bind func(any) string) (string, bool, error) {
		placeholders := make([]string, 0, len(values))
		for _, v := range values {
			s, ok := v.(string)
			if !ok {
				return "FALSE", true, nil
			}
			placeholders = append(placeholders, collateExpr(bind(s)))
		}
		return fmt.Sprintf("%s IN (%s)", collateExpr(column), strings.Join(placeholders, ", ")), true, nil
	}
}

func knownStringStartsWith(column string) func(string, func(any) string) (string, bool, error) {
	return func(prefix string, bind func(any) string) (string, bool, error) {
		pattern := escapeLikePattern(prefix) + "%"
		return fmt.Sprintf("%s LIKE %s ESCAPE '\\'",
			collateExpr(column),
			collateExpr(bind(pattern)),
		), true, nil
	}
}

func comparisonSQLOp(op querysql.ComparisonOperator) (string, bool) {
	switch op {
	case querysql.OpEqual:
		return "=", true
	case querysql.OpNotEqual:
		return "!=", true
	case querysql.OpLess:
		return "<", true
	case querysql.OpLessEqual:
		return "<=", true
	case querysql.OpGreater:
		return ">", true
	case querysql.OpGreaterEqual:
		return ">=", true
	default:
		return "", false
	}
}

func escapeLikePattern(s string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return replacer.Replace(s)
}

func joinComma(parts []string) string {
	return strings.Join(parts, ", ")
}
