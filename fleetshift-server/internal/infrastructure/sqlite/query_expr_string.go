package sqlite

import (
	"fmt"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// knownStringField returns an [querysql.SQLExpr] for a ProtoJSON/CEL
// string column. Compatible string operands use normal string
// semantics; incompatible == is false (0), incompatible != is
// "column IS NOT NULL", and ordered cross-type comparisons fail closed.
func knownStringField(column string) querysql.SQLExpr {
	return querysql.SQLExpr{
		SQL:     column,
		Compare: knownStringCompare(column),
		In:      knownStringIn(column),
	}
}

func knownStringCompare(column string) func(querysql.ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		s, ok := lit.(string)
		if !ok {
			switch op {
			case querysql.OpEqual:
				return "0", true, nil
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
		return fmt.Sprintf("%s %s %s", column, sqlOp, bind(s)), true, nil
	}
}

func knownStringIn(column string) func([]any, func(any) string) (string, bool, error) {
	return func(values []any, bind func(any) string) (string, bool, error) {
		placeholders := make([]string, 0, len(values))
		for _, v := range values {
			s, ok := v.(string)
			if !ok {
				continue
			}
			placeholders = append(placeholders, bind(s))
		}
		if len(placeholders) == 0 {
			return "0", true, nil
		}
		return fmt.Sprintf("%s IN (%s)", column, strings.Join(placeholders, ", ")), true, nil
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

func joinOr(parts []string) string {
	return strings.Join(parts, " OR ")
}
