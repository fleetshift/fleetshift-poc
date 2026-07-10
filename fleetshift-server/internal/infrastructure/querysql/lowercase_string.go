package querysql

import (
	"fmt"
	"strings"
)

// LowercaseStringCompare returns a [SQLExpr.Compare] hook that binds
// string equality/inequality literals lowercased. Use for fields
// whose domain treats the value as case-insensitive or stores a
// normalized lowercase form (e.g. fulfillment state), so filter
// literals match regardless of API spelling case. Non-string
// literals and ordered comparisons fall back to the generic path.
func LowercaseStringCompare(column string) func(op ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
	return func(op ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		if op != OpEqual && op != OpNotEqual {
			return "", false, nil
		}
		s, ok := lit.(string)
		if !ok {
			return "", false, nil
		}
		sqlOp := "="
		if op == OpNotEqual {
			sqlOp = "!="
		}
		return fmt.Sprintf("%s %s %s", column, sqlOp, bind(strings.ToLower(s))), true, nil
	}
}

// LowercaseStringIn returns a [SQLExpr.In] hook that binds each string
// list element lowercased, matching [LowercaseStringCompare].
func LowercaseStringIn(column string) func(values []any, bind func(any) string) (string, bool, error) {
	return func(values []any, bind func(any) string) (string, bool, error) {
		placeholders := make([]string, 0, len(values))
		for _, v := range values {
			s, ok := v.(string)
			if !ok {
				return "", false, nil
			}
			placeholders = append(placeholders, bind(strings.ToLower(s)))
		}
		return fmt.Sprintf("%s IN (%s)", column, strings.Join(placeholders, ", ")), true, nil
	}
}

// LowercaseStringStartsWith returns a [SQLExpr.StartsWith] hook that
// binds a lowercased, LIKE-escaped prefix pattern. Same domain rule
// as [LowercaseStringCompare]: case-fold filter literals for fields
// that are not case-sensitive in the domain.
func LowercaseStringStartsWith(column string) func(prefix string, bind func(any) string) (string, bool, error) {
	return func(prefix string, bind func(any) string) (string, bool, error) {
		pattern := escapeLikePattern(strings.ToLower(prefix)) + "%"
		return fmt.Sprintf("%s LIKE %s ESCAPE '\\'", column, bind(pattern)), true, nil
	}
}
