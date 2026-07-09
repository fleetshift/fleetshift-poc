package querysql

import (
	"fmt"
	"strings"
)

// LowercaseStringCompare returns a [SQLExpr.Compare] hook that binds
// string equality/inequality literals lowercased. Used for fields
// whose stored values are lowercase (e.g. fulfillment state) while
// API responses may expose uppercase spellings (e.g. proto enums).
// Non-string literals and ordered comparisons fall back to the
// generic path.
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
