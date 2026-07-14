package querysql

import (
	"fmt"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// BoolLit is the SQL spelling of boolean constants for a dialect
// (Postgres TRUE/FALSE, SQLite 1/0).
type BoolLit struct {
	True  string
	False string
}

// PostgresBool and SQLiteBool are the dialect boolean literals used
// by known-scalar Compare hooks.
var (
	PostgresBool = BoolLit{True: "TRUE", False: "FALSE"}
	SQLiteBool   = BoolLit{True: "1", False: "0"}
)

// StringFieldOption configures [KnownStringField].
type StringFieldOption func(*stringFieldConfig)

type stringFieldConfig struct {
	// Collate is a SQL collation clause including quotes when needed,
	// e.g. `"C"`. Empty means the database default. Postgres filters
	// use COLLATE "C" so ordering matches CEL's UTF-8 code-point order
	// regardless of the cluster's libc locale.
	Collate string
}

// WithCollate sets the SQL collation for string comparisons and
// prefix matches (for example `"C"` on Postgres).
func WithCollate(collation string) StringFieldOption {
	return func(c *stringFieldConfig) { c.Collate = collation }
}

// KnownStringField returns an [SQLExpr] for a ProtoJSON/CEL string
// column. Compatible string operands use normal string semantics;
// incompatible == is false, incompatible != is "column IS NOT NULL"
// (present value differs; missing stays a non-match), and ordered
// cross-type comparisons fail closed.
func KnownStringField(column string, bools BoolLit, opts ...StringFieldOption) SQLExpr {
	cfg := stringFieldConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return SQLExpr{
		SQL:        column,
		Compare:    knownStringCompare(column, bools, cfg.Collate),
		In:         knownStringIn(column, bools, cfg.Collate),
		StartsWith: knownStringStartsWith(column, cfg.Collate),
	}
}

func collateExpr(expr, collation string) string {
	if collation == "" {
		return expr
	}
	return fmt.Sprintf("(%s) COLLATE %s", expr, collation)
}

func knownStringCompare(column string, bools BoolLit, collation string) func(ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		s, ok := lit.(string)
		if !ok {
			switch op {
			case OpEqual:
				return bools.False, true, nil
			case OpNotEqual:
				// Present string != non-string is true; missing
				// (SQL NULL) must remain a non-match.
				return fmt.Sprintf("(%s IS NOT NULL)", column), true, nil
			default:
				return "", false, fmt.Errorf("filter: %w: ordered comparison requires a string operand", domain.ErrInvalidArgument)
			}
		}
		sqlOp, ok := comparisonSQLOp(op)
		if !ok {
			return "", false, nil
		}
		left := collateExpr(column, collation)
		right := collateExpr(bind(s), collation)
		return fmt.Sprintf("%s %s %s", left, sqlOp, right), true, nil
	}
}

func knownStringIn(column string, bools BoolLit, collation string) func([]any, func(any) string) (string, bool, error) {
	return func(values []any, bind func(any) string) (string, bool, error) {
		placeholders := make([]string, 0, len(values))
		for _, v := range values {
			s, ok := v.(string)
			if !ok {
				// Heterogeneous membership against a string field is
				// always false (CEL: string not in list of ints).
				return bools.False, true, nil
			}
			placeholders = append(placeholders, collateExpr(bind(s), collation))
		}
		return fmt.Sprintf("%s IN (%s)", collateExpr(column, collation), strings.Join(placeholders, ", ")), true, nil
	}
}

func knownStringStartsWith(column, collation string) func(string, func(any) string) (string, bool, error) {
	if collation == "" {
		return nil // generic LIKE is correct
	}
	return func(prefix string, bind func(any) string) (string, bool, error) {
		pattern := escapeLikePattern(prefix) + "%"
		return fmt.Sprintf("%s LIKE %s ESCAPE '\\'",
			collateExpr(column, collation),
			collateExpr(bind(pattern), collation),
		), true, nil
	}
}

// ProtoJSONInt64Field exposes an integer SQL column as its ProtoJSON
// decimal-string form (same known-string rules).
func ProtoJSONInt64Field(textSQL string, bools BoolLit, opts ...StringFieldOption) SQLExpr {
	return KnownStringField(textSQL, bools, opts...)
}

func comparisonSQLOp(op ComparisonOperator) (string, bool) {
	switch op {
	case OpEqual:
		return "=", true
	case OpNotEqual:
		return "!=", true
	case OpLess:
		return "<", true
	case OpLessEqual:
		return "<=", true
	case OpGreater:
		return ">", true
	case OpGreaterEqual:
		return ">=", true
	default:
		return "", false
	}
}
