package querysql

import (
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// PostgresKnownTimestamp returns an [SQLExpr] for a native TIMESTAMPTZ
// column under timestamp() conversion.
//
// The column stays unwrapped so btree indexes remain usable. CEL
// timestamps have nanosecond precision while TIMESTAMPTZ stores
// microseconds, so comparisons rewrite against that grid:
//
//   - microsecond-aligned literals use ordinary column op literal
//   - equality/inequality with a sub-microsecond remainder become
//     constant false/true for a present column (NULL when missing)
//   - ordered ops with a remainder compare against floor_to_micro(lit)
//   - IN drops non-aligned list elements
func PostgresKnownTimestamp(column string) SQLExpr {
	return SQLExpr{
		SQL:     column,
		Compare: postgresKnownTimestampCompare(column),
		In:      postgresKnownTimestampIn(column),
	}
}

func postgresKnownTimestampCompare(column string) func(ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		t, err := timestampLit(lit)
		if err != nil {
			return "", false, err
		}
		if !hasSubMicroRemainder(t) {
			sqlOp, ok := comparisonSQLOp(op)
			if !ok {
				return "", false, nil
			}
			return fmt.Sprintf("%s %s %s::timestamptz", column, sqlOp, bind(t)), true, nil
		}

		floor := floorToMicroUTC(t)
		switch op {
		case OpEqual:
			return presentBool(column, false), true, nil
		case OpNotEqual:
			return presentBool(column, true), true, nil
		case OpLess, OpLessEqual:
			return fmt.Sprintf("%s <= %s::timestamptz", column, bind(floor)), true, nil
		case OpGreater, OpGreaterEqual:
			return fmt.Sprintf("%s > %s::timestamptz", column, bind(floor)), true, nil
		default:
			return "", false, nil
		}
	}
}

func postgresKnownTimestampIn(column string) func([]any, func(any) string) (string, bool, error) {
	return func(values []any, bind func(any) string) (string, bool, error) {
		placeholders := make([]string, 0, len(values))
		for _, v := range values {
			t, err := timestampLit(v)
			if err != nil {
				return "", false, err
			}
			if hasSubMicroRemainder(t) {
				continue
			}
			placeholders = append(placeholders, bind(t)+"::timestamptz")
		}
		if len(placeholders) == 0 {
			return presentBool(column, false), true, nil
		}
		return fmt.Sprintf("%s IN (%s)", column, joinComma(placeholders)), true, nil
	}
}

// presentBool preserves missing-field (SQL NULL) behavior for
// compile-time constant true/false over a present TIMESTAMPTZ column.
func presentBool(column string, presentValue bool) string {
	lit := "FALSE"
	if presentValue {
		lit = "TRUE"
	}
	return fmt.Sprintf("(CASE WHEN %s IS NULL THEN NULL ELSE %s END)", column, lit)
}

// floorToMicroUTC truncates t to the microsecond grid in UTC by
// dropping any sub-microsecond nanoseconds (always toward -∞ within
// the second's nanosecond field).
func floorToMicroUTC(t time.Time) time.Time {
	t = t.UTC()
	rem := t.Nanosecond() % 1000
	if rem == 0 {
		return t
	}
	return t.Add(-time.Duration(rem) * time.Nanosecond)
}

func hasSubMicroRemainder(t time.Time) bool {
	return t.UTC().Nanosecond()%1000 != 0
}

// PostgresKnownTimestampString exposes a native TIMESTAMPTZ column as
// its canonical ProtoJSON string for direct string operations (without
// timestamp() conversion).
func PostgresKnownTimestampString(column string) SQLExpr {
	text := fmt.Sprintf("cel_ts_protojson_tstz(%s)", column)
	return KnownStringField(text, PostgresBool, WithCollate(`"C"`))
}

// CanonicalTimestampText lowers timestamp(<text expr>) against a
// write-time fixed-width UTC norm string ([FormatTimestampNorm]).
// Equality, inequality, IN, and ordered comparisons all bind that
// form of the literal and compare textExpr directly — no per-row
// cel_ts_norm. Use this for storage siblings such as condition
// _lastTransitionTimeNorm. Opaque / non-canonical strings should keep
// [PostgresTextTimestamp] / [PostgresDynamicTimestamp] (or SQLite
// equivalents). Direct CEL string ops on the ProtoJSON response field
// must target the ProtoJSON spelling, not this expression.
func CanonicalTimestampText(textExpr string, opts ...StringFieldOption) SQLExpr {
	cfg := stringFieldConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return SQLExpr{
		SQL:     textExpr,
		Compare: canonicalTimestampCompare(textExpr, cfg.Collate),
		In:      canonicalTimestampIn(textExpr, cfg.Collate),
	}
}

func canonicalTimestampCompare(textExpr, collation string) func(ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		t, err := timestampLit(lit)
		if err != nil {
			return "", false, err
		}
		sqlOp, ok := comparisonSQLOp(op)
		if !ok {
			return "", false, nil
		}
		norm := FormatTimestampNorm(t)
		left := collateExpr(textExpr, collation)
		right := collateExpr(bind(norm), collation)
		return fmt.Sprintf("%s %s %s", left, sqlOp, right), true, nil
	}
}

func canonicalTimestampIn(textExpr, collation string) func([]any, func(any) string) (string, bool, error) {
	return func(values []any, bind func(any) string) (string, bool, error) {
		placeholders := make([]string, 0, len(values))
		for _, v := range values {
			t, err := timestampLit(v)
			if err != nil {
				return "", false, err
			}
			placeholders = append(placeholders, collateExpr(bind(FormatTimestampNorm(t)), collation))
		}
		return fmt.Sprintf("%s IN (%s)", collateExpr(textExpr, collation), joinComma(placeholders)), true, nil
	}
}

// PostgresTextTimestamp lowers timestamp(<text expr>) via cel_ts_norm,
// preserving nanoseconds and rejecting non-RFC 3339 input as SQL NULL.
// This is the correct slow fallback for dynamic / unindexed strings;
// native TIMESTAMPTZ columns use [PostgresKnownTimestamp] instead.
func PostgresTextTimestamp(textExpr string) SQLExpr {
	converted := fmt.Sprintf("cel_ts_norm(%s)", textExpr)
	return SQLExpr{
		SQL:     converted,
		Compare: timestampNormCompare(converted),
		In:      timestampNormIn(converted),
	}
}

// PostgresDynamicTimestamp lowers timestamp(<jsonb path>) against a
// JSON string value. Non-string / invalid / missing → SQL NULL.
// Uses cel_ts_norm per candidate row; prefer write-time or indexed
// normalized values for high-traffic known paths.
func PostgresDynamicTimestamp(jsonbExpr string) SQLExpr {
	converted := fmt.Sprintf(
		`(CASE WHEN jsonb_typeof(%s) = 'string' THEN cel_ts_norm((%s) #>> '{}') END)`,
		jsonbExpr, jsonbExpr,
	)
	return SQLExpr{
		SQL:     converted,
		Compare: timestampNormCompare(converted),
		In:      timestampNormIn(converted),
	}
}

// SQLiteKnownTimestamp compares a TEXT RFC3339 column via cel_ts_norm
// against a bound fixed-width UTC norm string.
func SQLiteKnownTimestamp(column string) SQLExpr {
	converted := fmt.Sprintf("cel_ts_norm(%s)", column)
	return SQLExpr{
		SQL:     converted,
		Compare: timestampNormCompare(converted),
		In:      timestampNormIn(converted),
	}
}

// SQLiteKnownTimestampString exposes a TEXT timestamp column as its
// canonical ProtoJSON string for direct string operations.
func SQLiteKnownTimestampString(column string) SQLExpr {
	text := fmt.Sprintf("cel_ts_protojson(%s)", column)
	return KnownStringField(text, SQLiteBool)
}

// SQLiteDynamicTimestamp lowers timestamp(<json path>) for a value
// that must be a present JSON string at runtime.
func SQLiteDynamicTimestamp(column, path string) SQLExpr {
	typeOf := fmt.Sprintf("json_type(%s, %s)", column, path)
	extract := fmt.Sprintf("json_extract(%s, %s)", column, path)
	converted := fmt.Sprintf(
		`(CASE WHEN %s = 'text' THEN cel_ts_norm(%s) END)`,
		typeOf, extract,
	)
	return SQLExpr{
		SQL:     converted,
		Compare: timestampNormCompare(converted),
		In:      timestampNormIn(converted),
	}
}

func timestampNormCompare(converted string) func(ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		t, err := timestampLit(lit)
		if err != nil {
			return "", false, err
		}
		sqlOp, ok := comparisonSQLOp(op)
		if !ok {
			return "", false, nil
		}
		return fmt.Sprintf("%s %s %s", converted, sqlOp, bind(FormatTimestampNorm(t))), true, nil
	}
}

func timestampNormIn(converted string) func([]any, func(any) string) (string, bool, error) {
	return func(values []any, bind func(any) string) (string, bool, error) {
		placeholders := make([]string, 0, len(values))
		for _, v := range values {
			t, err := timestampLit(v)
			if err != nil {
				return "", false, err
			}
			placeholders = append(placeholders, bind(FormatTimestampNorm(t)))
		}
		return fmt.Sprintf("%s IN (%s)", converted, joinComma(placeholders)), true, nil
	}
}

func timestampLit(lit any) (time.Time, error) {
	switch v := lit.(type) {
	case time.Time:
		return v.UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("filter: %w: timestamp comparison requires a timestamp() literal", domain.ErrInvalidArgument)
	}
}
