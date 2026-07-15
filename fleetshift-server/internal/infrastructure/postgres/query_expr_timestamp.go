package postgres

import (
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// conditionLastTransitionTimeNormJSONKey is the storage-only sibling of
// condition lastTransitionTime. It holds [formatTimestampNorm] of the
// same instant so timestamp() filters can equality-contain and order
// without per-row parsing. It must not appear in QueryResources
// responses; both spellings are derived from time.Time on write.
const conditionLastTransitionTimeNormJSONKey = "_lastTransitionTimeNorm"

const timestampNormLayout = "2006-01-02T15:04:05.000000000Z"

// formatTimestampNorm returns the fixed-width UTC form used for
// timestamp() instant comparisons in SQL.
func formatTimestampNorm(t time.Time) string {
	return t.UTC().Format(timestampNormLayout)
}

// knownTimestamp returns an [querysql.SQLExpr] for a native TIMESTAMPTZ
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
func knownTimestamp(column string) querysql.SQLExpr {
	return querysql.SQLExpr{
		SQL:     column,
		Compare: knownTimestampCompare(column),
		In:      knownTimestampIn(column),
	}
}

func knownTimestampCompare(column string) func(querysql.ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
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
		case querysql.OpEqual:
			return presentBool(column, false), true, nil
		case querysql.OpNotEqual:
			return presentBool(column, true), true, nil
		case querysql.OpLess, querysql.OpLessEqual:
			return fmt.Sprintf("%s <= %s::timestamptz", column, bind(floor)), true, nil
		case querysql.OpGreater, querysql.OpGreaterEqual:
			return fmt.Sprintf("%s > %s::timestamptz", column, bind(floor)), true, nil
		default:
			return "", false, nil
		}
	}
}

func knownTimestampIn(column string) func([]any, func(any) string) (string, bool, error) {
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

func presentBool(column string, presentValue bool) string {
	lit := "FALSE"
	if presentValue {
		lit = "TRUE"
	}
	return fmt.Sprintf("(CASE WHEN %s IS NULL THEN NULL ELSE %s END)", column, lit)
}

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

// knownTimestampString exposes a native TIMESTAMPTZ column as its
// canonical ProtoJSON string for direct string operations (without
// timestamp() conversion).
func knownTimestampString(column string) querysql.SQLExpr {
	return knownStringField(fmt.Sprintf("cel_ts_protojson_tstz(%s)", column))
}

// canonicalTimestampText lowers timestamp(<text expr>) against a
// write-time fixed-width UTC norm string ([formatTimestampNorm]).
// Equality, inequality, IN, and ordered comparisons all bind that
// form of the literal and compare textExpr directly — no per-row
// cel_ts_norm. Use this for storage siblings such as condition
// _lastTransitionTimeNorm.
func canonicalTimestampText(textExpr string) querysql.SQLExpr {
	return querysql.SQLExpr{
		SQL:     textExpr,
		Compare: canonicalTimestampCompare(textExpr),
		In:      canonicalTimestampIn(textExpr),
	}
}

func canonicalTimestampCompare(textExpr string) func(querysql.ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		t, err := timestampLit(lit)
		if err != nil {
			return "", false, err
		}
		sqlOp, ok := comparisonSQLOp(op)
		if !ok {
			return "", false, nil
		}
		norm := formatTimestampNorm(t)
		return fmt.Sprintf("%s %s %s", collateExpr(textExpr), sqlOp, collateExpr(bind(norm))), true, nil
	}
}

func canonicalTimestampIn(textExpr string) func([]any, func(any) string) (string, bool, error) {
	return func(values []any, bind func(any) string) (string, bool, error) {
		placeholders := make([]string, 0, len(values))
		for _, v := range values {
			t, err := timestampLit(v)
			if err != nil {
				return "", false, err
			}
			placeholders = append(placeholders, collateExpr(bind(formatTimestampNorm(t))))
		}
		return fmt.Sprintf("%s IN (%s)", collateExpr(textExpr), joinComma(placeholders)), true, nil
	}
}

// textTimestamp lowers timestamp(<text expr>) via cel_ts_norm,
// preserving nanoseconds and rejecting non-RFC 3339 input as SQL NULL.
func textTimestamp(textExpr string) querysql.SQLExpr {
	converted := fmt.Sprintf("cel_ts_norm(%s)", textExpr)
	return querysql.SQLExpr{
		SQL:     converted,
		Compare: timestampNormCompare(converted),
		In:      timestampNormIn(converted),
	}
}

// dynamicTimestamp lowers timestamp(<jsonb path>) against a JSON
// string value. Non-string / invalid / missing → SQL NULL.
func dynamicTimestamp(jsonbExpr string) querysql.SQLExpr {
	converted := fmt.Sprintf(
		`(CASE WHEN jsonb_typeof(%s) = 'string' THEN cel_ts_norm((%s) #>> '{}') END)`,
		jsonbExpr, jsonbExpr,
	)
	return querysql.SQLExpr{
		SQL:     converted,
		Compare: timestampNormCompare(converted),
		In:      timestampNormIn(converted),
	}
}

func timestampNormCompare(converted string) func(querysql.ComparisonOperator, any, func(any) string) (string, bool, error) {
	return func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
		t, err := timestampLit(lit)
		if err != nil {
			return "", false, err
		}
		sqlOp, ok := comparisonSQLOp(op)
		if !ok {
			return "", false, nil
		}
		return fmt.Sprintf("%s %s %s", converted, sqlOp, bind(formatTimestampNorm(t))), true, nil
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
			placeholders = append(placeholders, bind(formatTimestampNorm(t)))
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
