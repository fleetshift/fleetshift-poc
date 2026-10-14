package sqlite

import (
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// conditionLastTransitionTimeNormJSONKey is the storage-only sibling of
// condition lastTransitionTime. It holds [formatTimestampNorm] of the
// same instant so timestamp() filters can compare without per-row
// parsing. It must not appear in QueryResources responses; both
// spellings are derived from time.Time on write.
const conditionLastTransitionTimeNormJSONKey = "_lastTransitionTimeNorm"

const timestampNormLayout = "2006-01-02T15:04:05.000000000Z"

// formatTimestampNorm returns the fixed-width UTC form used for
// timestamp() instant comparisons in SQL.
func formatTimestampNorm(t time.Time) string {
	return t.UTC().Format(timestampNormLayout)
}

// knownTimestamp compares a TEXT RFC3339 column via cel_ts_norm
// against a bound fixed-width UTC norm string.
func knownTimestamp(column string) querysql.SQLExpr {
	converted := fmt.Sprintf("cel_ts_norm(%s)", column)
	return querysql.SQLExpr{
		SQL:     converted,
		Compare: timestampNormCompare(converted),
		In:      timestampNormIn(converted),
	}
}

// knownTimestampString exposes a TEXT timestamp column as its
// canonical ProtoJSON string for direct string operations.
func knownTimestampString(column string) querysql.SQLExpr {
	return knownStringField(fmt.Sprintf("cel_ts_protojson(%s)", column))
}

// canonicalTimestampText lowers timestamp(<text expr>) against a
// write-time fixed-width UTC norm string ([formatTimestampNorm]).
// Equality, inequality, IN, and ordered comparisons all bind that
// form of the literal and compare textExpr directly — no per-row
// cel_ts_norm.
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
		return fmt.Sprintf("%s %s %s", textExpr, sqlOp, bind(formatTimestampNorm(t))), true, nil
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
			placeholders = append(placeholders, bind(formatTimestampNorm(t)))
		}
		return fmt.Sprintf("%s IN (%s)", textExpr, joinComma(placeholders)), true, nil
	}
}

// dynamicTimestamp lowers timestamp(<json path>) for a value that
// must be a present JSON string at runtime.
func dynamicTimestamp(column, path string) querysql.SQLExpr {
	typeOf := fmt.Sprintf("json_type(%s, %s)", column, path)
	extract := fmt.Sprintf("json_extract(%s, %s)", column, path)
	converted := fmt.Sprintf(
		`(CASE WHEN %s = 'text' THEN cel_ts_norm(%s) END)`,
		typeOf, extract,
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
