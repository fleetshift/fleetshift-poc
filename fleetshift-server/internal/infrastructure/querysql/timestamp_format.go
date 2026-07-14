package querysql

import (
	"fmt"
	"time"
)

// timestampNormLayout is a fixed-width UTC RFC 3339 form with exactly
// nine fractional digits. Lexicographic order matches chronological
// order across CEL's full timestamp range (years 0001–9999), which
// signed unix nanoseconds cannot represent.
const timestampNormLayout = "2006-01-02T15:04:05.000000000Z"

// FormatTimestampNorm returns the fixed-width UTC form used for
// timestamp() instant comparisons in SQL.
func FormatTimestampNorm(t time.Time) string {
	return t.UTC().Format(timestampNormLayout)
}

// ConditionLastTransitionTimeNormJSONKey is the storage-only sibling of
// condition lastTransitionTime. It holds [FormatTimestampNorm] of the
// same instant so timestamp() filters can equality-contain and order
// without per-row parsing. It must not appear in QueryResources
// responses; both spellings are derived from time.Time on write.
const ConditionLastTransitionTimeNormJSONKey = "_lastTransitionTimeNorm"

// FormatProtoJSONTimestamp returns the ProtoJSON spelling of t
// (Z-normalized; 0, 3, 6, or 9 fractional digits). Direct string
// comparisons against timestamp-valued response fields must use this
// form, not the database's native rendering.
func FormatProtoJSONTimestamp(t time.Time) string {
	t = t.UTC()
	base := t.Format("2006-01-02T15:04:05")
	nanos := t.Nanosecond()
	if nanos == 0 {
		return base + "Z"
	}
	switch {
	case nanos%1_000_000 == 0:
		return fmt.Sprintf("%s.%03dZ", base, nanos/1_000_000)
	case nanos%1_000 == 0:
		return fmt.Sprintf("%s.%06dZ", base, nanos/1_000)
	default:
		return fmt.Sprintf("%s.%09dZ", base, nanos)
	}
}

// ParseCELTimestamp parses a CEL timestamp() string argument. It
// accepts RFC 3339 / RFC 3339 Nano and rejects values outside years
// 0001–9999.
func ParseCELTimestamp(s string) (time.Time, error) {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}, err
	}
	t = t.UTC()
	if y := t.Year(); y < 1 || y > 9999 {
		return time.Time{}, fmt.Errorf("timestamp out of CEL range")
	}
	return t, nil
}
