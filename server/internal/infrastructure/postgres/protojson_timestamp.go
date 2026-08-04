package postgres

import (
	"encoding/json"
	"fmt"
	"time"
)

// protoJSONTimestamp is a [time.Time] that encoding/json marshals as
// a ProtoJSON timestamp string (Z-normalized; 0, 3, 6, or 9 fractional
// digits). Unmarshal accepts any RFC 3339 / RFC 3339 Nano form so
// older encoding/json spellings round-trip; the next Marshal emits
// the canonical ProtoJSON form.
//
// Use this for inventory condition lastTransitionTime (and similar
// known timestamp strings) so storage matches QueryResources response
// spelling and GIN containment equality can participate.
type protoJSONTimestamp time.Time

// Time returns the underlying UTC instant.
func (t protoJSONTimestamp) Time() time.Time {
	return time.Time(t).UTC()
}

// MarshalJSON implements [json.Marshaler].
func (t protoJSONTimestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(formatProtoJSONTimestamp(t.Time()))
}

// UnmarshalJSON implements [json.Unmarshaler].
func (t *protoJSONTimestamp) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*t = protoJSONTimestamp{}
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		*t = protoJSONTimestamp{}
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return fmt.Errorf("protojson timestamp: %w", err)
	}
	*t = protoJSONTimestamp(parsed.UTC())
	return nil
}

// formatProtoJSONTimestamp returns the ProtoJSON spelling of t
// (Z-normalized; 0, 3, 6, or 9 fractional digits). Direct string
// comparisons against timestamp-valued response fields must use this
// form, not the database's native rendering.
func formatProtoJSONTimestamp(t time.Time) string {
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
