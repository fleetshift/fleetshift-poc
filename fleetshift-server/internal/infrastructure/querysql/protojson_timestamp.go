package querysql

import (
	"encoding/json"
	"fmt"
	"time"
)

// ProtoJSONTimestamp is a [time.Time] that encoding/json marshals as
// a ProtoJSON timestamp string (Z-normalized; 0, 3, 6, or 9 fractional
// digits). Unmarshal accepts any RFC 3339 / RFC 3339 Nano form so
// older encoding/json spellings round-trip; the next Marshal emits
// the canonical ProtoJSON form.
//
// Use this for inventory condition lastTransitionTime (and similar
// known timestamp strings) so storage matches QueryResources response
// spelling and GIN containment equality can participate.
type ProtoJSONTimestamp time.Time

// Time returns the underlying UTC instant.
func (t ProtoJSONTimestamp) Time() time.Time {
	return time.Time(t).UTC()
}

// MarshalJSON implements [json.Marshaler].
func (t ProtoJSONTimestamp) MarshalJSON() ([]byte, error) {
	return json.Marshal(FormatProtoJSONTimestamp(t.Time()))
}

// UnmarshalJSON implements [json.Unmarshaler].
func (t *ProtoJSONTimestamp) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		*t = ProtoJSONTimestamp{}
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}
	if s == "" {
		*t = ProtoJSONTimestamp{}
		return nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return fmt.Errorf("protojson timestamp: %w", err)
	}
	*t = ProtoJSONTimestamp(parsed.UTC())
	return nil
}
