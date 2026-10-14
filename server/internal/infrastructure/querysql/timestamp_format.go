package querysql

import (
	"fmt"
	"time"
)

// ParseCELTimestamp parses a CEL timestamp() string argument. It
// accepts RFC 3339 / RFC 3339 Nano and rejects values outside years
// 0001–9999. The compiler binds successful parses as time.Time
// literals; backends decide how to render those literals in SQL.
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
