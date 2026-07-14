package querysql_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

func TestProtoJSONTimestamp_MarshalCanonical(t *testing.T) {
	cases := []struct {
		in   time.Time
		want string
	}{
		{time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC), `"2026-06-01T12:00:00Z"`},
		{time.Date(2026, 6, 1, 12, 0, 0, 500_000_000, time.UTC), `"2026-06-01T12:00:00.500Z"`},
		{time.Date(2026, 6, 1, 12, 0, 0, 123_456_000, time.UTC), `"2026-06-01T12:00:00.123456Z"`},
		{time.Date(2026, 6, 1, 12, 0, 0, 123_456_789, time.UTC), `"2026-06-01T12:00:00.123456789Z"`},
	}
	for _, tt := range cases {
		b, err := json.Marshal(querysql.ProtoJSONTimestamp(tt.in))
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != tt.want {
			t.Errorf("Marshal(%v) = %s, want %s", tt.in, b, tt.want)
		}
	}
}

func TestProtoJSONTimestamp_UnmarshalLegacyThenRemarshalCanonical(t *testing.T) {
	// encoding/json historically emitted .5Z; ProtoJSON wants .500Z.
	var tstamp querysql.ProtoJSONTimestamp
	if err := json.Unmarshal([]byte(`"2026-06-01T12:00:00.5Z"`), &tstamp); err != nil {
		t.Fatal(err)
	}
	b, err := json.Marshal(tstamp)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"2026-06-01T12:00:00.500Z"` {
		t.Errorf("re-marshal = %s, want canonical .500Z", b)
	}
}
