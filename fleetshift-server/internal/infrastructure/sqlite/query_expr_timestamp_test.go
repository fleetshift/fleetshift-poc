package sqlite

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

func TestCanonicalTimestampText_EqualityUsesFixedWidthNorm(t *testing.T) {
	expr := canonicalTimestampText("col")
	lit := time.Date(2026, 6, 1, 12, 0, 0, 500_000_000, time.UTC)
	var got any
	sql, handled, err := expr.Compare(querysql.OpEqual, lit, func(v any) string {
		got = v
		return "?1"
	})
	if err != nil || !handled {
		t.Fatalf("Compare: handled=%v err=%v", handled, err)
	}
	if sql != "col = ?1" {
		t.Errorf("SQL = %q, want col = ?1", sql)
	}
	want := formatTimestampNorm(lit)
	if got != want {
		t.Errorf("bound = %v, want fixed-width %q", got, want)
	}
	if strings.Contains(sql, "cel_ts_norm") {
		t.Errorf("SQL = %q, must not use cel_ts_norm", sql)
	}
}

func TestCanonicalTimestampText_OffsetAndOrdered(t *testing.T) {
	expr := canonicalTimestampText("col")
	lit := time.Date(2026, 6, 1, 8, 0, 0, 0, time.FixedZone("EDT", -4*3600))
	var got any
	_, handled, err := expr.Compare(querysql.OpEqual, lit, func(v any) string {
		got = v
		return "?1"
	})
	if err != nil || !handled {
		t.Fatalf("Compare: handled=%v err=%v", handled, err)
	}
	if got != formatTimestampNorm(lit.UTC()) {
		t.Errorf("bound = %v, want Z-normalized norm", got)
	}

	sql, handled, err := expr.Compare(querysql.OpLess, lit.UTC(), func(any) string { return "?1" })
	if err != nil || !handled {
		t.Fatalf("<: handled=%v err=%v", handled, err)
	}
	if strings.Contains(sql, "cel_ts_norm") {
		t.Errorf("< SQL = %q, want direct text compare", sql)
	}
}

func TestProtoJSONTimestamp_MarshalCanonical(t *testing.T) {
	b, err := json.Marshal(protoJSONTimestamp(time.Date(2026, 6, 1, 12, 0, 0, 500_000_000, time.UTC)))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"2026-06-01T12:00:00.500Z"` {
		t.Errorf("Marshal = %s, want .500Z", b)
	}

	var tstamp protoJSONTimestamp
	if err := json.Unmarshal([]byte(`"2026-06-01T12:00:00.5Z"`), &tstamp); err != nil {
		t.Fatal(err)
	}
	b, err = json.Marshal(tstamp)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `"2026-06-01T12:00:00.500Z"` {
		t.Errorf("re-marshal = %s, want .500Z", b)
	}
}

func TestFormatTimestampNorm_OffsetEquivalent(t *testing.T) {
	a, _ := querysql.ParseCELTimestamp("2026-06-01T12:00:00Z")
	b, _ := querysql.ParseCELTimestamp("2026-06-01T08:00:00-04:00")
	if formatTimestampNorm(a) != formatTimestampNorm(b) {
		t.Fatalf("offset-equivalent: %q vs %q", formatTimestampNorm(a), formatTimestampNorm(b))
	}
}
