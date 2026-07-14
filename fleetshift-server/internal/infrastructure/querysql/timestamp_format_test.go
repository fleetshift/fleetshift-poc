package querysql_test

import (
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

func TestFormatTimestampNorm_FullCELRangeSortsChronologically(t *testing.T) {
	earlier, err := querysql.ParseCELTimestamp("0001-01-01T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	later, err := querysql.ParseCELTimestamp("9999-12-31T23:59:59.999999999Z")
	if err != nil {
		t.Fatal(err)
	}
	a := querysql.FormatTimestampNorm(earlier)
	b := querysql.FormatTimestampNorm(later)
	if a >= b {
		t.Fatalf("norm order: %q >= %q", a, b)
	}
	// Unix nanoseconds wrap and must not be used for these instants.
	if time.Unix(0, earlier.UnixNano()).UTC().Equal(earlier) {
		t.Fatal("expected UnixNano reconstruction to fail for year 0001")
	}
}

func TestFormatTimestampNorm_PreservesNanoseconds(t *testing.T) {
	a, _ := querysql.ParseCELTimestamp("2026-06-01T12:00:00.123456789Z")
	b, _ := querysql.ParseCELTimestamp("2026-06-01T12:00:00.123456788Z")
	if querysql.FormatTimestampNorm(a) == querysql.FormatTimestampNorm(b) {
		t.Fatal("nanosecond difference lost in norm form")
	}
}

func TestFormatTimestampNorm_OffsetEquivalent(t *testing.T) {
	a, _ := querysql.ParseCELTimestamp("2026-06-01T12:00:00Z")
	b, _ := querysql.ParseCELTimestamp("2026-06-01T08:00:00-04:00")
	if querysql.FormatTimestampNorm(a) != querysql.FormatTimestampNorm(b) {
		t.Fatalf("offset-equivalent instants: %q vs %q",
			querysql.FormatTimestampNorm(a), querysql.FormatTimestampNorm(b))
	}
}

func TestFormatProtoJSONTimestamp(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"2026-06-01T12:00:00Z", "2026-06-01T12:00:00Z"},
		{"2026-06-01T12:00:00.5Z", "2026-06-01T12:00:00.500Z"},
		{"2026-06-01T12:00:00.123456789Z", "2026-06-01T12:00:00.123456789Z"},
		{"2026-06-01T12:00:00.123456000Z", "2026-06-01T12:00:00.123456Z"},
	}
	for _, tt := range cases {
		tm, err := time.Parse(time.RFC3339Nano, tt.in)
		if err != nil {
			t.Fatal(err)
		}
		got := querysql.FormatProtoJSONTimestamp(tm)
		if got != tt.want {
			t.Errorf("FormatProtoJSONTimestamp(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestParseCELTimestamp_RejectsInvalid(t *testing.T) {
	for _, s := range []string{
		"2026-06-01",          // date-only
		"infinity",            // Postgres special
		"2026-06-01 12:00:00", // missing T / offset
		"",
	} {
		if _, err := querysql.ParseCELTimestamp(s); err == nil {
			t.Errorf("ParseCELTimestamp(%q) succeeded, want error", s)
		}
	}
}
