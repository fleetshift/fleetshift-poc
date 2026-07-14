package postgres

import (
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

func TestCelTsNorm_MatchesGoFormatter(t *testing.T) {
	db := OpenTestDB(t)

	cases := []struct {
		in   string
		want string // empty means SQL NULL
	}{
		{"2026-06-01T12:00:00Z", querysql.FormatTimestampNorm(mustParseTS(t, "2026-06-01T12:00:00Z"))},
		{"2026-06-01T08:00:00-04:00", querysql.FormatTimestampNorm(mustParseTS(t, "2026-06-01T12:00:00Z"))},
		{"2026-06-01T12:00:00.123456789Z", querysql.FormatTimestampNorm(mustParseTS(t, "2026-06-01T12:00:00.123456789Z"))},
		{"2026-06-01T12:00:00,123456789Z", querysql.FormatTimestampNorm(mustParseTS(t, "2026-06-01T12:00:00,123456789Z"))},
		{"2026-06-01T12:00:00.123456789012Z", querysql.FormatTimestampNorm(mustParseTS(t, "2026-06-01T12:00:00.123456789012Z"))},
		{"0001-01-01T00:00:00Z", querysql.FormatTimestampNorm(mustParseTS(t, "0001-01-01T00:00:00Z"))},
		{"9999-12-31T23:59:59.999999999Z", querysql.FormatTimestampNorm(mustParseTS(t, "9999-12-31T23:59:59.999999999Z"))},
		{"2026-06-01", ""},             // date-only
		{"infinity", ""},               // Postgres special
		{"2026-06-01 12:00:00+00", ""}, // missing T
		{"2026-06-01t12:00:00Z", ""},   // lowercase t (rejected by Go)
		{"2026-06-01T12:00:00z", ""},   // lowercase z
		{"0000-01-01T00:00:00Z", ""},   // outside CEL range
	}
	for _, tt := range cases {
		t.Run(tt.in, func(t *testing.T) {
			// Differential: ParseCELTimestamp acceptance must match NULL vs value.
			_, parseErr := querysql.ParseCELTimestamp(tt.in)
			var got *string
			if err := db.QueryRow(`SELECT cel_ts_norm($1)`, tt.in).Scan(&got); err != nil {
				t.Fatalf("cel_ts_norm(%q): %v", tt.in, err)
			}
			if tt.want == "" {
				if parseErr == nil {
					t.Fatalf("ParseCELTimestamp(%q) succeeded, but test expects SQL NULL", tt.in)
				}
				if got != nil {
					t.Errorf("cel_ts_norm(%q) = %q, want NULL", tt.in, *got)
				}
				return
			}
			if parseErr != nil {
				t.Fatalf("ParseCELTimestamp(%q): %v", tt.in, parseErr)
			}
			if got == nil || *got != tt.want {
				t.Errorf("cel_ts_norm(%q) = %v, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestCelTsNorm_NanosecondInequality(t *testing.T) {
	db := OpenTestDB(t)
	var eq bool
	err := db.QueryRow(`
SELECT cel_ts_norm($1) = cel_ts_norm($2)`,
		"2026-06-01T12:00:00.123456789Z",
		"2026-06-01T12:00:00.123456788Z",
	).Scan(&eq)
	if err != nil {
		t.Fatal(err)
	}
	if eq {
		t.Fatal("nanosecond-differing timestamps compared equal under cel_ts_norm")
	}
}

func TestCelTsProtojsonTstz(t *testing.T) {
	db := OpenTestDB(t)
	var got string
	err := db.QueryRow(`SELECT cel_ts_protojson_tstz($1::timestamptz)`, "2026-06-01T12:00:00.5Z").Scan(&got)
	if err != nil {
		t.Fatal(err)
	}
	want := querysql.FormatProtoJSONTimestamp(mustParseTS(t, "2026-06-01T12:00:00.5Z"))
	if got != want {
		t.Errorf("cel_ts_protojson_tstz = %q, want %q", got, want)
	}
}

func mustParseTS(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := querysql.ParseCELTimestamp(s)
	if err != nil {
		t.Fatal(err)
	}
	return tm
}
