package querysql_test

import (
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

func TestParseCELTimestamp_AcceptsRFC3339(t *testing.T) {
	tm, err := querysql.ParseCELTimestamp("2026-06-01T08:00:00-04:00")
	if err != nil {
		t.Fatal(err)
	}
	want := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	if !tm.Equal(want) {
		t.Errorf("got %v, want %v", tm, want)
	}
}

func TestParseCELTimestamp_RejectsInvalid(t *testing.T) {
	for _, s := range []string{
		"2026-06-01",          // date-only
		"infinity",            // Postgres special
		"2026-06-01 12:00:00", // missing T / offset
		"",
		"0000-01-01T00:00:00Z", // outside CEL range
	} {
		if _, err := querysql.ParseCELTimestamp(s); err == nil {
			t.Errorf("ParseCELTimestamp(%q) succeeded, want error", s)
		}
	}
}
