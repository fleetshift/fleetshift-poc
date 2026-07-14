package querysql_test

import (
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

func TestCanonicalTimestampText_EqualityUsesFixedWidthNorm(t *testing.T) {
	expr := querysql.CanonicalTimestampText("col")
	lit := time.Date(2026, 6, 1, 12, 0, 0, 500_000_000, time.UTC)
	var got any
	sql, handled, err := expr.Compare(querysql.OpEqual, lit, func(v any) string {
		got = v
		return "$1"
	})
	if err != nil || !handled {
		t.Fatalf("Compare: handled=%v err=%v", handled, err)
	}
	if sql != "col = $1" {
		t.Errorf("SQL = %q, want col = $1", sql)
	}
	want := querysql.FormatTimestampNorm(lit)
	if got != want {
		t.Errorf("bound = %v, want fixed-width %q", got, want)
	}
	if strings.Contains(sql, "cel_ts_norm") {
		t.Errorf("SQL = %q, must not use cel_ts_norm", sql)
	}
}

func TestCanonicalTimestampText_OffsetLiteralCanonicalizes(t *testing.T) {
	expr := querysql.CanonicalTimestampText("col")
	lit := time.Date(2026, 6, 1, 8, 0, 0, 0, time.FixedZone("EDT", -4*3600))
	var got any
	_, handled, err := expr.Compare(querysql.OpEqual, lit, func(v any) string {
		got = v
		return "$1"
	})
	if err != nil || !handled {
		t.Fatalf("Compare: handled=%v err=%v", handled, err)
	}
	want := querysql.FormatTimestampNorm(lit.UTC())
	if got != want {
		t.Errorf("bound = %v, want %q", got, want)
	}
}

func TestCanonicalTimestampText_InequalityInAndOrderedAvoidCelTsNorm(t *testing.T) {
	expr := querysql.CanonicalTimestampText("col", querysql.WithCollate(`"C"`))
	lit := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

	sql, handled, err := expr.Compare(querysql.OpNotEqual, lit, func(any) string { return "$1" })
	if err != nil || !handled {
		t.Fatalf("!=: handled=%v err=%v", handled, err)
	}
	if strings.Contains(sql, "cel_ts_norm") {
		t.Errorf("!= SQL = %q, want direct text compare", sql)
	}

	sql, handled, err = expr.In([]any{lit}, func(any) string { return "$1" })
	if err != nil || !handled {
		t.Fatalf("IN: handled=%v err=%v", handled, err)
	}
	if strings.Contains(sql, "cel_ts_norm") {
		t.Errorf("IN SQL = %q, want direct text membership", sql)
	}

	var got any
	sql, handled, err = expr.Compare(querysql.OpLess, lit, func(v any) string {
		got = v
		return "$1"
	})
	if err != nil || !handled {
		t.Fatalf("<: handled=%v err=%v", handled, err)
	}
	if strings.Contains(sql, "cel_ts_norm") {
		t.Errorf("< SQL = %q, ordered must compare stored norm directly", sql)
	}
	if !strings.Contains(sql, `COLLATE "C"`) {
		t.Errorf("< SQL = %q, want COLLATE \"C\"", sql)
	}
	if got != querysql.FormatTimestampNorm(lit) {
		t.Errorf("bound = %v, want fixed-width norm", got)
	}
}
