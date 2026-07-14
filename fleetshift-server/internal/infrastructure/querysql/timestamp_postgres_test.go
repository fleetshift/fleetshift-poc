package querysql_test

import (
	"strings"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

func TestPostgresKnownTimestamp_SubMicroOneNanosecondDiffers(t *testing.T) {
	// Review regression: stored .123456000 must not equal CEL .123456001
	// after pgx/TIMESTAMPTZ truncation.
	expr := querysql.PostgresKnownTimestamp("inv.observed_at")
	lit := mustTS(t, "2026-06-01T12:00:00.123456001Z")
	sql, handled, err := expr.Compare(querysql.OpEqual, lit, func(any) string { return "$1" })
	if err != nil || !handled {
		t.Fatalf("Compare: handled=%v err=%v", handled, err)
	}
	want := "(CASE WHEN inv.observed_at IS NULL THEN NULL ELSE FALSE END)"
	if sql != want {
		t.Errorf("SQL = %q, want %q", sql, want)
	}
	sql, handled, err = expr.In([]any{lit}, func(any) string { return "$1" })
	if err != nil || !handled {
		t.Fatalf("In: handled=%v err=%v", handled, err)
	}
	if sql != want {
		t.Errorf("In SQL = %q, want %q", sql, want)
	}
}

func TestPostgresKnownTimestamp_MicrosecondAlignedEquality(t *testing.T) {
	expr := querysql.PostgresKnownTimestamp("inv.observed_at")
	lit := mustTS(t, "2026-06-01T12:00:00.123456Z")
	var args []any
	bind := func(v any) string {
		args = append(args, v)
		return "$1"
	}
	sql, handled, err := expr.Compare(querysql.OpEqual, lit, bind)
	if err != nil || !handled {
		t.Fatalf("Compare: sql=%q handled=%v err=%v", sql, handled, err)
	}
	if sql != "inv.observed_at = $1::timestamptz" {
		t.Errorf("SQL = %q, want bare column equality for microsecond-aligned literal", sql)
	}
	if strings.Contains(sql, "cel_ts_norm") {
		t.Errorf("SQL = %q, native TIMESTAMPTZ must not use cel_ts_norm", sql)
	}
}

func TestPostgresKnownTimestamp_SubMicroEqualityIsConstantFalse(t *testing.T) {
	expr := querysql.PostgresKnownTimestamp("inv.observed_at")
	lit := mustTS(t, "2026-06-01T12:00:00.123456789Z")
	sql, handled, err := expr.Compare(querysql.OpEqual, lit, func(any) string { return "$1" })
	if err != nil || !handled {
		t.Fatalf("Compare: sql=%q handled=%v err=%v", sql, handled, err)
	}
	want := "(CASE WHEN inv.observed_at IS NULL THEN NULL ELSE FALSE END)"
	if sql != want {
		t.Errorf("SQL = %q, want %q", sql, want)
	}
}

func TestPostgresKnownTimestamp_SubMicroInequalityIsConstantTrue(t *testing.T) {
	expr := querysql.PostgresKnownTimestamp("inv.observed_at")
	lit := mustTS(t, "2026-06-01T12:00:00.123456789Z")
	sql, handled, err := expr.Compare(querysql.OpNotEqual, lit, func(any) string { return "$1" })
	if err != nil || !handled {
		t.Fatalf("Compare: sql=%q handled=%v err=%v", sql, handled, err)
	}
	want := "(CASE WHEN inv.observed_at IS NULL THEN NULL ELSE TRUE END)"
	if sql != want {
		t.Errorf("SQL = %q, want %q", sql, want)
	}
}

func TestPostgresKnownTimestamp_SubMicroOrderedOps(t *testing.T) {
	expr := querysql.PostgresKnownTimestamp("inv.observed_at")
	lit := mustTS(t, "2026-06-01T12:00:00.123456789Z")
	floor := mustTS(t, "2026-06-01T12:00:00.123456Z")

	cases := []struct {
		op      querysql.ComparisonOperator
		wantSQL string
	}{
		{querysql.OpLess, "inv.observed_at <= $1::timestamptz"},
		{querysql.OpLessEqual, "inv.observed_at <= $1::timestamptz"},
		{querysql.OpGreater, "inv.observed_at > $1::timestamptz"},
		{querysql.OpGreaterEqual, "inv.observed_at > $1::timestamptz"},
	}
	for _, tt := range cases {
		var got any
		bind := func(v any) string {
			got = v
			return "$1"
		}
		sql, handled, err := expr.Compare(tt.op, lit, bind)
		if err != nil || !handled {
			t.Fatalf("%v: handled=%v err=%v", tt.op, handled, err)
		}
		if sql != tt.wantSQL {
			t.Errorf("%v: SQL = %q, want %q", tt.op, sql, tt.wantSQL)
		}
		if gt, ok := got.(time.Time); !ok || !gt.Equal(floor) {
			t.Errorf("%v: bound %v, want floor %v", tt.op, got, floor)
		}
	}
}

func TestPostgresKnownTimestamp_InDropsSubMicroLiterals(t *testing.T) {
	expr := querysql.PostgresKnownTimestamp("inv.observed_at")
	aligned := mustTS(t, "2026-06-01T12:00:00.123456Z")
	sub := mustTS(t, "2026-06-01T12:00:00.123456789Z")

	sql, handled, err := expr.In([]any{sub}, func(any) string { return "$1" })
	if err != nil || !handled {
		t.Fatalf("In(sub only): handled=%v err=%v", handled, err)
	}
	wantFalse := "(CASE WHEN inv.observed_at IS NULL THEN NULL ELSE FALSE END)"
	if sql != wantFalse {
		t.Errorf("In(sub only) = %q, want %q", sql, wantFalse)
	}

	var args []any
	bind := func(v any) string {
		args = append(args, v)
		return "$1"
	}
	sql, handled, err = expr.In([]any{sub, aligned}, bind)
	if err != nil || !handled {
		t.Fatalf("In(mixed): handled=%v err=%v", handled, err)
	}
	if sql != "inv.observed_at IN ($1::timestamptz)" {
		t.Errorf("In(mixed) = %q, want only aligned literal", sql)
	}
	if len(args) != 1 || !args[0].(time.Time).Equal(aligned) {
		t.Errorf("In(mixed) args = %v, want [%v]", args, aligned)
	}
}

func mustTS(t *testing.T, s string) time.Time {
	t.Helper()
	tm, err := querysql.ParseCELTimestamp(s)
	if err != nil {
		t.Fatal(err)
	}
	return tm
}
