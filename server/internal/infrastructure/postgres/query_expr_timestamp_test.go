package postgres

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
		return "$1"
	})
	if err != nil || !handled {
		t.Fatalf("Compare: handled=%v err=%v", handled, err)
	}
	if sql != `(col) COLLATE "C" = ($1) COLLATE "C"` {
		t.Errorf("SQL = %q, want collated equality", sql)
	}
	want := formatTimestampNorm(lit)
	if got != want {
		t.Errorf("bound = %v, want fixed-width %q", got, want)
	}
	if strings.Contains(sql, "cel_ts_norm") {
		t.Errorf("SQL = %q, must not use cel_ts_norm", sql)
	}
}

func TestCanonicalTimestampText_OffsetLiteralCanonicalizes(t *testing.T) {
	expr := canonicalTimestampText("col")
	lit := time.Date(2026, 6, 1, 8, 0, 0, 0, time.FixedZone("EDT", -4*3600))
	var got any
	_, handled, err := expr.Compare(querysql.OpEqual, lit, func(v any) string {
		got = v
		return "$1"
	})
	if err != nil || !handled {
		t.Fatalf("Compare: handled=%v err=%v", handled, err)
	}
	want := formatTimestampNorm(lit.UTC())
	if got != want {
		t.Errorf("bound = %v, want %q", got, want)
	}
}

func TestCanonicalTimestampText_InequalityInAndOrderedAvoidCelTsNorm(t *testing.T) {
	expr := canonicalTimestampText("col")
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
	if got != formatTimestampNorm(lit) {
		t.Errorf("bound = %v, want fixed-width norm", got)
	}
}

func TestKnownTimestamp_SubMicroOneNanosecondDiffers(t *testing.T) {
	expr := knownTimestamp("inv.observed_at")
	lit := mustParseTS(t, "2026-06-01T12:00:00.123456001Z")
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

func TestKnownTimestamp_MicrosecondAlignedEquality(t *testing.T) {
	expr := knownTimestamp("inv.observed_at")
	lit := mustParseTS(t, "2026-06-01T12:00:00.123456Z")
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

func TestKnownTimestamp_SubMicroEqualityIsConstantFalse(t *testing.T) {
	expr := knownTimestamp("inv.observed_at")
	lit := mustParseTS(t, "2026-06-01T12:00:00.123456789Z")
	sql, handled, err := expr.Compare(querysql.OpEqual, lit, func(any) string { return "$1" })
	if err != nil || !handled {
		t.Fatalf("Compare: sql=%q handled=%v err=%v", sql, handled, err)
	}
	want := "(CASE WHEN inv.observed_at IS NULL THEN NULL ELSE FALSE END)"
	if sql != want {
		t.Errorf("SQL = %q, want %q", sql, want)
	}
}

func TestKnownTimestamp_SubMicroInequalityIsConstantTrue(t *testing.T) {
	expr := knownTimestamp("inv.observed_at")
	lit := mustParseTS(t, "2026-06-01T12:00:00.123456789Z")
	sql, handled, err := expr.Compare(querysql.OpNotEqual, lit, func(any) string { return "$1" })
	if err != nil || !handled {
		t.Fatalf("Compare: sql=%q handled=%v err=%v", sql, handled, err)
	}
	want := "(CASE WHEN inv.observed_at IS NULL THEN NULL ELSE TRUE END)"
	if sql != want {
		t.Errorf("SQL = %q, want %q", sql, want)
	}
}

func TestKnownTimestamp_SubMicroOrderedOps(t *testing.T) {
	expr := knownTimestamp("inv.observed_at")
	lit := mustParseTS(t, "2026-06-01T12:00:00.123456789Z")
	floor := mustParseTS(t, "2026-06-01T12:00:00.123456Z")

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

func TestKnownTimestamp_InDropsSubMicroLiterals(t *testing.T) {
	expr := knownTimestamp("inv.observed_at")
	aligned := mustParseTS(t, "2026-06-01T12:00:00.123456Z")
	sub := mustParseTS(t, "2026-06-01T12:00:00.123456789Z")

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
		b, err := json.Marshal(protoJSONTimestamp(tt.in))
		if err != nil {
			t.Fatal(err)
		}
		if string(b) != tt.want {
			t.Errorf("Marshal(%v) = %s, want %s", tt.in, b, tt.want)
		}
	}
}

func TestProtoJSONTimestamp_UnmarshalLegacyThenRemarshalCanonical(t *testing.T) {
	var tstamp protoJSONTimestamp
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

func TestFormatTimestampNorm_FullCELRangeSortsChronologically(t *testing.T) {
	earlier, err := querysql.ParseCELTimestamp("0001-01-01T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	later, err := querysql.ParseCELTimestamp("9999-12-31T23:59:59.999999999Z")
	if err != nil {
		t.Fatal(err)
	}
	a := formatTimestampNorm(earlier)
	b := formatTimestampNorm(later)
	if a >= b {
		t.Fatalf("norm order: %q >= %q", a, b)
	}
	if time.Unix(0, earlier.UnixNano()).UTC().Equal(earlier) {
		t.Fatal("expected UnixNano reconstruction to fail for year 0001")
	}
}

func TestFormatTimestampNorm_PreservesNanoseconds(t *testing.T) {
	a, _ := querysql.ParseCELTimestamp("2026-06-01T12:00:00.123456789Z")
	b, _ := querysql.ParseCELTimestamp("2026-06-01T12:00:00.123456788Z")
	if formatTimestampNorm(a) == formatTimestampNorm(b) {
		t.Fatal("nanosecond difference lost in norm form")
	}
}

func TestFormatTimestampNorm_OffsetEquivalent(t *testing.T) {
	a, _ := querysql.ParseCELTimestamp("2026-06-01T12:00:00Z")
	b, _ := querysql.ParseCELTimestamp("2026-06-01T08:00:00-04:00")
	if formatTimestampNorm(a) != formatTimestampNorm(b) {
		t.Fatalf("offset-equivalent instants: %q vs %q",
			formatTimestampNorm(a), formatTimestampNorm(b))
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
		got := formatProtoJSONTimestamp(tm)
		if got != tt.want {
			t.Errorf("formatProtoJSONTimestamp(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
