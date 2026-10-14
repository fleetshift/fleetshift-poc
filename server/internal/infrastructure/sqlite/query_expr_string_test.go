package sqlite

import (
	"fmt"
	"testing"
)

func TestKnownStringIn_SkipsNonStringValues(t *testing.T) {
	expr := knownStringField("col")

	sql, handled, err := expr.In([]any{int64(1)}, func(any) string { return "?1" })
	if err != nil || !handled {
		t.Fatalf("In(non-string only): handled=%v err=%v", handled, err)
	}
	if sql != "0" {
		t.Errorf("In(non-string only) = %q, want \"0\"", sql)
	}

	var args []any
	bind := func(v any) string {
		args = append(args, v)
		return fmt.Sprintf("?%d", len(args))
	}
	sql, handled, err = expr.In([]any{int64(1), "a", true, "b"}, bind)
	if err != nil || !handled {
		t.Fatalf("In(mixed): handled=%v err=%v", handled, err)
	}
	if sql != "col IN (?1, ?2)" {
		t.Errorf("In(mixed) = %q, want only string placeholders", sql)
	}
	if len(args) != 2 || args[0] != "a" || args[1] != "b" {
		t.Errorf("In(mixed) args = %v, want [a b]", args)
	}

	sql, handled, err = expr.In([]any{"x", "y"}, func(v any) string {
		return "?" + v.(string)
	})
	if err != nil || !handled {
		t.Fatalf("In(strings only): handled=%v err=%v", handled, err)
	}
	if sql != "col IN (?x, ?y)" {
		t.Errorf("In(strings only) = %q, want unchanged string IN clause", sql)
	}
}
