package querysql_test

import (
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

func TestLowercaseStringCompare(t *testing.T) {
	cmp := querysql.LowercaseStringCompare("f.state")
	binds := []any{}
	bind := func(v any) string {
		binds = append(binds, v)
		return "?"
	}

	sql, handled, err := cmp(querysql.OpEqual, "ACTIVE", bind)
	if err != nil {
		t.Fatalf("Compare: %v", err)
	}
	if !handled {
		t.Fatal("expected handled=true")
	}
	if sql != "f.state = ?" {
		t.Errorf("SQL = %q, want f.state = ?", sql)
	}
	if len(binds) != 1 || binds[0] != "active" {
		t.Errorf("bound = %#v, want [\"active\"]", binds)
	}
}

func TestLowercaseStringIn(t *testing.T) {
	in := querysql.LowercaseStringIn("f.state")
	binds := []any{}
	bind := func(v any) string {
		binds = append(binds, v)
		return "?"
	}
	sql, handled, err := in([]any{"CREATING", "Active"}, bind)
	if err != nil {
		t.Fatalf("In: %v", err)
	}
	if !handled {
		t.Fatal("expected handled=true")
	}
	if !strings.Contains(sql, "f.state IN") {
		t.Errorf("SQL = %q", sql)
	}
	if len(binds) != 2 || binds[0] != "creating" || binds[1] != "active" {
		t.Errorf("bound = %#v", binds)
	}
}

func TestLowercaseStringStartsWith(t *testing.T) {
	sw := querysql.LowercaseStringStartsWith("f.state")
	binds := []any{}
	bind := func(v any) string {
		binds = append(binds, v)
		return "?"
	}

	sql, handled, err := sw("CRE", bind)
	if err != nil {
		t.Fatalf("StartsWith: %v", err)
	}
	if !handled {
		t.Fatal("expected handled=true")
	}
	if sql != `f.state LIKE ? ESCAPE '\'` {
		t.Errorf("SQL = %q, want f.state LIKE ? ESCAPE '\\'", sql)
	}
	if len(binds) != 1 || binds[0] != "cre%" {
		t.Errorf("bound = %#v, want [\"cre%%\"]", binds)
	}

	binds = nil
	sql, handled, err = sw(`A%B_C\D`, bind)
	if err != nil {
		t.Fatalf("StartsWith (metacharacters): %v", err)
	}
	if !handled {
		t.Fatal("expected handled=true")
	}
	if sql != `f.state LIKE ? ESCAPE '\'` {
		t.Errorf("SQL = %q", sql)
	}
	if len(binds) != 1 || binds[0] != `a\%b\_c\\d%` {
		t.Errorf("bound = %#v, want escaped lowercased pattern", binds)
	}
}
