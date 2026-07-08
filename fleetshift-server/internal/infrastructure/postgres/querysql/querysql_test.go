// These tests cover only this package's own job -- CEL AST lowering,
// independent of any real field mapping (see the package doc's
// "Package split" section). They exercise it through a trivial stub
// [querysql.FieldResolver] rather than a real one; the actual
// FleetShift/Postgres field shape (resource.labels[...], JSONB
// extraction, schema-backed path validation, safe numeric/boolean
// casts, and the injection-safety of label/condition keys) is tested
// against the real resolver in
// ../query_filter_test.go, plus end-to-end in queryrepotest.
package querysql_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres/querysql"
)

// stubResolver resolves every field path to its dotted string form as
// bare (uncast, uncasted) SQL text, regardless of hint. It exists
// purely so this package's own AND/OR/NOT/comparison/in tests can
// compile a filter without depending on any real row shape.
type stubResolver struct{}

func (stubResolver) Resolve(path querysql.FieldPath, _ querysql.TypeHint, _ querysql.ResolveContext) (querysql.SQLExpr, error) {
	return querysql.SQLExpr{SQL: path.String()}, nil
}

func compile(t *testing.T, filter string) querysql.SQLPredicate {
	t.Helper()
	c := querysql.Compiler{Fields: stubResolver{}}
	pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: filter})
	if err != nil {
		t.Fatalf("CompileFilter(%q): unexpected error: %v", filter, err)
	}
	return pred
}

func compileErr(t *testing.T, filter string) error {
	t.Helper()
	c := querysql.Compiler{Fields: stubResolver{}}
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: filter})
	if err == nil {
		t.Fatalf("CompileFilter(%q): got nil error, want an error", filter)
	}
	return err
}

func TestCompileFilter_Empty(t *testing.T) {
	pred := compile(t, "")
	if pred.SQL != "TRUE" {
		t.Errorf("SQL = %q, want TRUE", pred.SQL)
	}
	if len(pred.Args) != 0 {
		t.Errorf("Args = %v, want empty", pred.Args)
	}
}

// TestCompileFilter_LogicalAndComparisonMechanics exercises the
// boolean structure (&&, ||, !), comparison operators (including
// flipping when the literal is on the left), and "in" list handling
// this package owns, independent of what any field path actually
// means.
func TestCompileFilter_LogicalAndComparisonMechanics(t *testing.T) {
	tests := []struct {
		name     string
		filter   string
		wantArgs []any
	}{
		{
			name:     "equals",
			filter:   `kind == "extension"`,
			wantArgs: []any{"extension"},
		},
		{
			name:     "not equals",
			filter:   `kind != "extension"`,
			wantArgs: []any{"extension"},
		},
		{
			name:     "and",
			filter:   `kind == "extension" && resource_type == "kind.fleetshift.io/Cluster"`,
			wantArgs: []any{"extension", "kind.fleetshift.io/Cluster"},
		},
		{
			name:     "or",
			filter:   `kind == "extension" || kind == "platform"`,
			wantArgs: []any{"extension", "platform"},
		},
		{
			name:     "not",
			filter:   `!(kind == "platform")`,
			wantArgs: []any{"platform"},
		},
		{
			name:     "literal on left flips operator",
			filter:   `"clusters/managed" == platform_name`,
			wantArgs: []any{"clusters/managed"},
		},
		{
			name:     "in list",
			filter:   `kind in ["platform", "extension"]`,
			wantArgs: []any{"platform", "extension"},
		},
		{
			name:     "in list is empty",
			filter:   `kind in []`,
			wantArgs: []any{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred := compile(t, tt.filter)
			if len(pred.Args) != len(tt.wantArgs) {
				t.Fatalf("Args = %v, want %v", pred.Args, tt.wantArgs)
			}
			for i, want := range tt.wantArgs {
				if pred.Args[i] != want {
					t.Errorf("Args[%d] = %v, want %v", i, pred.Args[i], want)
				}
			}
			if pred.SQL == "" {
				t.Errorf("SQL is empty")
			}
		})
	}
}

// TestCompileFilter_InEmptyListReferencesResolvedField proves `field
// in []` compiles to a predicate that still references the field's
// resolved SQL expression -- not a bare "FALSE" literal -- so any
// bind parameter the resolver produced while resolving it (e.g. a
// label key) stays referenced by the SQL text instead of dangling
// unused.
func TestCompileFilter_InEmptyListReferencesResolvedField(t *testing.T) {
	pred := compile(t, `kind in []`)
	if !strings.Contains(pred.SQL, "kind") {
		t.Errorf("SQL = %q, want it to still reference the resolved field", pred.SQL)
	}
	if !strings.Contains(pred.SQL, "FALSE") {
		t.Errorf("SQL = %q, want it to be a constant-false predicate", pred.SQL)
	}
}

// TestCompileFilter_TypeHintDerivedFromLiteral proves the compiler
// derives a [querysql.TypeHint] from the literal side of a comparison
// (or the first element of an "in" list) and passes it to the
// FieldResolver, rather than the resolver having to inspect the CEL
// AST itself.
func TestCompileFilter_TypeHintDerivedFromLiteral(t *testing.T) {
	tests := []struct {
		name     string
		filter   string
		wantHint querysql.TypeHint
	}{
		{"string literal", `kind == "extension"`, querysql.TypeHintString},
		{"int literal", `resource.generation == 3`, querysql.TypeHintNumber},
		{"bool literal", `resource.healthy == true`, querysql.TypeHintBool},
		{"literal on left", `3 < resource.generation`, querysql.TypeHintNumber},
		{"in list uses first element", `kind in ["a", "b"]`, querysql.TypeHintString},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotHints []querysql.TypeHint
			recorder := recordingResolver(func(path querysql.FieldPath, hint querysql.TypeHint, _ querysql.ResolveContext) (querysql.SQLExpr, error) {
				gotHints = append(gotHints, hint)
				return querysql.SQLExpr{SQL: path.String()}, nil
			})
			c := querysql.Compiler{Fields: recorder}
			if _, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: tt.filter}); err != nil {
				t.Fatalf("CompileFilter(%q): unexpected error: %v", tt.filter, err)
			}
			if len(gotHints) == 0 {
				t.Fatalf("resolver was never called")
			}
			for _, got := range gotHints {
				if got != tt.wantHint {
					t.Errorf("hint = %v, want %v", got, tt.wantHint)
				}
			}
		})
	}
}

type recordingResolver func(querysql.FieldPath, querysql.TypeHint, querysql.ResolveContext) (querysql.SQLExpr, error)

func (f recordingResolver) Resolve(path querysql.FieldPath, hint querysql.TypeHint, ctx querysql.ResolveContext) (querysql.SQLExpr, error) {
	return f(path, hint, ctx)
}

// TestCompileFilter_ComparisonRequiresFieldAndLiteral proves
// literal-vs-literal and field-vs-field comparisons are rejected:
// neither carries a queryable field on exactly one side, so neither
// is pushable to this row-shaped SQL.
func TestCompileFilter_ComparisonRequiresFieldAndLiteral(t *testing.T) {
	for _, filter := range []string{
		`"a" == "b"`,
		`kind == resource_type`,
	} {
		err := compileErr(t, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

// TestCompileFilter_NoResolverConfigured proves a Compiler with a nil
// Fields fails with a descriptive error -- not a nil-pointer panic --
// as soon as a filter actually references a field.
func TestCompileFilter_NoResolverConfigured(t *testing.T) {
	c := querysql.Compiler{}
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: `kind == "extension"`})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestCompileFilter_InvalidSyntax(t *testing.T) {
	err := compileErr(t, `kind ==`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestCompileFilter_UnsupportedMacro(t *testing.T) {
	for _, filter := range []string{
		`["a", "b"].exists(x, x == "a")`,
		`["a", "b"].all(x, x == "a")`,
		`has(resource.spec)`,
	} {
		err := compileErr(t, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

func TestCompileFilter_UnsupportedOperators(t *testing.T) {
	for _, filter := range []string{
		`kind.matches("ext.*")`,
		`1 + 1 == 2`,
	} {
		err := compileErr(t, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

// TestCompileFilter_FieldPathFlattening proves index and select
// syntax both flatten to the same [querysql.FieldPath], and that a
// resolver error for an unsupported path propagates as
// ErrInvalidArgument.
func TestCompileFilter_FieldPathFlattening(t *testing.T) {
	var gotPaths []string
	c := querysql.Compiler{Fields: recordingResolver(func(path querysql.FieldPath, _ querysql.TypeHint, _ querysql.ResolveContext) (querysql.SQLExpr, error) {
		gotPaths = append(gotPaths, path.String())
		return querysql.SQLExpr{SQL: "col"}, nil
	})}

	for _, filter := range []string{
		`resource.labels.team == "x"`,
		`resource.labels["team"] == "x"`,
	} {
		gotPaths = nil
		if _, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: filter}); err != nil {
			t.Fatalf("CompileFilter(%q): unexpected error: %v", filter, err)
		}
		if len(gotPaths) != 1 || gotPaths[0] != "resource.labels.team" {
			t.Errorf("filter %q: paths = %v, want [\"resource.labels.team\"]", filter, gotPaths)
		}
	}
}

// TestCompileFilter_ResolverErrorPropagates proves a FieldResolver's
// error (e.g. an unsupported field) surfaces as-is from CompileFilter.
func TestCompileFilter_ResolverErrorPropagates(t *testing.T) {
	sentinel := fmt.Errorf("boom: %w", domain.ErrInvalidArgument)
	c := querysql.Compiler{Fields: recordingResolver(func(querysql.FieldPath, querysql.TypeHint, querysql.ResolveContext) (querysql.SQLExpr, error) {
		return querysql.SQLExpr{}, sentinel
	})}
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: `kind == "extension"`})
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it to wrap the resolver's error", err)
	}
}

// TestCompileFilter_InEmptyListStillResolvesField proves `field in
// []` still resolves (and thus validates) the field, rather than
// short-circuiting straight to a FALSE literal before ever consulting
// the resolver -- otherwise an unsupported/typo'd field name would
// silently compile instead of failing closed (see
// ../query_filter_test.go's
// TestQueryFieldResolver_UnsupportedFieldInEmptyList for the
// end-to-end proof against the real resolver).
func TestCompileFilter_InEmptyListStillResolvesField(t *testing.T) {
	sentinel := fmt.Errorf("boom: %w", domain.ErrInvalidArgument)
	c := querysql.Compiler{Fields: recordingResolver(func(querysql.FieldPath, querysql.TypeHint, querysql.ResolveContext) (querysql.SQLExpr, error) {
		return querysql.SQLExpr{}, sentinel
	})}
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: `kind in []`})
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it to wrap the resolver's error even for an empty \"in\" list", err)
	}
}

func TestCompileFilter_GuardInsideOrDoesNotCount(t *testing.T) {
	// The resource_type guard only counts when it's a top-level `&&`
	// conjunct; inside an `||` branch it doesn't establish the type
	// for the whole expression. This is exercised here via
	// GuardedResourceType directly, since guard *detection* is this
	// package's job even though what a guard *licenses* is the
	// resolver's.
	var gotGuard *domain.ResourceType
	c := querysql.Compiler{Fields: recordingResolver(func(_ querysql.FieldPath, _ querysql.TypeHint, ctx querysql.ResolveContext) (querysql.SQLExpr, error) {
		gotGuard = ctx.GuardedResourceType
		return querysql.SQLExpr{SQL: "col"}, nil
	})}

	if _, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `resource_type == "kind.fleetshift.io/Cluster" && kind == "extension"`,
	}); err != nil {
		t.Fatalf("CompileFilter: unexpected error: %v", err)
	}
	if gotGuard == nil || *gotGuard != domain.ResourceType("kind.fleetshift.io/Cluster") {
		t.Errorf("GuardedResourceType = %v, want kind.fleetshift.io/Cluster", gotGuard)
	}

	gotGuard = nil
	if _, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `(resource_type == "kind.fleetshift.io/Cluster") || kind == "extension"`,
	}); err != nil {
		t.Fatalf("CompileFilter: unexpected error: %v", err)
	}
	if gotGuard != nil {
		t.Errorf("GuardedResourceType = %v, want nil (guard inside || does not count)", gotGuard)
	}
}
