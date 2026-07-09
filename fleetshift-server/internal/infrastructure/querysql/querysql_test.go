// These tests cover only this package's own job -- CEL AST lowering,
// independent of any real field mapping (see the package doc's
// "Package split" section). They exercise it through a trivial stub
// [querysql.FieldResolver] rather than a real one; the actual
// FleetShift/Postgres field shape (resource.labels[...], JSONB
// extraction, schema-backed path validation, safe numeric/boolean
// casts, and the injection-safety of label/condition keys) is tested
// against the real resolver in
// ../postgres/query_filter_test.go, plus end-to-end in queryrepotest.
package querysql_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
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

// TestCompileFilter_ConcurrentSharedCompiler proves a single Compiler
// (and its shared CEL env) can CompileFilter from many goroutines at
// once. Run with -race.
func TestCompileFilter_ConcurrentSharedCompiler(t *testing.T) {
	c := querysql.Compiler{Fields: stubResolver{}}
	filters := []string{
		`resource_type == "kind.fleetshift.io/Cluster"`,
		`name == "alpha" && resource_type == "kind.fleetshift.io/Cluster"`,
		`!(name == "beta")`,
		`resource_type in ["kind.fleetshift.io/Cluster", "kind.fleetshift.io/Node"]`,
	}

	const goroutines = 32
	var wg sync.WaitGroup
	errCh := make(chan error, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			filter := filters[i%len(filters)]
			if _, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: filter}); err != nil {
				errCh <- fmt.Errorf("goroutine %d filter %q: %w", i, filter, err)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
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
			filter:   `resource_type == "kind.fleetshift.io/Cluster"`,
			wantArgs: []any{"kind.fleetshift.io/Cluster"},
		},
		{
			name:     "not equals",
			filter:   `resource_type != "kind.fleetshift.io/Cluster"`,
			wantArgs: []any{"kind.fleetshift.io/Cluster"},
		},
		{
			name:     "and",
			filter:   `resource_type == "kind.fleetshift.io/Cluster" && name == "//kind.fleetshift.io/clusters/managed"`,
			wantArgs: []any{"kind.fleetshift.io/Cluster", "//kind.fleetshift.io/clusters/managed"},
		},
		{
			name:     "or",
			filter:   `resource_type == "kind.fleetshift.io/Cluster" || resource_type == "kubernetes.fleetshift.io/Node"`,
			wantArgs: []any{"kind.fleetshift.io/Cluster", "kubernetes.fleetshift.io/Node"},
		},
		{
			name:     "not",
			filter:   `!(resource_type == "kind.fleetshift.io/Cluster")`,
			wantArgs: []any{"kind.fleetshift.io/Cluster"},
		},
		{
			name:     "literal on left flips operator",
			filter:   `"//kind.fleetshift.io/clusters/managed" == name`,
			wantArgs: []any{"//kind.fleetshift.io/clusters/managed"},
		},
		{
			name:     "in list",
			filter:   `resource_type in ["kind.fleetshift.io/Cluster", "kubernetes.fleetshift.io/Node"]`,
			wantArgs: []any{"kind.fleetshift.io/Cluster", "kubernetes.fleetshift.io/Node"},
		},
		{
			name:     "in list is empty",
			filter:   `resource_type in []`,
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
	pred := compile(t, `resource_type in []`)
	if !strings.Contains(pred.SQL, "resource_type") {
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
		{"string literal", `resource_type == "kind.fleetshift.io/Cluster"`, querysql.TypeHintString},
		{"int literal", `resource.generation == 3`, querysql.TypeHintNumber},
		{"bool literal", `resource.healthy == true`, querysql.TypeHintBool},
		{"literal on left", `3 < resource.generation`, querysql.TypeHintNumber},
		{"in list uses first element", `resource_type in ["a", "b"]`, querysql.TypeHintString},
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
		`name == resource_type`,
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
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: `resource_type == "kind.fleetshift.io/Cluster"`})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestCompileFilter_InvalidSyntax(t *testing.T) {
	err := compileErr(t, `name ==`)
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
		`name.matches("ext.*")`,
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
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: `resource_type == "kind.fleetshift.io/Cluster"`})
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it to wrap the resolver's error", err)
	}
}

// TestCompileFilter_InEmptyListStillResolvesField proves `field in
// []` still resolves (and thus validates) the field, rather than
// short-circuiting straight to a FALSE literal before ever consulting
// the resolver -- otherwise an unsupported/typo'd field name would
// silently compile instead of failing closed (see
// ../postgres/query_filter_test.go's
// TestQueryFieldResolver_UnsupportedFieldInEmptyList for the
// end-to-end proof against the real resolver).
func TestCompileFilter_InEmptyListStillResolvesField(t *testing.T) {
	sentinel := fmt.Errorf("boom: %w", domain.ErrInvalidArgument)
	c := querysql.Compiler{Fields: recordingResolver(func(querysql.FieldPath, querysql.TypeHint, querysql.ResolveContext) (querysql.SQLExpr, error) {
		return querysql.SQLExpr{}, sentinel
	})}
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: `resource_type in []`})
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
		Filter: `resource_type == "kind.fleetshift.io/Cluster" && name == "//kind.fleetshift.io/clusters/managed"`,
	}); err != nil {
		t.Fatalf("CompileFilter: unexpected error: %v", err)
	}
	if gotGuard == nil || *gotGuard != domain.ResourceType("kind.fleetshift.io/Cluster") {
		t.Errorf("GuardedResourceType = %v, want kind.fleetshift.io/Cluster", gotGuard)
	}

	gotGuard = nil
	if _, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `(resource_type == "kind.fleetshift.io/Cluster") || name == "//kind.fleetshift.io/clusters/managed"`,
	}); err != nil {
		t.Fatalf("CompileFilter: unexpected error: %v", err)
	}
	if gotGuard != nil {
		t.Errorf("GuardedResourceType = %v, want nil (guard inside || does not count)", gotGuard)
	}
}

// TestCompileFilter_CompareHookOverridesGenericComparison proves a
// [querysql.SQLExpr.Compare] hook can replace the generic
// "SQL op <bound literal>" path when it reports handled=true, and
// that handled=false falls back to the generic path.
func TestCompileFilter_CompareHookOverridesGenericComparison(t *testing.T) {
	c := querysql.Compiler{Fields: recordingResolver(func(path querysql.FieldPath, _ querysql.TypeHint, _ querysql.ResolveContext) (querysql.SQLExpr, error) {
		return querysql.SQLExpr{
			SQL: path.String(),
			Compare: func(op querysql.ComparisonOperator, lit any, bind func(any) string) (string, bool, error) {
				if op != querysql.OpEqual {
					return "", false, nil
				}
				return "OVERRIDDEN(" + bind(lit) + ")", true, nil
			},
		}, nil
	})}

	pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `resource_type == "kind.fleetshift.io/Cluster"`,
	})
	if err != nil {
		t.Fatalf("CompileFilter: %v", err)
	}
	if pred.SQL != "OVERRIDDEN($1)" {
		t.Errorf("SQL = %q, want OVERRIDDEN($1)", pred.SQL)
	}

	pred, err = c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `resource_type != "kind.fleetshift.io/Cluster"`,
	})
	if err != nil {
		t.Fatalf("CompileFilter (!=): %v", err)
	}
	if !strings.Contains(pred.SQL, "resource_type != $1") {
		t.Errorf("SQL = %q, want generic fallback for !=", pred.SQL)
	}
}

// TestCompileFilter_InHookOverridesGenericIn proves a
// [querysql.SQLExpr.In] hook can replace the generic "SQL IN (...)"
// path when it reports handled=true, and that handled=false falls
// back to the generic path.
func TestCompileFilter_InHookOverridesGenericIn(t *testing.T) {
	c := querysql.Compiler{Fields: recordingResolver(func(path querysql.FieldPath, _ querysql.TypeHint, _ querysql.ResolveContext) (querysql.SQLExpr, error) {
		return querysql.SQLExpr{
			SQL: path.String(),
			In: func(values []any, bind func(any) string) (string, bool, error) {
				if len(values) != 2 {
					return "", false, nil
				}
				return "IN_OVERRIDDEN(" + bind(values[0]) + ", " + bind(values[1]) + ")", true, nil
			},
		}, nil
	})}

	pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `resource_type in ["kind.fleetshift.io/Cluster", "kubernetes.fleetshift.io/Node"]`,
	})
	if err != nil {
		t.Fatalf("CompileFilter: %v", err)
	}
	if pred.SQL != "IN_OVERRIDDEN($1, $2)" {
		t.Errorf("SQL = %q, want IN_OVERRIDDEN($1, $2)", pred.SQL)
	}

	pred, err = c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `resource_type in ["only-one"]`,
	})
	if err != nil {
		t.Fatalf("CompileFilter (fallback): %v", err)
	}
	if !strings.Contains(pred.SQL, "resource_type IN ($1)") {
		t.Errorf("SQL = %q, want generic fallback for a single-element list", pred.SQL)
	}
}
