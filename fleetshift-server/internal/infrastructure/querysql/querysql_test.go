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
// purely so this package's own AND/OR/NOT/comparison/in/has tests can
// compile a filter without depending on any real row shape.
type stubResolver struct{}

func (stubResolver) Resolve(path querysql.FieldPath, _ querysql.TypeHint, _ querysql.ResolveContext) (querysql.SQLExpr, error) {
	return querysql.SQLExpr{SQL: path.String()}, nil
}

func (stubResolver) ResolvePresence(path querysql.FieldPath, _ querysql.ResolveContext) (string, error) {
	return "HAS(" + path.String() + ")", nil
}

func (stubResolver) ResolveJSONMembership(target querysql.JSONMembershipTarget, key string, _ querysql.ResolveContext) (string, error) {
	return fmt.Sprintf("JSON_MEM(%d,%v,%q)", target.Root, target.Path, key), nil
}

func compile(t *testing.T, filter string) querysql.SQLPredicate {
	t.Helper()
	c := querysql.Compiler{Fields: stubResolver{}, Params: dollarTestParams{}}
	pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: filter})
	if err != nil {
		t.Fatalf("CompileFilter(%q): unexpected error: %v", filter, err)
	}
	return pred
}

func compileErr(t *testing.T, filter string) error {
	t.Helper()
	c := querysql.Compiler{Fields: stubResolver{}, Params: dollarTestParams{}}
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: filter})
	if err == nil {
		t.Fatalf("CompileFilter(%q): got nil error, want an error", filter)
	}
	return err
}

func TestCompileFilter_ConcurrentSharedCompiler(t *testing.T) {
	c := querysql.Compiler{Fields: stubResolver{}, Params: dollarTestParams{}}
	filters := []string{
		`resourceType == "kind.fleetshift.io/Cluster"`,
		`name == "alpha" && resourceType == "kind.fleetshift.io/Cluster"`,
		`!(name == "beta")`,
		`resourceType in ["kind.fleetshift.io/Cluster", "kind.fleetshift.io/Node"]`,
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
			filter:   `resourceType == "kind.fleetshift.io/Cluster"`,
			wantArgs: []any{"kind.fleetshift.io/Cluster"},
		},
		{
			name:     "not equals",
			filter:   `resourceType != "kind.fleetshift.io/Cluster"`,
			wantArgs: []any{"kind.fleetshift.io/Cluster"},
		},
		{
			name:     "and",
			filter:   `resourceType == "kind.fleetshift.io/Cluster" && name == "//kind.fleetshift.io/clusters/managed"`,
			wantArgs: []any{"kind.fleetshift.io/Cluster", "//kind.fleetshift.io/clusters/managed"},
		},
		{
			name:     "or",
			filter:   `resourceType == "kind.fleetshift.io/Cluster" || resourceType == "kubernetes.fleetshift.io/Node"`,
			wantArgs: []any{"kind.fleetshift.io/Cluster", "kubernetes.fleetshift.io/Node"},
		},
		{
			name:     "not",
			filter:   `!(resourceType == "kind.fleetshift.io/Cluster")`,
			wantArgs: []any{"kind.fleetshift.io/Cluster"},
		},
		{
			name:     "literal on left flips operator",
			filter:   `"//kind.fleetshift.io/clusters/managed" == name`,
			wantArgs: []any{"//kind.fleetshift.io/clusters/managed"},
		},
		{
			name:     "in list",
			filter:   `resourceType in ["kind.fleetshift.io/Cluster", "kubernetes.fleetshift.io/Node"]`,
			wantArgs: []any{"kind.fleetshift.io/Cluster", "kubernetes.fleetshift.io/Node"},
		},
		{
			name:     "in list is empty",
			filter:   `resourceType in []`,
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
	pred := compile(t, `resourceType in []`)
	if !strings.Contains(pred.SQL, "resourceType") {
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
		{"string literal", `resourceType == "kind.fleetshift.io/Cluster"`, querysql.TypeHintString},
		{"int literal", `resource.generation == 3`, querysql.TypeHintNumber},
		{"bool literal", `resource.healthy == true`, querysql.TypeHintBool},
		{"literal on left", `3 < resource.generation`, querysql.TypeHintNumber},
		{"in list uses first element", `resourceType in ["a", "b"]`, querysql.TypeHintString},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotHints []querysql.TypeHint
			recorder := recordingResolver(func(path querysql.FieldPath, hint querysql.TypeHint, _ querysql.ResolveContext) (querysql.SQLExpr, error) {
				gotHints = append(gotHints, hint)
				return querysql.SQLExpr{SQL: path.String()}, nil
			})
			c := querysql.Compiler{Fields: recorder, Params: dollarTestParams{}}
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

func (f recordingResolver) ResolvePresence(path querysql.FieldPath, ctx querysql.ResolveContext) (string, error) {
	return stubResolver{}.ResolvePresence(path, ctx)
}

func (f recordingResolver) ResolveJSONMembership(target querysql.JSONMembershipTarget, key string, ctx querysql.ResolveContext) (string, error) {
	return stubResolver{}.ResolveJSONMembership(target, key, ctx)
}

// TestCompileFilter_ComparisonRequiresFieldAndLiteral proves
// literal-vs-literal and field-vs-field comparisons are rejected:
// neither carries a queryable field on exactly one side, so neither
// is pushable to this row-shaped SQL.
func TestCompileFilter_ComparisonRequiresFieldAndLiteral(t *testing.T) {
	for _, filter := range []string{
		`"a" == "b"`,
		`name == resourceType`,
	} {
		err := compileErr(t, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

// TestCompileFilter_InListRequiresHomogeneousLiterals proves mixed-type
// "in" lists are rejected before resolve: the type hint is inferred
// from the first element, and every subsequent literal must match it.
func TestCompileFilter_InListRequiresHomogeneousLiterals(t *testing.T) {
	for _, filter := range []string{
		`resourceType in ["a", 1]`,
		`resource.generation in [1, "a"]`,
		`resource.healthy in [true, "yes"]`,
		`resource.generation in [1, true]`,
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
	c := querysql.Compiler{Params: dollarTestParams{}}
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: `resourceType == "kind.fleetshift.io/Cluster"`})
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
	} {
		err := compileErr(t, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

// TestCompileFilter_HasAndContainerInDisambiguation locks the supported
// has()/in compile contract from the CEL has() plan: valid has(select),
// invalid has(index), value-list in vs container-key in, and reject
// shapes that must not become string search.
func TestCompileFilter_HasAndContainerInDisambiguation(t *testing.T) {
	accept := []struct {
		name   string
		filter string
	}{
		{"has select map key", `has(resource.labels.team)`},
		{"has select container", `has(resource.spec)`},
		{"has select nested", `has(resource.conditions.Ready.status)`},
		{"has select observation", `has(resource.observation)`},
		{"has select pauseReason", `has(resource.pauseReason)`},
		{"container key in labels", `"team" in resource.labels`},
		{"container key in conditions", `"Ready" in resource.conditions`},
		{"nested key in condition object", `"status" in resource.conditions.Ready`},
		{"root key in resource", `"spec" in resource`},
		{"root pauseReason in resource", `"pauseReason" in resource`},
		{"dynamic nested in observation", `"k" in resource.observation.foo`},
		{"value list membership unchanged", `resource.labels.team in ["a", "b"]`},
		{"envelope value list unchanged", `resourceType in ["a/T", "b/U"]`},
		{"timestamp value list unchanged", `timestamp(resource.localUpdateTime) in [timestamp("2026-06-01T12:00:00Z")]`},
	}
	for _, tt := range accept {
		t.Run("accept/"+tt.name, func(t *testing.T) {
			pred := compile(t, tt.filter)
			if pred.SQL == "" {
				t.Errorf("SQL is empty for %q", tt.filter)
			}
		})
	}

	reject := []string{
		`has(resource.labels["team"])`,
		`resource.spec == has(resource.observation)`,
		`1 in resource.labels`,
		`true in resource.labels`,
		`"team" in ["team"]`,
	}
	for _, filter := range reject {
		t.Run("reject/"+filter, func(t *testing.T) {
			err := compileErr(t, filter)
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("err = %v, want ErrInvalidArgument", err)
			}
		})
	}
}

// TestCompileFilter_HasAndContainerInPathEquivalence proves that for an
// object-valued parent, has(parent.key) and "key" in parent compile to
// the same presence/membership SQL under the stub resolver (same path
// segments + same operand).
func TestCompileFilter_HasAndContainerInPathEquivalence(t *testing.T) {
	pairs := []struct {
		name string
		has  string
		in   string
	}{
		{"labels key", `has(resource.labels.team)`, `"team" in resource.labels`},
		{"condition subfield", `has(resource.conditions.Ready.status)`, `"status" in resource.conditions.Ready`},
		{"root spec", `has(resource.spec)`, `"spec" in resource`},
		{"root pauseReason", `has(resource.pauseReason)`, `"pauseReason" in resource`},
	}
	for _, tt := range pairs {
		t.Run(tt.name, func(t *testing.T) {
			hasPred := compile(t, tt.has)
			inPred := compile(t, tt.in)
			if hasPred.SQL != inPred.SQL {
				t.Errorf("SQL mismatch:\n  has: %s\n  in:  %s", hasPred.SQL, inPred.SQL)
			}
			if len(hasPred.Args) != len(inPred.Args) {
				t.Fatalf("Args len has=%d in=%d", len(hasPred.Args), len(inPred.Args))
			}
			for i := range hasPred.Args {
				if hasPred.Args[i] != inPred.Args[i] {
					t.Errorf("Args[%d]: has=%v in=%v", i, hasPred.Args[i], inPred.Args[i])
				}
			}
		})
	}
}

func TestCompileFilter_UnsupportedOperators(t *testing.T) {
	for _, filter := range []string{
		`name.matches("ext.*")`,
		`1 + 1 == 2`,
		`name.endsWith("x")`,
		`name.contains("x")`,
	} {
		err := compileErr(t, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

// TestCompileFilter_StartsWith compiles field.startsWith("prefix") to a
// parameterized LIKE predicate with LIKE metacharacters in the prefix
// escaped, so a user-supplied "%" or "_" cannot widen the match.
func TestCompileFilter_StartsWith(t *testing.T) {
	tests := []struct {
		name     string
		filter   string
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "simple prefix",
			filter:   `name.startsWith("//kind.fleetshift.io/")`,
			wantSQL:  `name LIKE $1 ESCAPE '\'`,
			wantArgs: []any{`//kind.fleetshift.io/%`},
		},
		{
			name:     "escapes LIKE metacharacters",
			filter:   `name.startsWith("a%b_c\\d")`,
			wantSQL:  `name LIKE $1 ESCAPE '\'`,
			wantArgs: []any{`a\%b\_c\\d%`},
		},
		{
			name:     "empty prefix matches any non-null string",
			filter:   `name.startsWith("")`,
			wantSQL:  `name LIKE $1 ESCAPE '\'`,
			wantArgs: []any{`%`},
		},
		{
			name:     "nested field path",
			filter:   `resource.labels["team"].startsWith("plat")`,
			wantSQL:  `resource.labels.team LIKE $1 ESCAPE '\'`,
			wantArgs: []any{`plat%`},
		},
		{
			name:     "and with startsWith",
			filter:   `resourceType == "kind.fleetshift.io/Cluster" && name.startsWith("//kind")`,
			wantSQL:  `(resourceType = $1) AND (name LIKE $2 ESCAPE '\')`,
			wantArgs: []any{"kind.fleetshift.io/Cluster", `//kind%`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred := compile(t, tt.filter)
			if pred.SQL != tt.wantSQL {
				t.Errorf("SQL = %q, want %q", pred.SQL, tt.wantSQL)
			}
			if len(pred.Args) != len(tt.wantArgs) {
				t.Fatalf("Args = %v, want %v", pred.Args, tt.wantArgs)
			}
			for i, want := range tt.wantArgs {
				if pred.Args[i] != want {
					t.Errorf("Args[%d] = %v, want %v", i, pred.Args[i], want)
				}
			}
		})
	}
}

func TestCompileFilter_StartsWithRequiresFieldAndStringLiteral(t *testing.T) {
	for _, filter := range []string{
		`"literal".startsWith("lit")`,
		`name.startsWith(resourceType)`,
		`name.startsWith(1)`,
	} {
		err := compileErr(t, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

// TestCompileFilter_FieldPathFlattening proves index and select
// syntax both flatten to the same [querysql.FieldPath] for
// message-looking and map-looking paths, and that a resolver error
// for an unsupported path propagates as ErrInvalidArgument.
func TestCompileFilter_FieldPathFlattening(t *testing.T) {
	var gotPaths []string
	c := querysql.Compiler{Fields: recordingResolver(func(path querysql.FieldPath, _ querysql.TypeHint, _ querysql.ResolveContext) (querysql.SQLExpr, error) {
		gotPaths = append(gotPaths, path.String())
		return querysql.SQLExpr{SQL: "col"}, nil
	}), Params: dollarTestParams{}}

	for _, tt := range []struct {
		filters []string
		want    string
	}{
		{
			filters: []string{`resource.labels.team == "x"`, `resource.labels["team"] == "x"`},
			want:    "resource.labels.team",
		},
		{
			filters: []string{`resource.spec.apiServerPort == "x"`, `resource.spec["apiServerPort"] == "x"`},
			want:    "resource.spec.apiServerPort",
		},
		{
			filters: []string{`resource.labels.node_role == "x"`, `resource.labels["node_role"] == "x"`},
			want:    "resource.labels.node_role",
		},
	} {
		for _, filter := range tt.filters {
			gotPaths = nil
			if _, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: filter}); err != nil {
				t.Fatalf("CompileFilter(%q): unexpected error: %v", filter, err)
			}
			if len(gotPaths) != 1 || gotPaths[0] != tt.want {
				t.Errorf("filter %q: paths = %v, want [%q]", filter, gotPaths, tt.want)
			}
		}
	}
}

func TestCompileFilter_ResourceTypeSnakeCaseRejected(t *testing.T) {
	c := querysql.Compiler{Fields: recordingResolver(func(querysql.FieldPath, querysql.TypeHint, querysql.ResolveContext) (querysql.SQLExpr, error) {
		return querysql.SQLExpr{SQL: "col"}, nil
	}), Params: dollarTestParams{}}
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `resource_type == "kind.fleetshift.io/Cluster"`,
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument for undeclared resource_type", err)
	}
}

// TestCompileFilter_ResolverErrorPropagates proves a FieldResolver's
// error (e.g. an unsupported field) surfaces as-is from CompileFilter.
func TestCompileFilter_ResolverErrorPropagates(t *testing.T) {
	sentinel := fmt.Errorf("boom: %w", domain.ErrInvalidArgument)
	c := querysql.Compiler{Fields: recordingResolver(func(querysql.FieldPath, querysql.TypeHint, querysql.ResolveContext) (querysql.SQLExpr, error) {
		return querysql.SQLExpr{}, sentinel
	}), Params: dollarTestParams{}}
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: `resourceType == "kind.fleetshift.io/Cluster"`})
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
	}), Params: dollarTestParams{}}
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: `resourceType in []`})
	if !errors.Is(err, sentinel) {
		t.Errorf("err = %v, want it to wrap the resolver's error even for an empty \"in\" list", err)
	}
}

func TestCompileFilter_GuardInsideOrDoesNotCount(t *testing.T) {
	// The resourceType guard only counts when it's a top-level `&&`
	// conjunct; inside an `||` branch it doesn't establish the type
	// for optional schema validation of the whole expression. Spec
	// paths still compile without a guard; this test only checks
	// guard *detection*.
	var gotGuard *domain.ResourceType
	c := querysql.Compiler{Fields: recordingResolver(func(_ querysql.FieldPath, _ querysql.TypeHint, ctx querysql.ResolveContext) (querysql.SQLExpr, error) {
		gotGuard = ctx.GuardedResourceType
		return querysql.SQLExpr{SQL: "col"}, nil
	}), Params: dollarTestParams{}}

	if _, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `resourceType == "kind.fleetshift.io/Cluster" && name == "//kind.fleetshift.io/clusters/managed"`,
	}); err != nil {
		t.Fatalf("CompileFilter: unexpected error: %v", err)
	}
	if gotGuard == nil || *gotGuard != domain.ResourceType("kind.fleetshift.io/Cluster") {
		t.Errorf("GuardedResourceType = %v, want kind.fleetshift.io/Cluster", gotGuard)
	}

	gotGuard = nil
	if _, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `(resourceType == "kind.fleetshift.io/Cluster") || name == "//kind.fleetshift.io/clusters/managed"`,
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
	}), Params: dollarTestParams{}}

	pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `resourceType == "kind.fleetshift.io/Cluster"`,
	})
	if err != nil {
		t.Fatalf("CompileFilter: %v", err)
	}
	if pred.SQL != "OVERRIDDEN($1)" {
		t.Errorf("SQL = %q, want OVERRIDDEN($1)", pred.SQL)
	}

	pred, err = c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `resourceType != "kind.fleetshift.io/Cluster"`,
	})
	if err != nil {
		t.Fatalf("CompileFilter (!=): %v", err)
	}
	if !strings.Contains(pred.SQL, "resourceType <> $1") {
		t.Errorf("SQL = %q, want generic fallback for <>", pred.SQL)
	}
}

// TestCompileFilter_StartsWithHookOverridesGenericStartsWith proves a
// [querysql.SQLExpr.StartsWith] hook can replace the generic
// "SQL LIKE ..." path when it reports handled=true, and that
// handled=false falls back to the generic path.
func TestCompileFilter_StartsWithHookOverridesGenericStartsWith(t *testing.T) {
	c := querysql.Compiler{Fields: recordingResolver(func(path querysql.FieldPath, _ querysql.TypeHint, _ querysql.ResolveContext) (querysql.SQLExpr, error) {
		return querysql.SQLExpr{
			SQL: path.String(),
			StartsWith: func(prefix string, bind func(any) string) (string, bool, error) {
				if prefix != "A" {
					return "", false, nil
				}
				return "STARTS_OVERRIDDEN(" + bind(prefix) + ")", true, nil
			},
		}, nil
	}), Params: dollarTestParams{}}

	pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `name.startsWith("A")`,
	})
	if err != nil {
		t.Fatalf("CompileFilter: %v", err)
	}
	if pred.SQL != "STARTS_OVERRIDDEN($1)" {
		t.Errorf("SQL = %q, want STARTS_OVERRIDDEN($1)", pred.SQL)
	}
	if len(pred.Args) != 1 || pred.Args[0] != "A" {
		t.Errorf("Args = %#v, want [\"A\"]", pred.Args)
	}

	pred, err = c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `name.startsWith("b")`,
	})
	if err != nil {
		t.Fatalf("CompileFilter (fallback): %v", err)
	}
	if !strings.Contains(pred.SQL, "name LIKE $1 ESCAPE") {
		t.Errorf("SQL = %q, want generic LIKE fallback", pred.SQL)
	}
	if len(pred.Args) != 1 || pred.Args[0] != `b%` {
		t.Errorf("Args = %#v, want [\"b%%\"]", pred.Args)
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
	}), Params: dollarTestParams{}}

	pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `resourceType in ["kind.fleetshift.io/Cluster", "kubernetes.fleetshift.io/Node"]`,
	})
	if err != nil {
		t.Fatalf("CompileFilter: %v", err)
	}
	if pred.SQL != "IN_OVERRIDDEN($1, $2)" {
		t.Errorf("SQL = %q, want IN_OVERRIDDEN($1, $2)", pred.SQL)
	}

	pred, err = c.CompileFilter(context.Background(), querysql.CompileFilterInput{
		Filter: `resourceType in ["only-one"]`,
	})
	if err != nil {
		t.Fatalf("CompileFilter (fallback): %v", err)
	}
	if !strings.Contains(pred.SQL, "resourceType IN ($1)") {
		t.Errorf("SQL = %q, want generic fallback for a single-element list", pred.SQL)
	}
}
