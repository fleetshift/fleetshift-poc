package querysql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// TestCompileFilter_ParamBinderControlsPlaceholders proves the
// compiler's ParamBinder owns placeholder spelling: DollarParams
// emits $N, QuestionParams emits "?", and a custom binder's text
// appears in both the generic comparison path and Bind calls made
// from a FieldResolver.
func TestCompileFilter_ParamBinderControlsPlaceholders(t *testing.T) {
	t.Parallel()

	resolver := recordingResolver(func(path querysql.FieldPath, _ querysql.TypeHint, ctx querysql.ResolveContext) (querysql.SQLExpr, error) {
		// Bind a key the way a labels["team"] resolver would, so the
		// binder is exercised from ResolveContext.Bind as well as
		// from the compiler's own literal binding.
		keyPh := ctx.Bind("team")
		return querysql.SQLExpr{SQL: path.String() + "[" + keyPh + "]"}, nil
	})

	t.Run("dollar", func(t *testing.T) {
		c := querysql.Compiler{Fields: resolver, Params: querysql.DollarParams{}}
		pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: `resource.labels["team"] == "platform"`,
		})
		if err != nil {
			t.Fatalf("CompileFilter: %v", err)
		}
		if !strings.Contains(pred.SQL, "$1") || !strings.Contains(pred.SQL, "$2") {
			t.Errorf("SQL = %q, want $1 and $2 placeholders", pred.SQL)
		}
		if strings.Contains(pred.SQL, "?") {
			t.Errorf("SQL = %q, DollarParams must not emit ?", pred.SQL)
		}
		if len(pred.Args) != 2 || pred.Args[0] != "team" || pred.Args[1] != "platform" {
			t.Errorf("Args = %v, want [team platform]", pred.Args)
		}
	})

	t.Run("question", func(t *testing.T) {
		c := querysql.Compiler{Fields: resolver, Params: querysql.QuestionParams{}}
		pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: `resource.labels["team"] == "platform"`,
		})
		if err != nil {
			t.Fatalf("CompileFilter: %v", err)
		}
		if strings.Contains(pred.SQL, "$") {
			t.Errorf("SQL = %q, QuestionParams must not emit $N", pred.SQL)
		}
		if strings.Count(pred.SQL, "?") != 2 {
			t.Errorf("SQL = %q, want exactly two ? placeholders", pred.SQL)
		}
		if len(pred.Args) != 2 || pred.Args[0] != "team" || pred.Args[1] != "platform" {
			t.Errorf("Args = %v, want [team platform]", pred.Args)
		}
	})

	t.Run("custom", func(t *testing.T) {
		c := querysql.Compiler{Fields: resolver, Params: prefixBinder{prefix: ":p"}}
		pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: `resource.labels["team"] == "platform"`,
		})
		if err != nil {
			t.Fatalf("CompileFilter: %v", err)
		}
		if !strings.Contains(pred.SQL, ":p1") || !strings.Contains(pred.SQL, ":p2") {
			t.Errorf("SQL = %q, want :p1 and :p2 from custom binder", pred.SQL)
		}
	})

	t.Run("nil defaults to dollar", func(t *testing.T) {
		c := querysql.Compiler{Fields: stubResolver{}}
		pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: `resource_type == "kind.fleetshift.io/Cluster"`,
		})
		if err != nil {
			t.Fatalf("CompileFilter: %v", err)
		}
		if !strings.Contains(pred.SQL, "$1") {
			t.Errorf("SQL = %q, nil Params should default to DollarParams", pred.SQL)
		}
	})
}

type prefixBinder struct{ prefix string }

func (b prefixBinder) Placeholder(n int) string {
	return fmt.Sprintf("%s%d", b.prefix, n)
}
