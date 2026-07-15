package querysql_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// dollarTestParams is a test-local $N binder so querysql tests do not
// depend on a backend ParamBinder implementation.
type dollarTestParams struct{}

func (dollarTestParams) Placeholder(n int) string { return fmt.Sprintf("$%d", n) }

type questionTestParams struct{}

func (questionTestParams) Placeholder(n int) string { return fmt.Sprintf("?%d", n) }

// TestCompileFilter_ParamBinderControlsPlaceholders proves the
// compiler's ParamBinder owns placeholder spelling and that Params is
// required (nil fails closed).
func TestCompileFilter_ParamBinderControlsPlaceholders(t *testing.T) {
	t.Parallel()

	resolver := recordingResolver(func(path querysql.FieldPath, _ querysql.TypeHint, ctx querysql.ResolveContext) (querysql.SQLExpr, error) {
		keyPh := ctx.Bind("team")
		return querysql.SQLExpr{SQL: path.String() + "[" + keyPh + "]"}, nil
	})

	t.Run("dollar", func(t *testing.T) {
		c := querysql.Compiler{Fields: resolver, Params: dollarTestParams{}}
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
			t.Errorf("SQL = %q, dollar binder must not emit ?", pred.SQL)
		}
		if len(pred.Args) != 2 || pred.Args[0] != "team" || pred.Args[1] != "platform" {
			t.Errorf("Args = %v, want [team platform]", pred.Args)
		}
	})

	t.Run("question", func(t *testing.T) {
		c := querysql.Compiler{Fields: resolver, Params: questionTestParams{}}
		pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: `resource.labels["team"] == "platform"`,
		})
		if err != nil {
			t.Fatalf("CompileFilter: %v", err)
		}
		if strings.Contains(pred.SQL, "$") {
			t.Errorf("SQL = %q, question binder must not emit $N", pred.SQL)
		}
		if !strings.Contains(pred.SQL, "?1") || !strings.Contains(pred.SQL, "?2") {
			t.Errorf("SQL = %q, want ?1 and ?2 numbered placeholders", pred.SQL)
		}
		for _, tok := range strings.FieldsFunc(pred.SQL, func(r rune) bool {
			return r == ' ' || r == '(' || r == ')' || r == ',' || r == '[' || r == ']'
		}) {
			if tok == "?" {
				t.Errorf("SQL = %q, question binder must not emit bare ?", pred.SQL)
				break
			}
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

	t.Run("nil Params required", func(t *testing.T) {
		c := querysql.Compiler{Fields: stubResolver{}}
		_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{
			Filter: `resourceType == "kind.fleetshift.io/Cluster"`,
		})
		if err == nil {
			t.Fatal("CompileFilter: got nil error, want Params required")
		}
		if !strings.Contains(err.Error(), "Params is required") {
			t.Errorf("err = %v, want Params is required", err)
		}
	})
}

type prefixBinder struct{ prefix string }

func (b prefixBinder) Placeholder(n int) string {
	return fmt.Sprintf("%s%d", b.prefix, n)
}
