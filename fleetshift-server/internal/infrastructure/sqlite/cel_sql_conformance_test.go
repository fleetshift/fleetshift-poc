package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
)

// TestCELSQLConformance_CanonicalJSONNames proves representative
// supported filters select the same rows when evaluated with cel-go
// over the documented ProtoJSON-shaped activation and when lowered to
// SQL against JSON that uses the same keys.
func TestCELSQLConformance_CanonicalJSONNames(t *testing.T) {
	db := OpenTestDB(t)
	if _, err := db.Exec(`
CREATE TABLE conformance (
  id TEXT PRIMARY KEY,
  service_name TEXT NOT NULL,
  type_name TEXT NOT NULL,
  labels TEXT NOT NULL,
  spec TEXT NOT NULL
);
INSERT INTO conformance (id, service_name, type_name, labels, spec) VALUES
  ('a', 'kind.fleetshift.io', 'Cluster', '{"node_role":"worker","nodeRole":"other","seenAt":"2026-06-01T12:00:00Z"}', '{"apiServerPort":"6443","api_server_port":"wrong","value":"5","flag":true,"when":"2026-06-01T12:00:00Z","badWhen":"2026-06-01"}'),
  ('b', 'kind.fleetshift.io', 'Cluster', '{"node_role":"control","seenAt":"2026-06-01T08:00:00-04:00"}', '{"apiServerPort":"8443","value":5,"when":"2026-06-01T08:00:00-04:00","badWhen":"infinity"}'),
  ('c', 'other.fleetshift.io', 'Widget', '{"node_role":"worker"}', '{"apiServerPort":"6443","value":null}');
`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	env, err := cel.NewEnv(
		cel.EagerlyValidateDeclarations(true),
		cel.ASTValidators(cel.ValidateTimestampLiterals()),
		cel.Variable("name", cel.StringType),
		cel.Variable("resourceType", cel.StringType),
		cel.Variable("resource", cel.DynType),
	)
	if err != nil {
		t.Fatalf("cel env: %v", err)
	}

	type row struct {
		id  string
		act map[string]any
	}
	rows := []row{
		{
			id: "a",
			act: map[string]any{
				"name":         "//kind.fleetshift.io/clusters/a",
				"resourceType": "kind.fleetshift.io/Cluster",
				"resource": map[string]any{
					"labels": map[string]any{"node_role": "worker", "nodeRole": "other", "seenAt": "2026-06-01T12:00:00Z"},
					"spec": map[string]any{
						"apiServerPort":   "6443",
						"api_server_port": "wrong",
						"value":           "5",
						"flag":            true,
						"when":            "2026-06-01T12:00:00Z",
						"badWhen":         "2026-06-01",
					},
				},
			},
		},
		{
			id: "b",
			act: map[string]any{
				"name":         "//kind.fleetshift.io/clusters/b",
				"resourceType": "kind.fleetshift.io/Cluster",
				"resource": map[string]any{
					"labels": map[string]any{"node_role": "control", "seenAt": "2026-06-01T08:00:00-04:00"},
					"spec": map[string]any{
						"apiServerPort": "8443",
						"value":         float64(5), // JSON numbers are float64 in Go maps
						"when":          "2026-06-01T08:00:00-04:00",
						"badWhen":       "infinity",
					},
				},
			},
		},
		{
			id: "c",
			act: map[string]any{
				"name":         "//other.fleetshift.io/widgets/c",
				"resourceType": "other.fleetshift.io/Widget",
				"resource": map[string]any{
					"labels": map[string]any{"node_role": "worker"},
					"spec":   map[string]any{"apiServerPort": "6443", "value": nil},
				},
			},
		},
	}

	compiler := querysql.Compiler{
		Fields: queryFieldResolver{},
		Params: querysql.QuestionParams{},
	}

	for _, tt := range []struct {
		filter string
		want   []string
	}{
		{
			filter: `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.apiServerPort == "6443"`,
			want:   []string{"a"},
		},
		{
			filter: `resourceType == "kind.fleetshift.io/Cluster" && resource.spec["apiServerPort"] == "6443"`,
			want:   []string{"a"},
		},
		{
			filter: `resource.labels.node_role == "worker"`,
			want:   []string{"a", "c"},
		},
		{
			filter: `resource.labels["node_role"] == "worker"`,
			want:   []string{"a", "c"},
		},
		{
			filter: `resource.labels.nodeRole == "other"`,
			want:   []string{"a"},
		},
		// Heterogeneous JSON != : string "5" differs from number 5.
		{
			filter: `resource.spec.value != 5`,
			want:   []string{"a", "c"},
		},
		{
			filter: `resource.spec.value == 5`,
			want:   []string{"b"},
		},
		{
			filter: `resource.spec.value == "5"`,
			want:   []string{"a"},
		},
		// Missing path stays a non-match even for != .
		{
			filter: `resource.spec.missing != 5`,
			want:   nil,
		},
		{
			filter: `true || resource.spec.missing != 5`,
			want:   []string{"a", "b", "c"},
		},
		{
			filter: `false && resource.spec.missing != 5`,
			want:   nil,
		},
		{
			filter: `timestamp(resource.spec.when) == timestamp("2026-06-01T08:00:00-04:00")`,
			want:   []string{"a", "b"},
		},
		{
			filter: `timestamp(resource.spec.when) < timestamp("2026-06-01T13:00:00Z")`,
			want:   []string{"a", "b"},
		},
		// Nanosecond-preserving dynamic timestamp inequality.
		{
			filter: `timestamp(resource.spec.when) == timestamp("2026-06-01T12:00:00.123456789Z")`,
			want:   nil,
		},
		// Full CEL year range (unix-nanos would wrap).
		{
			filter: `timestamp(resource.spec.when) > timestamp("0001-01-01T00:00:00Z")`,
			want:   []string{"a", "b"},
		},
		{
			filter: `timestamp(resource.spec.when) < timestamp("9999-12-31T23:59:59.999999999Z")`,
			want:   []string{"a", "b"},
		},
		// timestamp() on known string label values.
		{
			filter: `timestamp(resource.labels.seenAt) == timestamp("2026-06-01T12:00:00Z")`,
			want:   []string{"a", "b"},
		},
		// Invalid / non-RFC3339 dynamic strings are non-matches.
		{
			filter: `timestamp(resource.spec.badWhen) == timestamp("2026-06-01T12:00:00Z")`,
			want:   nil,
		},
		{
			filter: `true || timestamp(resource.spec.badWhen) == timestamp("2026-06-01T12:00:00Z")`,
			want:   []string{"a", "b", "c"},
		},
	} {
		t.Run(tt.filter, func(t *testing.T) {
			ast, iss := env.Compile(tt.filter)
			if iss != nil && iss.Err() != nil {
				t.Fatalf("cel compile: %v", iss.Err())
			}
			prg, err := env.Program(ast)
			if err != nil {
				t.Fatalf("cel program: %v", err)
			}

			var celMatches []string
			for _, r := range rows {
				out, _, err := prg.Eval(r.act)
				if err != nil {
					// Missing map keys error in CEL; treat as non-match,
					// matching SQL JSON extraction that yields NULL.
					continue
				}
				if b, ok := out.Value().(bool); ok && b {
					celMatches = append(celMatches, r.id)
				}
			}

			pred, err := compiler.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: tt.filter})
			if err != nil {
				t.Fatalf("CompileFilter: %v", err)
			}
			q := `SELECT c.id FROM conformance c WHERE ` + rewriteConformanceSQL(pred.SQL)
			sqlMatches := queryIDs(t, db, q, pred.Args...)
			if !stringSlicesEqual(celMatches, tt.want) {
				t.Errorf("CEL matches = %v, want %v", celMatches, tt.want)
			}
			if !stringSlicesEqual(sqlMatches, tt.want) {
				t.Errorf("SQL matches = %v, want %v (SQL=%q args=%v)", sqlMatches, tt.want, pred.SQL, pred.Args)
			}
			if !stringSlicesEqual(celMatches, sqlMatches) {
				t.Errorf("CEL/SQL diverge: cel=%v sql=%v", celMatches, sqlMatches)
			}
		})
	}
}

// rewriteConformanceSQL maps the resolver's er/ri table aliases onto
// the conformance fixture columns.
func rewriteConformanceSQL(sql string) string {
	out := sql
	out = strings.ReplaceAll(out, "er.service_name", "c.service_name")
	out = strings.ReplaceAll(out, "er.type_name", "c.type_name")
	out = strings.ReplaceAll(out, "er.labels", "c.labels")
	out = strings.ReplaceAll(out, "ri.spec", "c.spec")
	return out
}

func queryIDs(t *testing.T, db *sql.DB, q string, args ...any) []string {
	t.Helper()
	rows, err := db.Query(q, args...)
	if err != nil {
		t.Fatalf("query %q args=%v: %v", q, args, err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return ids
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
