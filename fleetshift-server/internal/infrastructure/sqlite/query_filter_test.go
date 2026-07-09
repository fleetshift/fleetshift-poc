// This file tests queryFieldResolver -- this package's
// [querysql.FieldResolver] implementation for SQLite (json_extract /
// ->>, QuestionParams, safe numeric/boolean casts without
// pg_input_is_valid). End-to-end coverage against a real SQLite
// database lives in queryrepotest.
package sqlite

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

// staticQuerySchemas is a minimal [domain.QuerySchemaProvider] for
// field-resolver tests. Kept local so this package does not import
// transport/managedresource (layering: infrastructure → domain only).
type staticQuerySchemas map[domain.ResourceType]domain.ResourceQuerySchema

func (s staticQuerySchemas) GetResourceQuerySchema(_ context.Context, rt domain.ResourceType) (domain.ResourceQuerySchema, bool, error) {
	schema, ok := s[rt]
	return schema, ok, nil
}

func (s staticQuerySchemas) ListResourceQuerySchemas(_ context.Context) ([]domain.ResourceQuerySchema, error) {
	out := make([]domain.ResourceQuerySchema, 0, len(s))
	for _, schema := range s {
		out = append(out, schema)
	}
	return out, nil
}

func compileWithResolver(t *testing.T, c querysql.Compiler, filter string) querysql.SQLPredicate {
	t.Helper()
	pred, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: filter})
	if err != nil {
		t.Fatalf("CompileFilter(%q): unexpected error: %v", filter, err)
	}
	return pred
}

func compileWithResolverErr(t *testing.T, c querysql.Compiler, filter string) error {
	t.Helper()
	_, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: filter})
	if err == nil {
		t.Fatalf("CompileFilter(%q): got nil error, want an error", filter)
	}
	return err
}

func compile(t *testing.T, filter string) querysql.SQLPredicate {
	t.Helper()
	return compileWithResolver(t, querysql.Compiler{
		Fields: queryFieldResolver{},
		Params: querysql.QuestionParams{},
	}, filter)
}

func compileErr(t *testing.T, filter string) error {
	t.Helper()
	return compileWithResolverErr(t, querysql.Compiler{
		Fields: queryFieldResolver{},
		Params: querysql.QuestionParams{},
	}, filter)
}

func TestQueryFieldResolver_EnvelopeFields(t *testing.T) {
	tests := []struct {
		name     string
		filter   string
		wantArgs []any
		wantSQL  []string
		denySQL  []string
	}{
		{
			name:     "name equality special-cases to constituent columns",
			filter:   `name == "//kind.fleetshift.io/clusters/managed"`,
			wantArgs: []any{"kind.fleetshift.io", "clusters", "managed"},
			wantSQL:  []string{"er.service_name =", "er.collection_name =", "er.resource_id ="},
		},
		{
			name:     "resource_type equality special-cases to constituent columns",
			filter:   `resource_type == "kind.fleetshift.io/Cluster"`,
			wantArgs: []any{"kind.fleetshift.io", "Cluster"},
			wantSQL:  []string{"er.service_name =", "er.type_name ="},
			denySQL:  []string{"er.service_name || '/' || er.type_name"},
		},
		{
			name:     "resource_type inequality keeps concatenated expression",
			filter:   `resource_type != "kind.fleetshift.io/Cluster"`,
			wantArgs: []any{"kind.fleetshift.io/Cluster"},
			wantSQL:  []string{"er.service_name || '/' || er.type_name"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pred := compile(t, tt.filter)
			for _, frag := range tt.wantSQL {
				if !strings.Contains(pred.SQL, frag) {
					t.Errorf("SQL = %q, want substring %q", pred.SQL, frag)
				}
			}
			for _, frag := range tt.denySQL {
				if strings.Contains(pred.SQL, frag) {
					t.Errorf("SQL = %q, must not contain %q", pred.SQL, frag)
				}
			}
			if len(pred.Args) != len(tt.wantArgs) {
				t.Fatalf("Args = %v, want %v", pred.Args, tt.wantArgs)
			}
			for i, want := range tt.wantArgs {
				if pred.Args[i] != want {
					t.Errorf("Args[%d] = %v, want %v", i, pred.Args[i], want)
				}
			}
			if strings.Contains(pred.SQL, "$") {
				t.Errorf("SQL = %q, want QuestionParams (?) not DollarParams", pred.SQL)
			}
		})
	}
}

func TestQueryFieldResolver_ResourceLabelsUseJSONExtract(t *testing.T) {
	pred := compile(t, `resource.labels["team"] == "platform"`)
	if len(pred.Args) != 2 {
		t.Fatalf("Args = %v, want 2 (label key + comparison value)", pred.Args)
	}
	if pred.Args[0] != "team" {
		t.Errorf("Args[0] = %v, want \"team\" (the label key)", pred.Args[0])
	}
	if pred.Args[1] != "platform" {
		t.Errorf("Args[1] = %v, want \"platform\"", pred.Args[1])
	}
	if !strings.Contains(pred.SQL, "json_extract(er.labels") {
		t.Errorf("SQL = %q, want json_extract for label equality (no GIN)", pred.SQL)
	}
	if strings.Contains(pred.SQL, "@>") {
		t.Errorf("SQL = %q, SQLite must not use Postgres JSONB containment", pred.SQL)
	}
}

func TestQueryFieldResolver_HyphenatedLabelKey(t *testing.T) {
	pred := compile(t, `resource.inventory.labels["node-role"] == "worker"`)
	if !strings.Contains(pred.SQL, `'$."'`) {
		t.Errorf("SQL = %q, want quoted JSON path segment for hyphenated keys", pred.SQL)
	}
	if pred.Args[0] != "node-role" {
		t.Errorf("Args[0] = %v, want \"node-role\"", pred.Args[0])
	}
}

func TestQueryFieldResolver_InventoryConditionsUseJSONExtract(t *testing.T) {
	pred := compile(t, `resource.inventory.conditions["Ready"].status == "True"`)
	if !strings.Contains(pred.SQL, "json_extract(inv.conditions") {
		t.Errorf("SQL = %q, want json_extract for condition subfields", pred.SQL)
	}
	if strings.Contains(pred.SQL, "@>") {
		t.Errorf("SQL = %q, SQLite must not use Postgres JSONB containment", pred.SQL)
	}
	if len(pred.Args) != 2 {
		t.Fatalf("Args = %v, want 2 (condition type + value; subfield is inlined from whitelist)", pred.Args)
	}
	if pred.Args[0] != "Ready" {
		t.Errorf("Args[0] = %v, want \"Ready\"", pred.Args[0])
	}
	if pred.Args[1] != "True" {
		t.Errorf("Args[1] = %v, want \"True\"", pred.Args[1])
	}
}

func TestQueryFieldResolver_SpecGuardedByResourceType(t *testing.T) {
	pred := compile(t, `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.provider == "aws"`)
	if !strings.Contains(pred.SQL, "ri.spec ->> 'provider'") && !strings.Contains(pred.SQL, "ri.spec -> 'provider'") {
		t.Errorf("SQL = %q, want a ri.spec ->> 'provider' extraction", pred.SQL)
	}
}

func TestQueryFieldResolver_SpecWithoutGuardIsInvalid(t *testing.T) {
	err := compileErr(t, `resource.spec.provider == "aws"`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestQueryFieldResolver_NumericJSONCastIsGuardedAgainstInvalidInput(t *testing.T) {
	for _, tt := range []struct {
		name   string
		filter string
	}{
		{
			name:   "observation numeric comparison",
			filter: `resource_type == "kubernetes.fleetshift.io/Node" && resource.inventory.observation.capacity.cpu > 4`,
		},
		{
			name:   "spec numeric comparison",
			filter: `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.count > 4`,
		},
		{
			name:   "labels have no type guard at all, but share their column across every type",
			filter: `resource.labels["priority"] > 4`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pred := compile(t, tt.filter)
			if strings.Contains(pred.SQL, "pg_input_is_valid") {
				t.Errorf("SQL = %q, SQLite must not use pg_input_is_valid", pred.SQL)
			}
			if !strings.Contains(pred.SQL, "typeof(") {
				t.Errorf("SQL = %q, want a typeof(...) guard before the numeric cast", pred.SQL)
			}
			if !strings.Contains(pred.SQL, "CAST(") || !strings.Contains(pred.SQL, "AS REAL") {
				t.Errorf("SQL = %q, want CAST(... AS REAL) for numeric comparison", pred.SQL)
			}
		})
	}
}

func TestQueryFieldResolver_BooleanJSONCast(t *testing.T) {
	pred := compile(t, `resource_type == "kind.fleetshift.io/Cluster" && resource.inventory.observation.healthy == true`)
	if !strings.Contains(pred.SQL, "typeof(") {
		t.Errorf("SQL = %q, want typeof guard for boolean cast", pred.SQL)
	}
	if strings.Contains(pred.SQL, "pg_input_is_valid") {
		t.Errorf("SQL = %q, SQLite must not use pg_input_is_valid", pred.SQL)
	}
}

func TestQueryFieldResolver_SpecValidatedAgainstSchemaWhenAvailable(t *testing.T) {
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")
	schemas := staticQuerySchemas{
		rt: {
			ResourceType:   rt,
			APIVersion:     "v1",
			SpecDescriptor: (&timestamppb.Timestamp{}).ProtoReflect().Descriptor(),
		},
	}
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: schemas}, Params: querysql.QuestionParams{}}

	pred := compileWithResolver(t, c, `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.seconds == 5`)
	if !strings.Contains(pred.SQL, "ri.spec ->> 'seconds'") {
		t.Errorf("SQL = %q, want a ri.spec ->> 'seconds' extraction", pred.SQL)
	}

	err := compileWithResolverErr(t, c, `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.bogus_field == 5`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("CompileFilter with an unknown field: err = %v, want ErrInvalidArgument", err)
	}
}

func specTestDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	const src = `
syntax = "proto3";
package querysql.test;
message TestSpec {
  string api_server_port = 1;
}
`
	desc, err := dynamicapi.CompileInline(context.Background(),
		map[string]string{"test_spec.proto": src}, "test_spec.proto", "querysql.test.TestSpec")
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}
	return desc.Message
}

func TestQueryFieldResolver_SpecUsesJSONNameNotProtoNameForExtraction(t *testing.T) {
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")
	schemas := staticQuerySchemas{
		rt: {
			ResourceType:   rt,
			APIVersion:     "v1",
			SpecDescriptor: specTestDescriptor(t),
		},
	}
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: schemas}, Params: querysql.QuestionParams{}}

	for _, tt := range []struct {
		name   string
		filter string
	}{
		{"proto (snake_case) name", `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.api_server_port == "6443"`},
		{"JSON (camelCase) name", `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.apiServerPort == "6443"`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pred := compileWithResolver(t, c, tt.filter)
			if !strings.Contains(pred.SQL, "ri.spec ->> 'apiServerPort'") {
				t.Errorf("SQL = %q, want extraction keyed on the JSON name 'apiServerPort', not the proto name", pred.SQL)
			}
			if strings.Contains(pred.SQL, "'api_server_port'") {
				t.Errorf("SQL = %q, must not key JSON extraction on the proto (underscore) name", pred.SQL)
			}
		})
	}
}

func TestQueryFieldResolver_ResourceUIDIsPlainText(t *testing.T) {
	pred := compile(t, `resource.uid == "11111111-1111-1111-1111-111111111111"`)
	if !strings.Contains(pred.SQL, "er.uid") {
		t.Errorf("SQL = %q, want resource.uid to map to er.uid", pred.SQL)
	}
	if strings.Contains(pred.SQL, "er.uid::text") {
		t.Errorf("SQL = %q, SQLite uid is already TEXT; must not use Postgres ::text cast", pred.SQL)
	}
}

func TestQueryFieldResolver_InjectionAttempt(t *testing.T) {
	const payload = `team"; DROP TABLE extension_resources; --`
	pred := compile(t, `resource.labels["`+strings.ReplaceAll(payload, `"`, `\"`)+`"] == "x"`)
	if strings.Contains(pred.SQL, "DROP TABLE") {
		t.Errorf("SQL = %q, want the label key kept out of SQL text entirely", pred.SQL)
	}
	found := false
	for _, a := range pred.Args {
		if a == payload {
			found = true
		}
	}
	if !found {
		t.Errorf("Args = %v, want the raw payload bound as a parameter", pred.Args)
	}
}
