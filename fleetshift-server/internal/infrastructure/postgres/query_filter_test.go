// This file tests queryFieldResolver -- this package's
// [querysql.FieldResolver] implementation, i.e. the actual
// FleetShift/Postgres row shape a filter's field paths resolve to
// (column names, JSONB extraction, label/condition map keys, safe
// numeric/boolean casts, GIN containment rewrites, and schema-backed
// path validation). It is an internal (package postgres) test file,
// rather than package postgres_test like this package's other tests,
// purely so it can construct queryFieldResolver directly without a
// database -- see querysql's package doc for why this split exists.
// End-to-end coverage against a real Postgres/SQLite database lives
// in queryrepotest.
package postgres

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres/querysql"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

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
	return compileWithResolver(t, querysql.Compiler{Fields: queryFieldResolver{}}, filter)
}

func compileErr(t *testing.T, filter string) error {
	t.Helper()
	return compileWithResolverErr(t, querysql.Compiler{Fields: queryFieldResolver{}}, filter)
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
		},
		{
			name:     "resource_type inequality keeps concatenated expression",
			filter:   `resource_type != "kind.fleetshift.io/Cluster"`,
			wantArgs: []any{"kind.fleetshift.io/Cluster"},
			wantSQL:  []string{"er.service_name || '/' || er.type_name"},
		},
		{
			name:     "resource_type in special-cases to constituent-column tuples",
			filter:   `resource_type in ["kind.fleetshift.io/Cluster", "kubernetes.fleetshift.io/Node"]`,
			wantArgs: []any{"kind.fleetshift.io", "Cluster", "kubernetes.fleetshift.io", "Node"},
			wantSQL:  []string{"(er.service_name, er.type_name) IN", "($1, $2)", "($3, $4)"},
			denySQL:  []string{"er.service_name || '/' || er.type_name"},
		},
		{
			name:     "name in special-cases to constituent-column tuples",
			filter:   `name in ["//kind.fleetshift.io/clusters/managed", "//kubernetes.fleetshift.io/nodes/n1"]`,
			wantArgs: []any{"kind.fleetshift.io", "clusters", "managed", "kubernetes.fleetshift.io", "nodes", "n1"},
			wantSQL:  []string{"(er.service_name, er.collection_name, er.resource_id) IN", "($1, $2, $3)", "($4, $5, $6)"},
		},
		{
			name:     "and",
			filter:   `resource_type == "kind.fleetshift.io/Cluster" && name == "//kind.fleetshift.io/clusters/managed"`,
			wantArgs: []any{"kind.fleetshift.io", "Cluster", "kind.fleetshift.io", "clusters", "managed"},
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
			for _, frag := range tt.wantSQL {
				if !strings.Contains(pred.SQL, frag) {
					t.Errorf("SQL = %q, want it to contain %q", pred.SQL, frag)
				}
			}
			for _, frag := range tt.denySQL {
				if strings.Contains(pred.SQL, frag) {
					t.Errorf("SQL = %q, must not contain %q", pred.SQL, frag)
				}
			}
			if strings.Contains(pred.SQL, "service_name || '/' || type_name") &&
				strings.Contains(tt.filter, "resource_type ==") {
				t.Errorf("SQL = %q, resource_type equality must not use the concatenated expression", pred.SQL)
			}
		})
	}
}

func TestQueryFieldResolver_OldPOCEnvelopeFieldsRejected(t *testing.T) {
	for _, filter := range []string{
		`kind == "extension"`,
		`platform_name == "clusters/managed"`,
		`service_name == "kind.fleetshift.io"`,
		`api_version == "v1"`,
		`collection_name == "clusters"`,
		`resource_id == "managed"`,
	} {
		err := compileErr(t, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

func TestQueryFieldResolver_ResourceLabelsContainment(t *testing.T) {
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
	if !strings.Contains(pred.SQL, "er.labels @> jsonb_build_object(") {
		t.Errorf("SQL = %q, want GIN-friendly containment rewrite", pred.SQL)
	}
	if strings.Contains(pred.SQL, "->>") {
		t.Errorf("SQL = %q, equality must not use ->> extraction", pred.SQL)
	}
}

func TestQueryFieldResolver_ResourceLabelsInequalityKeepsExtraction(t *testing.T) {
	pred := compile(t, `resource.labels["team"] != "platform"`)
	if !strings.Contains(pred.SQL, "er.labels ->>") {
		t.Errorf("SQL = %q, want ->> extraction for !=", pred.SQL)
	}
	if strings.Contains(pred.SQL, "@>") {
		t.Errorf("SQL = %q, != must not use containment rewrite", pred.SQL)
	}
}

func TestQueryFieldResolver_InventoryLabelsContainment(t *testing.T) {
	pred := compile(t, `resource.inventory.labels["node-role"] == "worker"`)
	if !strings.Contains(pred.SQL, "inv.labels @> jsonb_build_object(") {
		t.Errorf("SQL = %q, want GIN-friendly containment rewrite", pred.SQL)
	}
}

func TestQueryFieldResolver_InventoryConditionsContainment(t *testing.T) {
	pred := compile(t, `resource.inventory.conditions["Ready"].status == "True"`)
	if !strings.Contains(pred.SQL, "inv.conditions @> jsonb_build_object(") {
		t.Errorf("SQL = %q, want GIN-friendly containment rewrite", pred.SQL)
	}
	if len(pred.Args) != 3 {
		t.Fatalf("Args = %v, want 3 (condition type + subfield + value)", pred.Args)
	}
	if pred.Args[0] != "Ready" {
		t.Errorf("Args[0] = %v, want \"Ready\"", pred.Args[0])
	}
	if pred.Args[1] != "status" {
		t.Errorf("Args[1] = %v, want \"status\"", pred.Args[1])
	}
	if pred.Args[2] != "True" {
		t.Errorf("Args[2] = %v, want \"True\"", pred.Args[2])
	}
}

func TestQueryFieldResolver_SpecGuardedByResourceType(t *testing.T) {
	pred := compile(t, `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.provider == "aws"`)
	if !strings.Contains(pred.SQL, "ri.spec -> 'provider'") && !strings.Contains(pred.SQL, "ri.spec ->> 'provider'") {
		t.Errorf("SQL = %q, want a ri.spec ->> 'provider' extraction", pred.SQL)
	}
}

func TestQueryFieldResolver_SpecWithoutGuardIsInvalid(t *testing.T) {
	err := compileErr(t, `resource.spec.provider == "aws"`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestQueryFieldResolver_ObservationGuardedByResourceType(t *testing.T) {
	pred := compile(t, `resource_type == "kubernetes.fleetshift.io/Node" && resource.inventory.observation.capacity.cpu > 4`)
	if !strings.Contains(pred.SQL, "::numeric") {
		t.Errorf("SQL = %q, want a numeric cast for the int literal comparison", pred.SQL)
	}
	// resource_type equality binds service+type; observation comparison binds 4.
	if len(pred.Args) != 3 || pred.Args[2] != int64(4) {
		t.Errorf("Args = %v, want [<service>, <type>, 4]", pred.Args)
	}
}

func TestQueryFieldResolver_NumericJSONCastIsGuardedAgainstInvalidInput(t *testing.T) {
	for _, tt := range []struct {
		name    string
		filter  string
		sqlType string
	}{
		{
			name:    "observation numeric comparison",
			filter:  `resource_type == "kubernetes.fleetshift.io/Node" && resource.inventory.observation.capacity.cpu > 4`,
			sqlType: "numeric",
		},
		{
			name:    "spec numeric comparison",
			filter:  `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.count > 4`,
			sqlType: "numeric",
		},
		{
			name:    "observation boolean comparison",
			filter:  `resource_type == "kubernetes.fleetshift.io/Node" && resource.inventory.observation.healthy == true`,
			sqlType: "boolean",
		},
		{
			name:    "labels have no type guard at all, but share their column across every type",
			filter:  `resource.labels["priority"] > 4`,
			sqlType: "numeric",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pred := compile(t, tt.filter)
			wantGuard := "pg_input_is_valid("
			if !strings.Contains(pred.SQL, wantGuard) {
				t.Errorf("SQL = %q, want a pg_input_is_valid(...) guard before the cast", pred.SQL)
			}
			if !strings.Contains(pred.SQL, "'"+tt.sqlType+"'") {
				t.Errorf("SQL = %q, want pg_input_is_valid's type argument to be %q", pred.SQL, tt.sqlType)
			}
			if !strings.Contains(pred.SQL, "CASE WHEN pg_input_is_valid") {
				t.Errorf("SQL = %q, want the ::%s cast wrapped in a CASE WHEN pg_input_is_valid(...) guard", pred.SQL, tt.sqlType)
			}
		})
	}
}

func TestQueryFieldResolver_ObservationWithoutGuardIsInvalid(t *testing.T) {
	err := compileErr(t, `resource.inventory.observation.capacity.cpu > 4`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestQueryFieldResolver_SpecValidatedAgainstSchemaWhenAvailable(t *testing.T) {
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")
	catalog := application.NewQuerySchemaCatalog()
	catalog.Register(domain.ResourceQuerySchema{
		ResourceType:   rt,
		SpecDescriptor: (&timestamppb.Timestamp{}).ProtoReflect().Descriptor(),
	})
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: catalog}}

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
	catalog := application.NewQuerySchemaCatalog()
	catalog.Register(domain.ResourceQuerySchema{
		ResourceType:   rt,
		SpecDescriptor: specTestDescriptor(t),
	})
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: catalog}}

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

// nestedSpecTestDescriptor has a singular nested message (valid to
// traverse), a repeated message field, and a string-to-message map.
// The compiler has no list/map traversal semantics, so continuing
// through the latter two must fail closed even though both are
// MessageKind (maps expose a synthetic map-entry message).
func nestedSpecTestDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	const src = `
syntax = "proto3";
package querysql.test;
message NestedSpec {
  message Item {
    string name = 1;
  }
  message Nested {
    string value = 1;
  }
  Nested nested = 1;
  repeated Item items = 2;
  map<string, Item> labels = 3;
}
`
	desc, err := dynamicapi.CompileInline(context.Background(),
		map[string]string{"nested_spec.proto": src}, "nested_spec.proto", "querysql.test.NestedSpec")
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}
	return desc.Message
}

func TestQueryFieldResolver_SpecRejectsTraversalThroughRepeatedOrMap(t *testing.T) {
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")
	catalog := application.NewQuerySchemaCatalog()
	catalog.Register(domain.ResourceQuerySchema{
		ResourceType:   rt,
		SpecDescriptor: nestedSpecTestDescriptor(t),
	})
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: catalog}}

	pred := compileWithResolver(t, c, `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.nested.value == "x"`)
	if !strings.Contains(pred.SQL, "ri.spec -> 'nested' ->> 'value'") {
		t.Errorf("SQL = %q, want singular message traversal to remain valid", pred.SQL)
	}

	// Terminal selection of a repeated/map field is still allowed;
	// only continuing through them is rejected.
	compileWithResolver(t, c, `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.items == "x"`)
	compileWithResolver(t, c, `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.labels == "x"`)

	for _, filter := range []string{
		`resource_type == "kind.fleetshift.io/Cluster" && resource.spec.items.name == "x"`,
		`resource_type == "kind.fleetshift.io/Cluster" && resource.spec.labels.name == "x"`,
		`resource_type == "kind.fleetshift.io/Cluster" && resource.spec.labels["team"].name == "x"`,
	} {
		err := compileWithResolverErr(t, c, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

func TestQueryFieldResolver_SpecPermissiveWhenSchemaAbsent(t *testing.T) {
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: application.NewQuerySchemaCatalog()}}
	compileWithResolver(t, c, `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.anything_goes == 5`)
}

func TestQueryFieldResolver_GuardInsideOrDoesNotCount(t *testing.T) {
	err := compileErr(t, `(resource_type == "kind.fleetshift.io/Cluster") || resource.spec.provider == "aws"`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestQueryFieldResolver_ResourceNameAndUID(t *testing.T) {
	pred := compile(t, `resource.name == "clusters/managed"`)
	if !strings.Contains(pred.SQL, "er.collection_name || '/' || er.resource_id") {
		t.Errorf("SQL = %q, want resource.name to map to relative name expression", pred.SQL)
	}

	pred = compile(t, `resource.uid == "11111111-1111-1111-1111-111111111111"`)
	if !strings.Contains(pred.SQL, "er.uid::text") {
		t.Errorf("SQL = %q, want resource.uid to map to er.uid::text", pred.SQL)
	}
}

func TestQueryFieldResolver_ManagedFields(t *testing.T) {
	for _, tt := range []struct {
		filter string
		column string
	}{
		{`resource.intent_version == 1`, "erm.current_version"},
		{`resource.state == "active"`, "f.state"},
		{`resource.pause_reason == "manual"`, "f.pause_reason"},
		{`resource.generation == 3`, "f.generation"},
	} {
		pred := compile(t, tt.filter)
		if !strings.Contains(pred.SQL, tt.column) {
			t.Errorf("filter %q: SQL = %q, want it to reference column %q", tt.filter, pred.SQL, tt.column)
		}
	}
}

func TestQueryFieldResolver_UnsupportedField(t *testing.T) {
	for _, filter := range []string{
		`not_a_real_field == "x"`,
		`resource.aliases == "x"`,
		`resource.effective_labels["env"] == "x"`,
		`resource.representations == "x"`,
		`resource.relationships == "x"`,
	} {
		err := compileErr(t, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

func TestQueryFieldResolver_UnsupportedFieldInEmptyList(t *testing.T) {
	for _, filter := range []string{
		`not_a_real_field in []`,
		`resource.aliases in []`,
	} {
		err := compileErr(t, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
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

func TestQueryFieldResolver_ConditionKeyInjectionAttempt(t *testing.T) {
	const payload = `Ready'; DROP TABLE extension_resource_inventory; --`
	pred := compile(t, `resource.inventory.conditions["`+strings.ReplaceAll(payload, `"`, `\"`)+`"].status == "True"`)
	if strings.Contains(pred.SQL, "DROP TABLE") {
		t.Errorf("SQL = %q, want the condition key kept out of SQL text entirely", pred.SQL)
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
