// This file tests queryFieldResolver -- this package's
// [querysql.FieldResolver] implementation, i.e. the actual
// FleetShift/Postgres row shape a filter's field paths resolve to
// (column names, JSONB extraction, label/condition map keys, safe
// numeric/boolean casts, and schema-backed path validation). It is an
// internal (package postgres) test file, rather than package
// postgres_test like this package's other tests, purely so it can
// construct queryFieldResolver directly without a database -- see
// querysql's package doc for why this split exists. End-to-end
// coverage against a real Postgres/SQLite database lives in
// queryrepotest.
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
	}{
		{"kind equals", `kind == "extension"`, []any{"extension"}},
		{"kind not equals", `kind != "extension"`, []any{"extension"}},
		{"and", `kind == "extension" && resource_type == "kind.fleetshift.io/Cluster"`, []any{"extension", "kind.fleetshift.io/Cluster"}},
		{"or", `kind == "extension" || kind == "platform"`, []any{"extension", "platform"}},
		{"not", `!(kind == "platform")`, []any{"platform"}},
		{"literal on left flips operator", `"clusters/managed" == platform_name`, []any{"clusters/managed"}},
		{"in list", `kind in ["platform", "extension"]`, []any{"platform", "extension"}},
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

func TestQueryFieldResolver_ResourceLabels(t *testing.T) {
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
	if !strings.Contains(pred.SQL, "resource_labels ->>") {
		t.Errorf("SQL = %q, want it to extract via resource_labels ->>", pred.SQL)
	}
}

func TestQueryFieldResolver_InventoryLabels(t *testing.T) {
	pred := compile(t, `resource.inventory.labels["node-role"] == "worker"`)
	if !strings.Contains(pred.SQL, "inventory_labels ->>") {
		t.Errorf("SQL = %q, want it to extract via inventory_labels ->>", pred.SQL)
	}
}

func TestQueryFieldResolver_InventoryConditions(t *testing.T) {
	pred := compile(t, `resource.inventory.conditions["Ready"].status == "True"`)
	if !strings.Contains(pred.SQL, "inventory_conditions ->") || !strings.Contains(pred.SQL, "->> 'status'") {
		t.Errorf("SQL = %q, want inventory_conditions extraction with ->> 'status'", pred.SQL)
	}
	if len(pred.Args) != 2 {
		t.Fatalf("Args = %v, want 2 (condition type + comparison value)", pred.Args)
	}
	if pred.Args[0] != "Ready" {
		t.Errorf("Args[0] = %v, want \"Ready\"", pred.Args[0])
	}
}

func TestQueryFieldResolver_SpecGuardedByResourceType(t *testing.T) {
	pred := compile(t, `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.provider == "aws"`)
	if !strings.Contains(pred.SQL, "spec -> 'provider'") && !strings.Contains(pred.SQL, "spec ->> 'provider'") {
		t.Errorf("SQL = %q, want a spec ->> 'provider' extraction", pred.SQL)
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
	if len(pred.Args) != 2 || pred.Args[1] != int64(4) {
		t.Errorf("Args = %v, want [<resource_type>, 4]", pred.Args)
	}
}

// TestQueryFieldResolver_NumericJSONCastIsGuardedAgainstInvalidInput
// proves numeric/boolean casts over JSON-text-extracted fields never
// compile to a bare `(expr)::numeric`/`(expr)::boolean`: a plain
// WHERE-clause AND gives Postgres no evaluation-order guarantee, so a
// resource_type guard elsewhere in the filter cannot be trusted to
// keep such a cast from being attempted against a differently-typed
// row's incompatible value at the same JSON path (see
// safeJSONCast's doc, and queryrepotest's
// NumericComparisonSafeAcrossConflictingResourceTypes for the
// corresponding end-to-end Postgres proof).
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
			wantGuard := "pg_input_is_valid(" // followed by the extraction expr and the sqlType
			if !strings.Contains(pred.SQL, wantGuard) {
				t.Errorf("SQL = %q, want a pg_input_is_valid(...) guard before the cast", pred.SQL)
			}
			if !strings.Contains(pred.SQL, "'"+tt.sqlType+"'") {
				t.Errorf("SQL = %q, want pg_input_is_valid's type argument to be %q", pred.SQL, tt.sqlType)
			}
			// The cast itself must be reachable only from inside a
			// CASE, never a bare top-level conjunct.
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

// TestQueryFieldResolver_SpecValidatedAgainstSchemaWhenAvailable
// proves that once a [domain.QuerySchemaProvider] has a descriptor
// registered for the guarded resource type, resource.spec.<path>
// segments are validated against real field names rather than
// accepted structurally.
func TestQueryFieldResolver_SpecValidatedAgainstSchemaWhenAvailable(t *testing.T) {
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")
	catalog := application.NewQuerySchemaCatalog()
	// timestamppb.Timestamp is a real, already-available descriptor
	// with known fields (seconds, nanos) -- reused here purely as a
	// stand-in message shape, not because specs are timestamps.
	catalog.Register(domain.ResourceQuerySchema{
		ResourceType:   rt,
		SpecDescriptor: (&timestamppb.Timestamp{}).ProtoReflect().Descriptor(),
	})
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: catalog}}

	pred := compileWithResolver(t, c, `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.seconds == 5`)
	if !strings.Contains(pred.SQL, "spec ->> 'seconds'") {
		t.Errorf("SQL = %q, want a spec ->> 'seconds' extraction", pred.SQL)
	}

	err := compileWithResolverErr(t, c, `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.bogus_field == 5`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("CompileFilter with an unknown field: err = %v, want ErrInvalidArgument", err)
	}
}

// specTestDescriptor compiles a tiny real proto message with a
// snake_case field (api_server_port) via the same protocompile-based
// path addons use (see dynamicapi.CompileInline), so its JSON name
// (apiServerPort) comes from the real proto3 JSON-name defaulting
// rule rather than something hand-picked for the test.
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

// TestQueryFieldResolver_SpecUsesJSONNameNotProtoNameForExtraction
// proves that resource.spec.<path> extracts using the field's JSON
// name (apiServerPort) even when the filter author spells the path
// with the proto field's underscore name (api_server_port) -- both
// must hit the same stored JSON key, since registrar.go's
// protojson.Marshal (default options) writes specs keyed by JSON
// name. See validateDescriptorPath's doc.
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
			if !strings.Contains(pred.SQL, "spec ->> 'apiServerPort'") {
				t.Errorf("SQL = %q, want extraction keyed on the JSON name 'apiServerPort', not the proto name", pred.SQL)
			}
			if strings.Contains(pred.SQL, "'api_server_port'") {
				t.Errorf("SQL = %q, must not key JSON extraction on the proto (underscore) name", pred.SQL)
			}
		})
	}
}

// TestQueryFieldResolver_SpecPermissiveWhenSchemaAbsent proves the
// documented fallback: a configured provider with nothing registered
// for the guarded type still validates resource.spec.<path>
// structurally only, rather than rejecting every field as unknown.
func TestQueryFieldResolver_SpecPermissiveWhenSchemaAbsent(t *testing.T) {
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: application.NewQuerySchemaCatalog()}}
	compileWithResolver(t, c, `resource_type == "kind.fleetshift.io/Cluster" && resource.spec.anything_goes == 5`)
}

func TestQueryFieldResolver_GuardInsideOrDoesNotCount(t *testing.T) {
	// The resource_type guard only counts when it's a top-level `&&`
	// conjunct; inside an `||` branch it doesn't establish the type
	// for the whole expression.
	err := compileErr(t, `(resource_type == "kind.fleetshift.io/Cluster") || resource.spec.provider == "aws"`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("err = %v, want ErrInvalidArgument", err)
	}
}

func TestQueryFieldResolver_ResourceNameAndUID(t *testing.T) {
	pred := compile(t, `resource.name == "clusters/managed"`)
	if !strings.Contains(pred.SQL, "platform_name") {
		t.Errorf("SQL = %q, want resource.name to map to platform_name", pred.SQL)
	}

	pred = compile(t, `resource.uid == "11111111-1111-1111-1111-111111111111"`)
	if !strings.Contains(pred.SQL, "extension_uid::text") {
		t.Errorf("SQL = %q, want resource.uid to map to extension_uid::text", pred.SQL)
	}
}

func TestQueryFieldResolver_ManagedFields(t *testing.T) {
	for _, tt := range []struct {
		filter string
		column string
	}{
		{`resource.intent_version == 1`, "intent_version"},
		{`resource.state == "active"`, "fulfillment_state"},
		{`resource.pause_reason == "manual"`, "pause_reason"},
		{`resource.generation == 3`, "generation"},
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
	} {
		err := compileErr(t, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

// TestQueryFieldResolver_UnsupportedFieldInEmptyList proves an
// unsupported/typo'd field is rejected even when compared against an
// empty "in" list. An empty list is always a constant-false
// predicate, but the field must still be resolved (and thus
// validated) to get there -- otherwise `not_a_real_field in []` would
// silently compile instead of failing closed like every other shape
// referencing that field does.
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

// TestQueryFieldResolver_InjectionAttempt proves that a label key
// containing SQL metacharacters becomes a bind parameter rather than
// SQL text: the compiled SQL must not contain the raw payload at all.
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
