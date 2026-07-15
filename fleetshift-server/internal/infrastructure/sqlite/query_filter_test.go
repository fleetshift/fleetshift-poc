// This file tests queryFieldResolver SQL generation only: json_extract
// / ->>, numbered questionParams ?N, safe casts via json_valid /
// json_type, norm-sibling paths, and SchemaProvider-backed path
// validation. Cross-backend CEL filter meaning lives in
// queryrepotest.SemanticFilterMatrix — do not restate those outcomes
// here.
package sqlite

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

// staticQuerySchemas is a minimal [domain.QuerySchemaProvider] for
// field-resolver tests. Kept local so this package does not import
// transport/extensionresource (layering: infrastructure → domain only).
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
		Params: questionParams{},
	}, filter)
}

func compileErr(t *testing.T, filter string) error {
	t.Helper()
	return compileWithResolverErr(t, querysql.Compiler{
		Fields: queryFieldResolver{},
		Params: questionParams{},
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
			name:     "resourceType equality special-cases to constituent columns",
			filter:   `resourceType == "kind.fleetshift.io/Cluster"`,
			wantArgs: []any{"kind.fleetshift.io", "Cluster"},
			wantSQL:  []string{"er.service_name =", "er.type_name ="},
			denySQL:  []string{"er.service_name || '/' || er.type_name"},
		},
		{
			name:     "resourceType inequality keeps concatenated expression",
			filter:   `resourceType != "kind.fleetshift.io/Cluster"`,
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
				t.Errorf("SQL = %q, want questionParams (?N) not dollarParams", pred.SQL)
			}
			if !strings.Contains(pred.SQL, "?1") {
				t.Errorf("SQL = %q, want numbered ?N placeholders", pred.SQL)
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
	pred := compile(t, `resource.localLabels["node-role"] == "worker"`)
	if !strings.Contains(pred.SQL, `'$."'`) {
		t.Errorf("SQL = %q, want quoted JSON path segment for hyphenated keys", pred.SQL)
	}
	if pred.Args[0] != "node-role" {
		t.Errorf("Args[0] = %v, want \"node-role\"", pred.Args[0])
	}
}

func TestQueryFieldResolver_InventoryConditionsUseJSONExtract(t *testing.T) {
	pred := compile(t, `resource.conditions["Ready"].status == "True"`)
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

	pred = compile(t, `resource.conditions["Ready"].lastTransitionTime == "2026-06-01T12:00:00.500Z"`)
	if !strings.Contains(pred.SQL, "json_extract(inv.conditions") {
		t.Errorf("SQL = %q, want json_extract for lastTransitionTime", pred.SQL)
	}
	if strings.Contains(pred.SQL, "cel_ts_protojson(") {
		t.Errorf("SQL = %q, lastTransitionTime must not wrap with cel_ts_protojson", pred.SQL)
	}
	if len(pred.Args) != 2 {
		t.Fatalf("Args = %v, want 2 (condition type + value; subfield is inlined)", pred.Args)
	}
	if pred.Args[1] != "2026-06-01T12:00:00.500Z" {
		t.Errorf("Args[1] = %v, want ProtoJSON lastTransitionTime literal", pred.Args[1])
	}

	pred = compile(t, `resource.conditions["Ready"].lastTransitionTime == "2026-06-01T12:00:00.5Z"`)
	if !argsContain(pred.Args, "2026-06-01T12:00:00.5Z") {
		t.Errorf("Args = %v, want exact .5Z literal (not normalized)", pred.Args)
	}
}

func TestQueryFieldResolver_TimestampOnConditionLastTransitionTime(t *testing.T) {
	norm500 := formatTimestampNorm(time.Date(2026, 6, 1, 12, 0, 0, 500_000_000, time.UTC))
	normNoon := formatTimestampNorm(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))

	pred := compile(t, `timestamp(resource.conditions["Ready"].lastTransitionTime) == timestamp("2026-06-01T12:00:00.500Z")`)
	if !strings.Contains(pred.SQL, "json_extract(inv.conditions") {
		t.Errorf("SQL = %q, want json_extract", pred.SQL)
	}
	if !strings.Contains(pred.SQL, conditionLastTransitionTimeNormJSONKey) {
		t.Errorf("SQL = %q, want norm sibling path", pred.SQL)
	}
	if strings.Contains(pred.SQL, "cel_ts_norm") {
		t.Errorf("SQL = %q, timestamp() equality must not use cel_ts_norm", pred.SQL)
	}
	if !argsContain(pred.Args, norm500) {
		t.Errorf("Args = %v, want fixed-width norm %q", pred.Args, norm500)
	}

	pred = compile(t, `timestamp(resource.conditions["Ready"].lastTransitionTime) == timestamp("2026-06-01T08:00:00-04:00")`)
	if !argsContain(pred.Args, normNoon) {
		t.Errorf("Args = %v, want Z-normalized fixed-width %q", pred.Args, normNoon)
	}

	pred = compile(t, `timestamp(resource.conditions["Ready"].lastTransitionTime) < timestamp("2026-06-01T13:00:00Z")`)
	if strings.Contains(pred.SQL, "cel_ts_norm") {
		t.Errorf("SQL = %q, ordered timestamp() must not use cel_ts_norm", pred.SQL)
	}
	if !strings.Contains(pred.SQL, conditionLastTransitionTimeNormJSONKey) {
		t.Errorf("SQL = %q, want norm sibling extract", pred.SQL)
	}
}

func TestQueryFieldResolver_SpecGuardedByResourceType(t *testing.T) {
	pred := compile(t, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.provider == "aws"`)
	if !strings.Contains(pred.SQL, "ri.spec") || !strings.Contains(pred.SQL, "json_extract") {
		t.Errorf("SQL = %q, want a ri.spec json_extract", pred.SQL)
	}
	if !argsContain(pred.Args, "provider") {
		t.Errorf("Args = %v, want bound JSON key \"provider\"", pred.Args)
	}
}

func TestQueryFieldResolver_SpecWithoutGuardCompiles(t *testing.T) {
	pred := compile(t, `resource.spec.provider == "aws"`)
	if !strings.Contains(pred.SQL, "ri.spec") {
		t.Errorf("SQL = %q, want a ri.spec extraction without a resourceType guard", pred.SQL)
	}
}

func TestQueryFieldResolver_OrOfTypedSpecBranchesCompiles(t *testing.T) {
	pred := compile(t, `(resourceType == "kind.fleetshift.io/Cluster" && resource.spec.provider == "aws") || (resourceType == "kubernetes.fleetshift.io/Node" && resource.observation.capacity.cpu > 4)`)
	if !strings.Contains(pred.SQL, " OR ") {
		t.Errorf("SQL = %q, want an OR of the two typed branches", pred.SQL)
	}
	if !strings.Contains(pred.SQL, "ri.spec") {
		t.Errorf("SQL = %q, want ri.spec from the Cluster branch", pred.SQL)
	}
	if !strings.Contains(pred.SQL, "inv.observation") {
		t.Errorf("SQL = %q, want inv.observation from the Node branch", pred.SQL)
	}
}

func TestQueryFieldResolver_NumericJSONPreservesJSONTypes(t *testing.T) {
	for _, tt := range []struct {
		name   string
		filter string
	}{
		{
			name:   "observation numeric comparison",
			filter: `resourceType == "kubernetes.fleetshift.io/Node" && resource.observation.capacity.cpu > 4`,
		},
		{
			name:   "spec numeric comparison",
			filter: `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.count > 4`,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pred := compile(t, tt.filter)
			if !strings.Contains(pred.SQL, "json_type(") {
				t.Errorf("SQL = %q, want json_type guard (no string↔number coercion)", pred.SQL)
			}
			if strings.Contains(pred.SQL, "json_valid(") {
				t.Errorf("SQL = %q, must not coerce JSON string numerics via json_valid", pred.SQL)
			}
			if !strings.Contains(pred.SQL, "'integer'") || !strings.Contains(pred.SQL, "'real'") {
				t.Errorf("SQL = %q, want only JSON number types", pred.SQL)
			}
		})
	}

	t.Run("labels reject ordered cross-type comparison", func(t *testing.T) {
		err := compileErr(t, `resource.labels["priority"] > 4`)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("err = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("JSON string numeric does not match int literal", func(t *testing.T) {
		db := OpenTestDB(t)
		if _, err := db.Exec(`CREATE TABLE t (j TEXT); INSERT INTO t VALUES ('{"n":"8"}'), ('{"n":8}');`); err != nil {
			t.Fatal(err)
		}
		pred := compile(t, `resource.spec.n > 4`)
		sql := strings.ReplaceAll(pred.SQL, "ri.spec", "t.j")
		// Drop resourceType guard pieces if present — this filter has none.
		rows, err := db.Query(`SELECT 1 FROM t WHERE `+sql, pred.Args...)
		if err != nil {
			t.Fatalf("query: %v", err)
		}
		defer rows.Close()
		var n int
		for rows.Next() {
			n++
		}
		if n != 1 {
			t.Fatalf("matched %d rows, want 1 (only JSON number 8)", n)
		}
	})
}

func TestQueryFieldResolver_BooleanJSONPreservesJSONTypes(t *testing.T) {
	pred := compile(t, `resourceType == "kind.fleetshift.io/Cluster" && resource.observation.healthy == true`)
	if !strings.Contains(pred.SQL, "json_type(") {
		t.Errorf("SQL = %q, want json_type guard for boolean", pred.SQL)
	}
	if !strings.Contains(pred.SQL, `"true"`) && !strings.Contains(pred.SQL, "'true'") {
		t.Errorf("SQL = %q, want JSON boolean type check", pred.SQL)
	}
}

func TestQueryFieldResolver_DollarPrefixedJSONKeysAreLiteral(t *testing.T) {
	for _, key := range []string{"$foo", "$.x", "$ref"} {
		filter := fmt.Sprintf(`resource.spec[%q] == "v"`, key)
		pred := compile(t, filter)
		if !argsContain(pred.Args, key) {
			t.Errorf("filter %q: Args = %v, want literal key", filter, pred.Args)
		}
		if strings.Contains(pred.SQL, " -> ") || strings.Contains(pred.SQL, " ->> ") {
			t.Errorf("SQL = %q, dollar keys must use quoted json_extract, not ->/->>", pred.SQL)
		}
		if !strings.Contains(pred.SQL, "json_extract(") {
			t.Errorf("SQL = %q, want json_extract", pred.SQL)
		}
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
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: schemas}, Params: questionParams{}}

	// Spec itself is a google.protobuf.Timestamp (ProtoJSON string):
	// nested protobuf fields like seconds must be rejected.
	err := compileWithResolverErr(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.seconds == 5`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("CompileFilter traversing Timestamp.seconds: err = %v, want ErrInvalidArgument", err)
	}

	err = compileWithResolverErr(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.bogus_field == 5`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("CompileFilter with an unknown field: err = %v, want ErrInvalidArgument", err)
	}
}

func TestQueryFieldResolver_TimestampFieldIsTerminalInSchema(t *testing.T) {
	desc := specWithTimestampField(t)
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")
	schemas := staticQuerySchemas{
		rt: {ResourceType: rt, APIVersion: "v1", SpecDescriptor: desc},
	}
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: schemas}, Params: questionParams{}}

	pred := compileWithResolver(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.when == "2026-06-01T12:00:00Z"`)
	if !strings.Contains(pred.SQL, "ri.spec") {
		t.Errorf("SQL = %q, want resource.spec.when extraction", pred.SQL)
	}
	if !argsContain(pred.Args, "when") {
		t.Errorf("Args = %v, want bound JSON key \"when\"", pred.Args)
	}

	err := compileWithResolverErr(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.when.seconds == 5`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("traversing Timestamp.seconds: err = %v, want ErrInvalidArgument", err)
	}
}

func specTestDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	const src = `
syntax = "proto3";
package querysql.test;
message TestSpec {
  string api_server_port = 1;
  string display_name = 2 [json_name = "title"];
}
`
	desc, err := dynamicapi.CompileInline(context.Background(),
		map[string]string{"test_spec.proto": src}, "test_spec.proto", "querysql.test.TestSpec")
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}
	return desc.Message
}

func specWithTimestampField(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	const src = `
syntax = "proto3";
package querysql.test;
import "google/protobuf/timestamp.proto";
message SpecWithWhen {
  google.protobuf.Timestamp when = 1;
}
`
	desc, err := dynamicapi.CompileInline(context.Background(),
		map[string]string{"spec_when.proto": src}, "spec_when.proto", "querysql.test.SpecWithWhen")
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}
	return desc.Message
}

func TestQueryFieldResolver_SpecJSONNameOnlyNoProtoAlias(t *testing.T) {
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")
	schemas := staticQuerySchemas{
		rt: {
			ResourceType:   rt,
			APIVersion:     "v1",
			SpecDescriptor: specTestDescriptor(t),
		},
	}
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: schemas}, Params: questionParams{}}

	t.Run("JSON camelCase accepted", func(t *testing.T) {
		for _, filter := range []string{
			`resourceType == "kind.fleetshift.io/Cluster" && resource.spec.apiServerPort == "6443"`,
			`resourceType == "kind.fleetshift.io/Cluster" && resource.spec["apiServerPort"] == "6443"`,
		} {
			pred := compileWithResolver(t, c, filter)
			if !argsContain(pred.Args, "apiServerPort") {
				t.Errorf("filter %q: Args = %v, want bound key apiServerPort", filter, pred.Args)
			}
			if argsContain(pred.Args, "api_server_port") {
				t.Errorf("filter %q: Args = %v, must not bind proto name", filter, pred.Args)
			}
		}
	})

	t.Run("proto snake_case rejected", func(t *testing.T) {
		for _, filter := range []string{
			`resourceType == "kind.fleetshift.io/Cluster" && resource.spec.api_server_port == "6443"`,
			`resourceType == "kind.fleetshift.io/Cluster" && resource.spec["api_server_port"] == "6443"`,
		} {
			err := compileWithResolverErr(t, c, filter)
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
			}
		}
	})

	t.Run("custom json_name accepted; proto name rejected", func(t *testing.T) {
		pred := compileWithResolver(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.title == "x"`)
		if !argsContain(pred.Args, "title") {
			t.Errorf("Args = %v, want bound custom json_name \"title\"", pred.Args)
		}
		err := compileWithResolverErr(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.display_name == "x"`)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("display_name: err = %v, want ErrInvalidArgument", err)
		}
	})
}

func nestedSpecTestDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	const src = `
syntax = "proto3";
package querysql.test;
import "google/protobuf/struct.proto";
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
  map<int32, Item> by_id = 4;
  google.protobuf.Struct metadata = 5;
}
`
	desc, err := dynamicapi.CompileInline(context.Background(),
		map[string]string{"nested_spec.proto": src}, "nested_spec.proto", "querysql.test.NestedSpec")
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}
	return desc.Message
}

func TestQueryFieldResolver_SpecMapTraversalAndRepeatedRejection(t *testing.T) {
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")
	schemas := staticQuerySchemas{
		rt: {
			ResourceType:   rt,
			APIVersion:     "v1",
			SpecDescriptor: nestedSpecTestDescriptor(t),
		},
	}
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: schemas}, Params: questionParams{}}

	pred := compileWithResolver(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.nested.value == "x"`)
	if !argsContain(pred.Args, "nested") || !argsContain(pred.Args, "value") {
		t.Errorf("Args = %v, want nested and value keys", pred.Args)
	}

	compileWithResolver(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.items == "x"`)
	compileWithResolver(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.labels == "x"`)

	for _, filter := range []string{
		`resourceType == "kind.fleetshift.io/Cluster" && resource.spec.labels["team"].name == "x"`,
		`resourceType == "kind.fleetshift.io/Cluster" && resource.spec.labels.team.name == "x"`,
	} {
		pred := compileWithResolver(t, c, filter)
		if !argsContain(pred.Args, "labels") || !argsContain(pred.Args, "team") || !argsContain(pred.Args, "name") {
			t.Errorf("filter %q: Args = %v, want labels/team/name", filter, pred.Args)
		}
	}

	pred = compileWithResolver(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.metadata["node-role"] == "worker"`)
	if !argsContain(pred.Args, "metadata") || !argsContain(pred.Args, "node-role") {
		t.Errorf("Args = %v, want metadata and exact Struct key", pred.Args)
	}

	for _, filter := range []string{
		`resourceType == "kind.fleetshift.io/Cluster" && resource.spec.items.name == "x"`,
		`resourceType == "kind.fleetshift.io/Cluster" && resource.spec.byId["1"].name == "x"`,
		`resourceType == "kind.fleetshift.io/Cluster" && resource.spec.labels["team"].bogus == "x"`,
	} {
		err := compileWithResolverErr(t, c, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

func TestQueryFieldResolver_SpecPermissiveExactKeysWhenSchemaAbsent(t *testing.T) {
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: staticQuerySchemas{}}, Params: questionParams{}}
	pred := compileWithResolver(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.api_server_port == 5`)
	if !argsContain(pred.Args, "api_server_port") {
		t.Errorf("Args = %v, want exact open-map key preserved (no camelCase rewrite)", pred.Args)
	}
	if argsContain(pred.Args, "apiServerPort") {
		t.Errorf("Args = %v, must not normalize absent-schema keys", pred.Args)
	}

	for _, key := range []string{`node-role`, `a.b`, `has"quote`, `has\slash`} {
		filter := fmt.Sprintf(`resource.spec["%s"] == "x"`, strings.ReplaceAll(strings.ReplaceAll(key, `\`, `\\`), `"`, `\"`))
		pred := compileWithResolver(t, c, filter)
		if !argsContain(pred.Args, key) {
			t.Errorf("filter %q: Args = %v, want exact key %q", filter, pred.Args, key)
		}
	}
}

func TestQueryFieldResolver_LabelSelectIndexEquivalence(t *testing.T) {
	for _, filter := range []string{
		`resource.labels.node_role == "worker"`,
		`resource.labels["node_role"] == "worker"`,
	} {
		pred := compile(t, filter)
		if !argsContain(pred.Args, "node_role") {
			t.Errorf("filter %q: Args = %v, want exact key node_role", filter, pred.Args)
		}
	}
	pred := compile(t, `resource.labels.nodeRole == "worker"`)
	if !argsContain(pred.Args, "nodeRole") {
		t.Errorf("Args = %v, want distinct key nodeRole", pred.Args)
	}
}

func argsContain(args []any, want any) bool {
	for _, a := range args {
		if a == want {
			return true
		}
	}
	return false
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

func TestQueryFieldResolver_NestedInventoryPathsRejected(t *testing.T) {
	for _, filter := range []string{
		`resource.inventory.labels["node-role"] == "worker"`,
		`resource.inventory.conditions["Ready"].status == "True"`,
		`resource.inventory.observation.capacity.cpu > 4`,
	} {
		err := compileErr(t, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}
