// This file tests queryFieldResolver SQL generation only: column
// names, JSONB extraction, GIN containment rewrites, cel_ts_* usage,
// COLLATE "C", and SchemaProvider-backed path validation. Cross-backend
// CEL filter meaning lives in queryrepotest.SemanticFilterMatrix —
// do not restate those outcomes here. Package postgres (not
// postgres_test) so queryFieldResolver can be constructed without a
// database.
package postgres

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
	return compileWithResolver(t, querysql.Compiler{Fields: queryFieldResolver{}, Params: dollarParams{}}, filter)
}

func compileErr(t *testing.T, filter string) error {
	t.Helper()
	return compileWithResolverErr(t, querysql.Compiler{Fields: queryFieldResolver{}, Params: dollarParams{}}, filter)
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
		{
			name:     "resourceType in special-cases to constituent-column tuples",
			filter:   `resourceType in ["kind.fleetshift.io/Cluster", "kubernetes.fleetshift.io/Node"]`,
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
			filter:   `resourceType == "kind.fleetshift.io/Cluster" && name == "//kind.fleetshift.io/clusters/managed"`,
			wantArgs: []any{"kind.fleetshift.io", "Cluster", "kind.fleetshift.io", "clusters", "managed"},
		},
		{
			name:     "name startsWith uses concatenated expression",
			filter:   `name.startsWith("//kind.fleetshift.io/")`,
			wantArgs: []any{`//kind.fleetshift.io/%`},
			wantSQL:  []string{`COLLATE "C" LIKE`, "'//' || er.service_name || '/' || er.collection_name || '/' || er.resource_id", `ESCAPE '\'`},
		},
		{
			name:     "resourceType startsWith uses concatenated expression",
			filter:   `resourceType.startsWith("kind.fleetshift.io/")`,
			wantArgs: []any{`kind.fleetshift.io/%`},
			wantSQL:  []string{`COLLATE "C" LIKE`, "er.service_name || '/' || er.type_name", `ESCAPE '\'`},
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
			if strings.Contains(pred.SQL, "er.service_name || '/' || er.type_name") &&
				strings.Contains(tt.filter, "resourceType ==") {
				t.Errorf("SQL = %q, resourceType equality must not use the concatenated expression", pred.SQL)
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
	if !strings.Contains(pred.SQL, "er.labels ->") {
		t.Errorf("SQL = %q, want -> extraction for !=", pred.SQL)
	}
	if strings.Contains(pred.SQL, "@>") {
		t.Errorf("SQL = %q, != must not use containment rewrite", pred.SQL)
	}
}

func TestQueryFieldResolver_ResourceLabelsStartsWithUsesExtraction(t *testing.T) {
	pred := compile(t, `resource.labels["team"].startsWith("plat")`)
	if len(pred.Args) != 2 {
		t.Fatalf("Args = %v, want 2 (label key + LIKE pattern)", pred.Args)
	}
	if pred.Args[0] != "team" {
		t.Errorf("Args[0] = %v, want \"team\" (the label key)", pred.Args[0])
	}
	if pred.Args[1] != `plat%` {
		t.Errorf("Args[1] = %v, want \"plat%%\"", pred.Args[1])
	}
	if !strings.Contains(pred.SQL, "er.labels ->") {
		t.Errorf("SQL = %q, want -> extraction for startsWith", pred.SQL)
	}
	if !strings.Contains(pred.SQL, "LIKE") || !strings.Contains(pred.SQL, `ESCAPE '\'`) {
		t.Errorf("SQL = %q, want LIKE ... ESCAPE", pred.SQL)
	}
	if strings.Contains(pred.SQL, "@>") {
		t.Errorf("SQL = %q, startsWith must not use containment rewrite", pred.SQL)
	}
}

func TestQueryFieldResolver_LocalLabelsContainment(t *testing.T) {
	pred := compile(t, `resource.localLabels["node-role"] == "worker"`)
	if !strings.Contains(pred.SQL, "inv.labels @> jsonb_build_object(") {
		t.Errorf("SQL = %q, want GIN-friendly containment rewrite", pred.SQL)
	}
}

func TestQueryFieldResolver_LocalUpdateTimeAndIndexUpdateTime(t *testing.T) {
	// Direct string ops compare against ProtoJSON rendering.
	pred := compile(t, `resource.localUpdateTime == "2026-06-01T12:00:00Z"`)
	if !strings.Contains(pred.SQL, "cel_ts_protojson_tstz(inv.observed_at)") {
		t.Errorf("SQL = %q, want ProtoJSON string compare on observed_at", pred.SQL)
	}
	pred = compile(t, `timestamp(resource.localUpdateTime) == timestamp("2026-06-01T12:00:00Z")`)
	if !strings.Contains(pred.SQL, "inv.observed_at") || !strings.Contains(pred.SQL, "::timestamptz") {
		t.Errorf("SQL = %q, want native TIMESTAMPTZ compare on observed_at", pred.SQL)
	}
	if strings.Contains(pred.SQL, "cel_ts_norm") {
		t.Errorf("SQL = %q, native timestamp() must not wrap with cel_ts_norm", pred.SQL)
	}
	pred = compile(t, `timestamp(resource.indexUpdateTime) == timestamp("2026-06-01T12:00:00Z")`)
	if !strings.Contains(pred.SQL, "inv.updated_at") {
		t.Errorf("SQL = %q, want inv.updated_at", pred.SQL)
	}
	// Sub-microsecond literal equality is constant-false for a present column.
	pred = compile(t, `timestamp(resource.localUpdateTime) == timestamp("2026-06-01T12:00:00.123456789Z")`)
	if !strings.Contains(pred.SQL, "CASE WHEN inv.observed_at IS NULL THEN NULL ELSE FALSE END") {
		t.Errorf("SQL = %q, want present-false for sub-microsecond equality", pred.SQL)
	}
}

func TestQueryFieldResolver_TimestampOnLabelString(t *testing.T) {
	pred := compile(t, `timestamp(resource.labels["seenAt"]) == timestamp("2026-06-01T12:00:00Z")`)
	if !strings.Contains(pred.SQL, "cel_ts_norm") {
		t.Errorf("SQL = %q, want cel_ts_norm over label text", pred.SQL)
	}
	if strings.Contains(pred.SQL, "FALSE") || strings.Contains(pred.SQL, "false") {
		t.Errorf("SQL = %q, timestamp(label) must not fold to false", pred.SQL)
	}
}

func TestQueryFieldResolver_StringOrderingUsesCollateC(t *testing.T) {
	pred := compile(t, `resource.pauseReason < "zzz"`)
	if !strings.Contains(pred.SQL, `COLLATE "C"`) {
		t.Errorf("SQL = %q, want COLLATE \"C\" for string ordering", pred.SQL)
	}
}

func TestQueryFieldResolver_InventoryConditionsContainment(t *testing.T) {
	pred := compile(t, `resource.conditions["Ready"].status == "True"`)
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

	// lastTransitionTime is write-canonical ProtoJSON, so equality can
	// use the same GIN containment rewrite as other condition strings.
	pred = compile(t, `resource.conditions["Ready"].lastTransitionTime == "2026-06-01T12:00:00.500Z"`)
	if !strings.Contains(pred.SQL, "inv.conditions @> jsonb_build_object(") {
		t.Errorf("SQL = %q, want GIN-friendly containment rewrite", pred.SQL)
	}
	if strings.Contains(pred.SQL, "cel_ts_protojson(") {
		t.Errorf("SQL = %q, lastTransitionTime must not wrap with cel_ts_protojson", pred.SQL)
	}
	if len(pred.Args) != 3 {
		t.Fatalf("Args = %v, want 3 (condition type + subfield + value)", pred.Args)
	}
	if pred.Args[2] != "2026-06-01T12:00:00.500Z" {
		t.Errorf("Args[2] = %v, want ProtoJSON lastTransitionTime literal", pred.Args[2])
	}

	// Non-canonical ProtoJSON spelling must not be rewritten: CEL string
	// equality is exact against the response spelling (.500Z in storage).
	pred = compile(t, `resource.conditions["Ready"].lastTransitionTime == "2026-06-01T12:00:00.5Z"`)
	if !argsContain(pred.Args, "2026-06-01T12:00:00.5Z") {
		t.Errorf("Args = %v, want exact .5Z literal (not normalized)", pred.Args)
	}
	if argsContain(pred.Args, "2026-06-01T12:00:00.500Z") {
		t.Errorf("Args = %v, must not rewrite .5Z to .500Z for direct string ==", pred.Args)
	}
}

func TestQueryFieldResolver_TimestampOnConditionLastTransitionTime(t *testing.T) {
	norm500 := formatTimestampNorm(time.Date(2026, 6, 1, 12, 0, 0, 500_000_000, time.UTC))
	normNoon := formatTimestampNorm(time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC))

	// Equality: fixed-width norm sibling + GIN containment (no cel_ts_norm).
	pred := compile(t, `timestamp(resource.conditions["Ready"].lastTransitionTime) == timestamp("2026-06-01T12:00:00.500Z")`)
	if !strings.Contains(pred.SQL, "inv.conditions @> jsonb_build_object(") {
		t.Errorf("SQL = %q, want GIN-friendly containment rewrite", pred.SQL)
	}
	if !argsContain(pred.Args, conditionLastTransitionTimeNormJSONKey) {
		t.Errorf("Args = %v, want norm JSON key", pred.Args)
	}
	if !argsContain(pred.Args, norm500) {
		t.Errorf("Args = %v, want fixed-width norm literal %q", pred.Args, norm500)
	}
	if strings.Contains(pred.SQL, "cel_ts_norm") {
		t.Errorf("SQL = %q, timestamp() equality must not use cel_ts_norm", pred.SQL)
	}

	pred = compile(t, `timestamp(resource.conditions["Ready"].lastTransitionTime) == timestamp("2026-06-01T08:00:00-04:00")`)
	if !argsContain(pred.Args, normNoon) {
		t.Errorf("Args = %v, want Z-normalized fixed-width %q", pred.Args, normNoon)
	}

	pred = compile(t, `timestamp(resource.conditions["Ready"].lastTransitionTime) != timestamp("2026-06-01T12:00:00Z")`)
	if strings.Contains(pred.SQL, "cel_ts_norm") {
		t.Errorf("SQL = %q, != must not use cel_ts_norm", pred.SQL)
	}
	if strings.Contains(pred.SQL, "@>") {
		t.Errorf("SQL = %q, != must not use containment", pred.SQL)
	}
	if !strings.Contains(pred.SQL, conditionLastTransitionTimeNormJSONKey) {
		t.Errorf("SQL = %q, want extract of norm sibling", pred.SQL)
	}

	pred = compile(t, `timestamp(resource.conditions["Ready"].lastTransitionTime) in [timestamp("2026-06-01T12:00:00Z")]`)
	if strings.Contains(pred.SQL, "cel_ts_norm") {
		t.Errorf("SQL = %q, IN must not use cel_ts_norm", pred.SQL)
	}
	if !argsContain(pred.Args, normNoon) {
		t.Errorf("Args = %v, want fixed-width norm", pred.Args)
	}

	// Ordered timestamp() compares the stored norm text directly.
	pred = compile(t, `timestamp(resource.conditions["Ready"].lastTransitionTime) < timestamp("2026-06-01T13:00:00Z")`)
	if strings.Contains(pred.SQL, "cel_ts_norm") {
		t.Errorf("SQL = %q, ordered timestamp() must not use cel_ts_norm", pred.SQL)
	}
	if !strings.Contains(pred.SQL, conditionLastTransitionTimeNormJSONKey) {
		t.Errorf("SQL = %q, want norm sibling extract", pred.SQL)
	}
	if !strings.Contains(pred.SQL, `COLLATE "C"`) {
		t.Errorf("SQL = %q, want COLLATE \"C\" for ordered norm compare", pred.SQL)
	}
}

func TestQueryFieldResolver_SpecGuardedByResourceType(t *testing.T) {
	pred := compile(t, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.provider == "aws"`)
	if !strings.Contains(pred.SQL, "ri.spec") || !strings.Contains(pred.SQL, "->") {
		t.Errorf("SQL = %q, want a ri.spec -> extraction", pred.SQL)
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

func TestQueryFieldResolver_ObservationWithoutGuardCompiles(t *testing.T) {
	pred := compile(t, `resource.observation.capacity.cpu > 4`)
	if !strings.Contains(pred.SQL, "inv.observation") {
		t.Errorf("SQL = %q, want an inv.observation extraction without a resourceType guard", pred.SQL)
	}
}

func TestQueryFieldResolver_OrOfTypedSpecBranchesCompiles(t *testing.T) {
	// Each branch carries its own resourceType == in the SQL; a
	// root-level guard is not required to compile type-shaped paths.
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

func TestQueryFieldResolver_ObservationGuardedByResourceType(t *testing.T) {
	pred := compile(t, `resourceType == "kubernetes.fleetshift.io/Node" && resource.observation.capacity.cpu > 4`)
	if !strings.Contains(pred.SQL, "::numeric") {
		t.Errorf("SQL = %q, want a numeric cast for the int literal comparison", pred.SQL)
	}
	// resourceType equality binds service+type; path segments capacity/cpu
	// are bound; observation comparison binds 4.
	if !argsContain(pred.Args, "capacity") || !argsContain(pred.Args, "cpu") || !argsContain(pred.Args, int64(4)) {
		t.Errorf("Args = %v, want capacity, cpu, and 4", pred.Args)
	}
}

func TestQueryFieldResolver_NumericJSONPreservesJSONTypes(t *testing.T) {
	for _, tt := range []struct {
		name    string
		filter  string
		sqlType string
	}{
		{
			name:    "observation numeric comparison",
			filter:  `resourceType == "kubernetes.fleetshift.io/Node" && resource.observation.capacity.cpu > 4`,
			sqlType: "number",
		},
		{
			name:    "spec numeric comparison",
			filter:  `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.count > 4`,
			sqlType: "number",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pred := compile(t, tt.filter)
			if !strings.Contains(pred.SQL, "jsonb_typeof(") {
				t.Errorf("SQL = %q, want jsonb_typeof guard (no string↔number coercion)", pred.SQL)
			}
			if strings.Contains(pred.SQL, "pg_input_is_valid") {
				t.Errorf("SQL = %q, must not coerce via pg_input_is_valid", pred.SQL)
			}
			if !strings.Contains(pred.SQL, "'"+tt.sqlType+"'") {
				t.Errorf("SQL = %q, want jsonb_typeof %q", pred.SQL, tt.sqlType)
			}
		})
	}

	t.Run("observation boolean equality uses typed jsonb", func(t *testing.T) {
		pred := compile(t, `resourceType == "kubernetes.fleetshift.io/Node" && resource.observation.healthy == true`)
		if !strings.Contains(pred.SQL, "to_jsonb(") {
			t.Errorf("SQL = %q, want to_jsonb typed equality", pred.SQL)
		}
	})

	t.Run("labels reject ordered cross-type comparison", func(t *testing.T) {
		err := compileErr(t, `resource.labels["priority"] > 4`)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("err = %v, want ErrInvalidArgument", err)
		}
	})
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
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: schemas}, Params: dollarParams{}}

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
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: schemas}, Params: dollarParams{}}

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
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: schemas}, Params: dollarParams{}}

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
		err = compileWithResolverErr(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.displayName == "x"`)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("displayName: err = %v, want ErrInvalidArgument", err)
		}
	})
}

// nestedSpecTestDescriptor has a singular nested message, a repeated
// message field, and a string-to-message map. Repeated traversal fails
// closed; string-keyed map traversal resumes JSON-name validation on
// the map value message.
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
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: schemas}, Params: dollarParams{}}

	pred := compileWithResolver(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.nested.value == "x"`)
	if !argsContain(pred.Args, "nested") || !argsContain(pred.Args, "value") {
		t.Errorf("Args = %v, want nested and value keys", pred.Args)
	}

	// Terminal selection of a repeated/map field is still allowed.
	compileWithResolver(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.items == "x"`)
	compileWithResolver(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.labels == "x"`)

	// String-keyed map: exact key, then resume message JSON names.
	for _, filter := range []string{
		`resourceType == "kind.fleetshift.io/Cluster" && resource.spec.labels["team"].name == "x"`,
		`resourceType == "kind.fleetshift.io/Cluster" && resource.spec.labels.team.name == "x"`,
	} {
		pred := compileWithResolver(t, c, filter)
		if !argsContain(pred.Args, "labels") || !argsContain(pred.Args, "team") || !argsContain(pred.Args, "name") {
			t.Errorf("filter %q: Args = %v, want labels/team/name", filter, pred.Args)
		}
	}

	// Struct field: remaining segments are exact literal keys.
	pred = compileWithResolver(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.metadata["node-role"] == "worker"`)
	if !argsContain(pred.Args, "metadata") || !argsContain(pred.Args, "node-role") {
		t.Errorf("Args = %v, want metadata and exact Struct key", pred.Args)
	}

	for _, filter := range []string{
		`resourceType == "kind.fleetshift.io/Cluster" && resource.spec.items.name == "x"`,
		`resourceType == "kind.fleetshift.io/Cluster" && resource.spec.byId["1"].name == "x"`, // non-string map key
		`resourceType == "kind.fleetshift.io/Cluster" && resource.spec.labels["team"].bogus == "x"`,
	} {
		err := compileWithResolverErr(t, c, filter)
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
		}
	}
}

func TestQueryFieldResolver_SpecPermissiveExactKeysWhenSchemaAbsent(t *testing.T) {
	c := querysql.Compiler{Fields: queryFieldResolver{SchemaProvider: staticQuerySchemas{}}, Params: dollarParams{}}
	pred := compileWithResolver(t, c, `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.api_server_port == 5`)
	if !argsContain(pred.Args, "api_server_port") {
		t.Errorf("Args = %v, want exact open-map key preserved (no camelCase rewrite)", pred.Args)
	}
	if argsContain(pred.Args, "apiServerPort") {
		t.Errorf("Args = %v, must not normalize absent-schema keys", pred.Args)
	}

	// Arbitrary exact keys (hyphen, dot, quote) bind safely.
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
	if argsContain(pred.Args, "node_role") {
		t.Errorf("Args = %v, nodeRole must not alias node_role", pred.Args)
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

func TestQueryFieldResolver_SpecOnOneSideOfOrCompiles(t *testing.T) {
	pred := compile(t, `(resourceType == "kind.fleetshift.io/Cluster") || resource.spec.provider == "aws"`)
	if !strings.Contains(pred.SQL, " OR ") {
		t.Errorf("SQL = %q, want OR", pred.SQL)
	}
	if !strings.Contains(pred.SQL, "ri.spec") {
		t.Errorf("SQL = %q, want ri.spec on the unguarded side of OR", pred.SQL)
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
		{`resource.intentVersion == "1"`, "erm.current_version"},
		{`resource.state == "ACTIVE"`, "f.state"},
		{`resource.pauseReason == "manual"`, "f.pause_reason"},
		{`resource.generation == "3"`, "f.generation"},
	} {
		pred := compile(t, tt.filter)
		if !strings.Contains(pred.SQL, tt.column) {
			t.Errorf("filter %q: SQL = %q, want it to reference column %q", tt.filter, pred.SQL, tt.column)
		}
	}

	// Int literals against ProtoJSON int64 string fields are heterogeneous
	// equality (always false), not column comparisons.
	for _, filter := range []string{
		`resource.intentVersion == 1`,
		`resource.generation == 3`,
		`resource.pauseReason == 1`,
		`resource.state == 1`,
		`resource.uid == true`,
	} {
		pred := compile(t, filter)
		if pred.SQL != "FALSE" && !strings.Contains(pred.SQL, "FALSE") {
			// SQLite uses 0; postgres uses FALSE.
			if pred.SQL != "0" && !strings.Contains(pred.SQL, "0") {
				t.Errorf("filter %q: SQL = %q, want heterogeneous-equality false", filter, pred.SQL)
			}
		}
	}

	pred := compile(t, `resource.pauseReason != 1`)
	if !strings.Contains(pred.SQL, "IS NOT NULL") {
		t.Errorf("SQL = %q, want present-only check for incompatible !=", pred.SQL)
	}
	err := compileErr(t, `resource.pauseReason > 1`)
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("ordered cross-type: err = %v, want ErrInvalidArgument", err)
	}
	pred = compile(t, `resource.pauseReason in [1]`)
	if pred.SQL != "FALSE" && !strings.Contains(pred.SQL, "FALSE") {
		t.Errorf("SQL = %q, want false for incompatible membership", pred.SQL)
	}

	// Snake_case message rejection and heterogeneous match outcomes are
	// covered by queryrepotest.SemanticFilterMatrix. Below: SQL-shape
	// only (literal binding / LIKE over the API enum projection).

	// Storage spelling is not an alias for the API enum name.
	pred = compile(t, `resource.state == "active"`)
	found := false
	for _, a := range pred.Args {
		if a == "active" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("args = %#v, want literal \"active\" (no case folding)", pred.Args)
	}

	pred = compile(t, `resource.state == "ACTIVE"`)
	found = false
	for _, a := range pred.Args {
		if a == "ACTIVE" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("args = %#v, want API enum spelling \"ACTIVE\"", pred.Args)
	}

	pred = compile(t, `resource.state.startsWith("CRE")`)
	if !strings.Contains(pred.SQL, "LIKE") || !strings.Contains(pred.SQL, `ESCAPE '\'`) {
		t.Errorf("SQL = %q, want LIKE ... ESCAPE over API enum projection", pred.SQL)
	}
	found = false
	for _, a := range pred.Args {
		if a == "CRE%" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("args = %#v, want prefix \"CRE%%\" (no case folding)", pred.Args)
	}
}

func TestQueryFieldResolver_UnsupportedField(t *testing.T) {
	for _, filter := range []string{
		`not_a_real_field == "x"`,
		`resource.aliases == "x"`,
		`resource.effective_labels["env"] == "x"`,
		`resource.representations == "x"`,
		`resource.relationships == "x"`,
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
	pred := compile(t, `resource.conditions["`+strings.ReplaceAll(payload, `"`, `\"`)+`"].status == "True"`)
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
