package querysql_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/querysql"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

const testRT = domain.ResourceType("kind.fleetshift.io/Cluster")

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

type countingQuerySchemas struct {
	inner staticQuerySchemas
	gets  int
}

func (c *countingQuerySchemas) GetResourceQuerySchema(ctx context.Context, rt domain.ResourceType) (domain.ResourceQuerySchema, bool, error) {
	c.gets++
	return c.inner.GetResourceQuerySchema(ctx, rt)
}

func (c *countingQuerySchemas) ListResourceQuerySchemas(ctx context.Context) ([]domain.ResourceQuerySchema, error) {
	return c.inner.ListResourceQuerySchemas(ctx)
}

func nestedSpecDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	const src = `
syntax = "proto3";
package querysql.test;
import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";
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
  google.protobuf.Struct metadata = 5;
  repeated string tags = 6;
  google.protobuf.Timestamp when = 7;
}
`
	desc, err := dynamicapi.CompileInline(context.Background(),
		map[string]string{"nested_spec.proto": src}, "nested_spec.proto", "querysql.test.NestedSpec")
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}
	return desc.Message
}

func TestCompiler_SchemaLookupReusedAcrossFieldResolutions(t *testing.T) {
	desc := nestedSpecDescriptor(t)
	inner := staticQuerySchemas{
		testRT: {ResourceType: testRT, SpecDescriptor: desc},
	}
	counting := &countingQuerySchemas{inner: inner}
	var specValidates int
	c := querysql.Compiler{
		Schemas: counting,
		Params:  dollarTestParams{},
		Fields: recordingResolver(func(path querysql.FieldPath, _ querysql.TypeHint, ctx querysql.ResolveContext) (querysql.SQLExpr, error) {
			if len(path.Segments) >= 2 && path.Segments[0] == "resource" && path.Segments[1] == "spec" {
				specValidates++
				if _, err := ctx.ValidateSpecPath(path.Segments[2:]); err != nil {
					return querysql.SQLExpr{}, err
				}
			}
			return querysql.SQLExpr{SQL: "col"}, nil
		}),
	}
	filter := `resourceType == "kind.fleetshift.io/Cluster" && resource.spec.nested.value == "a" && resource.spec.tags == "b"`
	if _, err := c.CompileFilter(context.Background(), querysql.CompileFilterInput{Filter: filter}); err != nil {
		t.Fatalf("CompileFilter: %v", err)
	}
	if specValidates != 2 {
		t.Fatalf("spec resolver calls = %d, want 2", specValidates)
	}
	if counting.gets != 1 {
		t.Errorf("schema gets = %d, want 1 across field resolutions", counting.gets)
	}
}
