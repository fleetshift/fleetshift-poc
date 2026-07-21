package querysql

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

const resolveContextTestRT = domain.ResourceType("kind.fleetshift.io/Cluster")

type resolveContextSchemas map[domain.ResourceType]domain.ResourceQuerySchema

func (s resolveContextSchemas) GetResourceQuerySchema(_ context.Context, rt domain.ResourceType) (domain.ResourceQuerySchema, bool, error) {
	schema, ok := s[rt]
	return schema, ok, nil
}

func (s resolveContextSchemas) ListResourceQuerySchemas(_ context.Context) ([]domain.ResourceQuerySchema, error) {
	out := make([]domain.ResourceQuerySchema, 0, len(s))
	for _, schema := range s {
		out = append(out, schema)
	}
	return out, nil
}

type countingResolveContextSchemas struct {
	inner resolveContextSchemas
	gets  int
}

func (c *countingResolveContextSchemas) GetResourceQuerySchema(ctx context.Context, rt domain.ResourceType) (domain.ResourceQuerySchema, bool, error) {
	c.gets++
	return c.inner.GetResourceQuerySchema(ctx, rt)
}

func (c *countingResolveContextSchemas) ListResourceQuerySchemas(ctx context.Context) ([]domain.ResourceQuerySchema, error) {
	return c.inner.ListResourceQuerySchemas(ctx)
}

type errResolveContextSchemas struct {
	err error
}

func (e errResolveContextSchemas) GetResourceQuerySchema(context.Context, domain.ResourceType) (domain.ResourceQuerySchema, bool, error) {
	return domain.ResourceQuerySchema{}, false, e.err
}

func (e errResolveContextSchemas) ListResourceQuerySchemas(context.Context) ([]domain.ResourceQuerySchema, error) {
	return nil, e.err
}

func resolveContextGuardRT() *domain.ResourceType {
	rt := resolveContextTestRT
	return &rt
}

func resolveContextNestedSpecDescriptor(t *testing.T) protoreflect.MessageDescriptor {
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

func resolveContextObservationDescriptor(t *testing.T) protoreflect.MessageDescriptor {
	t.Helper()
	const src = `
syntax = "proto3";
package querysql.test;
message Obs {
  string region = 1;
}
`
	desc, err := dynamicapi.CompileInline(context.Background(),
		map[string]string{"obs.proto": src}, "obs.proto", "querysql.test.Obs")
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}
	return desc.Message
}

func TestResolveContext_ValidateSpecPath(t *testing.T) {
	desc := resolveContextNestedSpecDescriptor(t)
	schemas := resolveContextSchemas{
		resolveContextTestRT: {ResourceType: resolveContextTestRT, SpecDescriptor: desc},
	}

	t.Run("no guard accepts structurally", func(t *testing.T) {
		ctx := newResolveContext(context.Background(), nil, schemas)
		got, err := ctx.ValidateSpecPath([]string{"anything", "goes"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fmt.Sprint(got) != "[anything goes]" {
			t.Errorf("got %v, want exact segments preserved", got)
		}
	})

	t.Run("no provider accepts structurally", func(t *testing.T) {
		ctx := newResolveContext(context.Background(), resolveContextGuardRT(), nil)
		got, err := ctx.ValidateSpecPath([]string{"api_server_port"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fmt.Sprint(got) != "[api_server_port]" {
			t.Errorf("got %v", got)
		}
	})

	t.Run("missing schema accepts structurally", func(t *testing.T) {
		ctx := newResolveContext(context.Background(), resolveContextGuardRT(), resolveContextSchemas{})
		got, err := ctx.ValidateSpecPath([]string{"api_server_port"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fmt.Sprint(got) != "[api_server_port]" {
			t.Errorf("got %v", got)
		}
	})

	t.Run("missing descriptor accepts structurally", func(t *testing.T) {
		ctx := newResolveContext(context.Background(), resolveContextGuardRT(), resolveContextSchemas{
			resolveContextTestRT: {ResourceType: resolveContextTestRT},
		})
		got, err := ctx.ValidateSpecPath([]string{"api_server_port"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fmt.Sprint(got) != "[api_server_port]" {
			t.Errorf("got %v", got)
		}
	})

	t.Run("valid descriptor path", func(t *testing.T) {
		ctx := newResolveContext(context.Background(), resolveContextGuardRT(), schemas)
		got, err := ctx.ValidateSpecPath([]string{"nested", "value"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if fmt.Sprint(got) != "[nested value]" {
			t.Errorf("got %v", got)
		}
	})

	t.Run("invalid canonical name", func(t *testing.T) {
		ctx := newResolveContext(context.Background(), resolveContextGuardRT(), schemas)
		_, err := ctx.ValidateSpecPath([]string{"Nested"}) // proto name, not JSON name
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("err = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("invalid traversal into list", func(t *testing.T) {
		ctx := newResolveContext(context.Background(), resolveContextGuardRT(), schemas)
		_, err := ctx.ValidateSpecPath([]string{"items", "name"})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("err = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("provider error propagates", func(t *testing.T) {
		sentinel := errors.New("schema boom")
		ctx := newResolveContext(context.Background(), resolveContextGuardRT(), errResolveContextSchemas{err: sentinel})
		_, err := ctx.ValidateSpecPath([]string{"nested"})
		if !errors.Is(err, sentinel) {
			t.Fatalf("err = %v, want sentinel", err)
		}
		if !strings.Contains(err.Error(), string(resolveContextTestRT)) {
			t.Errorf("err = %v, want resource type in message", err)
		}
	})
}

func TestResolveContext_ValidateObservationPath_SeparateDescriptor(t *testing.T) {
	specDesc := resolveContextNestedSpecDescriptor(t)
	obsDesc := resolveContextObservationDescriptor(t)
	schemas := resolveContextSchemas{
		resolveContextTestRT: {
			ResourceType:                   resolveContextTestRT,
			SpecDescriptor:                 specDesc,
			InventoryObservationDescriptor: obsDesc,
		},
	}
	ctx := newResolveContext(context.Background(), resolveContextGuardRT(), schemas)

	if _, err := ctx.ValidateObservationPath([]string{"region"}); err != nil {
		t.Fatalf("observation region: %v", err)
	}
	if _, err := ctx.ValidateSpecPath([]string{"nested"}); err != nil {
		t.Fatalf("spec nested: %v", err)
	}

	// Spec field name must not validate under observation descriptor.
	if _, err := ctx.ValidateObservationPath([]string{"nested"}); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("observation nested: err = %v, want ErrInvalidArgument", err)
	}
	// Observation field name must not validate under spec descriptor.
	if _, err := ctx.ValidateSpecPath([]string{"region"}); !errors.Is(err, domain.ErrInvalidArgument) {
		t.Errorf("spec region: err = %v, want ErrInvalidArgument", err)
	}
}

func TestResolveContext_ClassifyResourceContainer(t *testing.T) {
	desc := resolveContextNestedSpecDescriptor(t)
	schemas := resolveContextSchemas{
		resolveContextTestRT: {
			ResourceType:                   resolveContextTestRT,
			SpecDescriptor:                 desc,
			InventoryObservationDescriptor: desc,
		},
	}
	ctx := newResolveContext(context.Background(), resolveContextGuardRT(), schemas)

	tests := []struct {
		name    string
		segs    []string
		want    ContainerKind
		wantErr bool
	}{
		{name: "labels object", segs: []string{"labels"}, want: ContainerKindObject},
		{name: "label value scalar", segs: []string{"labels", "team"}, want: ContainerKindScalar},
		{name: "conditions object", segs: []string{"conditions"}, want: ContainerKindObject},
		{name: "condition entry object", segs: []string{"conditions", "Ready"}, want: ContainerKindObject},
		{name: "condition subfield scalar", segs: []string{"conditions", "Ready", "status"}, want: ContainerKindScalar},
		{name: "spec root object", segs: []string{"spec"}, want: ContainerKindObject},
		{name: "known message", segs: []string{"spec", "nested"}, want: ContainerKindObject},
		{name: "known map", segs: []string{"spec", "labels"}, want: ContainerKindObject},
		{name: "known list", segs: []string{"spec", "tags"}, want: ContainerKindList},
		{name: "scalar string", segs: []string{"spec", "nested", "value"}, want: ContainerKindScalar},
		{name: "timestamp terminal scalar", segs: []string{"spec", "when"}, want: ContainerKindScalar},
		{name: "open Struct unknown", segs: []string{"spec", "metadata", "foo"}, want: ContainerKindUnknown},
		{name: "name scalar", segs: []string{"name"}, want: ContainerKindScalar},
		{name: "unsupported", segs: []string{"bogus"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ctx.ClassifyResourceContainer(tt.segs)
			if tt.wantErr {
				if !errors.Is(err, domain.ErrInvalidArgument) {
					t.Fatalf("err = %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Kind != tt.want {
				t.Errorf("Kind = %v, want %v", got.Kind, tt.want)
			}
		})
	}

	t.Run("no guard keeps open JSON unknown", func(t *testing.T) {
		free := newResolveContext(context.Background(), nil, schemas)
		got, err := free.ClassifyResourceContainer([]string{"spec", "nested"})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got.Kind != ContainerKindUnknown {
			t.Errorf("Kind = %v, want Unknown", got.Kind)
		}
	})

	t.Run("Timestamp root descriptor is scalar for nested path rejection via validate", func(t *testing.T) {
		tsSchemas := resolveContextSchemas{
			resolveContextTestRT: {
				ResourceType:   resolveContextTestRT,
				SpecDescriptor: (&timestamppb.Timestamp{}).ProtoReflect().Descriptor(),
			},
		}
		tsCtx := newResolveContext(context.Background(), resolveContextGuardRT(), tsSchemas)
		_, err := tsCtx.ValidateSpecPath([]string{"seconds"})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
	})
}

func TestResolveContext_SchemaLookupReusedPerCompilation(t *testing.T) {
	desc := resolveContextNestedSpecDescriptor(t)
	inner := resolveContextSchemas{
		resolveContextTestRT: {ResourceType: resolveContextTestRT, SpecDescriptor: desc},
	}
	counting := &countingResolveContextSchemas{inner: inner}
	ctx := newResolveContext(context.Background(), resolveContextGuardRT(), counting)

	if _, err := ctx.ValidateSpecPath([]string{"nested"}); err != nil {
		t.Fatalf("first validate: %v", err)
	}
	if _, err := ctx.ValidateSpecPath([]string{"tags"}); err != nil {
		t.Fatalf("second validate: %v", err)
	}
	class, err := ctx.ClassifyResourceContainer([]string{"spec", "labels"})
	if err != nil {
		t.Fatalf("classify: %v", err)
	}
	if class.Kind != ContainerKindObject {
		t.Errorf("Kind = %v, want Object", class.Kind)
	}
	if counting.gets != 1 {
		t.Errorf("schema gets = %d, want 1", counting.gets)
	}

	// A fresh ResolveContext (new compilation) must look up again.
	counting.gets = 0
	ctx2 := newResolveContext(context.Background(), resolveContextGuardRT(), counting)
	if _, err := ctx2.ValidateSpecPath([]string{"nested"}); err != nil {
		t.Fatalf("fresh validate: %v", err)
	}
	if counting.gets != 1 {
		t.Errorf("fresh compilation gets = %d, want 1", counting.gets)
	}
}
