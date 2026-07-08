package application_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestQuerySchemaCatalog_RegisterAndGet(t *testing.T) {
	c := application.NewQuerySchemaCatalog()
	ctx := context.Background()

	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")

	if _, ok, err := c.GetResourceQuerySchema(ctx, rt); err != nil || ok {
		t.Fatalf("GetResourceQuerySchema before Register: ok=%v err=%v, want ok=false err=nil", ok, err)
	}

	desc := (&timestamppb.Timestamp{}).ProtoReflect().Descriptor()
	c.Register(domain.ResourceQuerySchema{
		ResourceType:   rt,
		ServiceName:    "kind.fleetshift.io",
		TypeName:       "Cluster",
		SpecDescriptor: desc,
	})

	got, ok, err := c.GetResourceQuerySchema(ctx, rt)
	if err != nil || !ok {
		t.Fatalf("GetResourceQuerySchema after Register: ok=%v err=%v, want ok=true err=nil", ok, err)
	}
	if got.SpecDescriptor != desc {
		t.Errorf("SpecDescriptor = %v, want %v", got.SpecDescriptor, desc)
	}

	schemas, err := c.ListResourceQuerySchemas(ctx)
	if err != nil {
		t.Fatalf("ListResourceQuerySchemas: %v", err)
	}
	if len(schemas) != 1 || schemas[0].ResourceType != rt {
		t.Errorf("ListResourceQuerySchemas = %+v, want a single entry for %q", schemas, rt)
	}
}

func TestQuerySchemaCatalog_Unregister(t *testing.T) {
	c := application.NewQuerySchemaCatalog()
	ctx := context.Background()
	const rt = domain.ResourceType("kind.fleetshift.io/Cluster")

	c.Register(domain.ResourceQuerySchema{ResourceType: rt})
	c.Unregister(rt)

	if _, ok, err := c.GetResourceQuerySchema(ctx, rt); err != nil || ok {
		t.Fatalf("GetResourceQuerySchema after Unregister: ok=%v err=%v, want ok=false err=nil", ok, err)
	}
}
