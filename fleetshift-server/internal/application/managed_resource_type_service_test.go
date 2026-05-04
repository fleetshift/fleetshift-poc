package application_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/jsonschema"
)

func newTypeService(t *testing.T) *application.ManagedResourceTypeService {
	t.Helper()
	return &application.ManagedResourceTypeService{
		Store:          newStore(t),
		SchemaCompiler: jsonschema.Compiler{},
	}
}

func TestManagedResourceTypeService_CRUD(t *testing.T) {
	ctx := context.Background()
	svc := newTypeService(t)

	schema := domain.RawSchema(`{"type":"object","properties":{"provider":{"type":"string"}},"required":["provider"]}`)

	// Create
	def, err := svc.Create(ctx, application.CreateTypeInput{
		ResourceType: "clusters",
		Relation:     domain.RegisteredSelfTarget{AddonTarget: "addon-cluster-mgmt"},
		Signature: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.test"},
			ContentHash:    []byte("hash"),
			SignatureBytes: []byte("sig"),
		},
		SpecSchema: &schema,
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if def.ResourceType != "clusters" {
		t.Errorf("ResourceType = %q, want %q", def.ResourceType, "clusters")
	}
	if def.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}

	// Get
	got, err := svc.Get(ctx, "clusters")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ResourceType != "clusters" {
		t.Errorf("Get: ResourceType = %q", got.ResourceType)
	}
	rst, ok := got.Relation.(domain.RegisteredSelfTarget)
	if !ok {
		t.Fatalf("Relation type = %T, want RegisteredSelfTarget", got.Relation)
	}
	if rst.AddonTarget != "addon-cluster-mgmt" {
		t.Errorf("AddonTarget = %q", rst.AddonTarget)
	}

	// List
	defs, err := svc.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(defs) != 1 {
		t.Fatalf("List len = %d, want 1", len(defs))
	}

	// Delete
	if err := svc.Delete(ctx, "clusters"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err = svc.Get(ctx, "clusters")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestManagedResourceTypeService_CreateInvalidSchema(t *testing.T) {
	ctx := context.Background()
	svc := newTypeService(t)

	bad := domain.RawSchema(`{"type": "not-a-real-type"}`)
	_, err := svc.Create(ctx, application.CreateTypeInput{
		ResourceType: "bad-schema",
		Relation:     domain.RegisteredSelfTarget{AddonTarget: "addon"},
		Signature: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "s", Issuer: "i"},
			ContentHash:    []byte("h"),
			SignatureBytes: []byte("s"),
		},
		SpecSchema: &bad,
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("Create with invalid schema: got %v, want ErrInvalidArgument", err)
	}
}

func TestManagedResourceTypeService_CreateMissingFields(t *testing.T) {
	ctx := context.Background()
	svc := newTypeService(t)

	_, err := svc.Create(ctx, application.CreateTypeInput{})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("Create empty: got %v, want ErrInvalidArgument", err)
	}

	_, err = svc.Create(ctx, application.CreateTypeInput{
		ResourceType: "x",
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("Create no relation: got %v, want ErrInvalidArgument", err)
	}
}

func TestManagedResourceTypeService_CreateDuplicate(t *testing.T) {
	ctx := context.Background()
	svc := newTypeService(t)

	in := application.CreateTypeInput{
		ResourceType: "clusters",
		Relation:     domain.RegisteredSelfTarget{AddonTarget: "addon"},
		Signature: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "s", Issuer: "i"},
			ContentHash:    []byte("h"),
			SignatureBytes: []byte("s"),
		},
	}
	if _, err := svc.Create(ctx, in); err != nil {
		t.Fatalf("first Create: %v", err)
	}
	_, err := svc.Create(ctx, in)
	if !errors.Is(err, domain.ErrAlreadyExists) {
		t.Fatalf("second Create: got %v, want ErrAlreadyExists", err)
	}
}
