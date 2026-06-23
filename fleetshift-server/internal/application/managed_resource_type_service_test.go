package application_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func newTypeService(t *testing.T) *application.ManagedResourceTypeService {
	t.Helper()
	return application.NewManagedResourceTypeService(newStore(t))
}

func TestManagedResourceTypeService_CRUD(t *testing.T) {
	ctx := context.Background()
	svc := newTypeService(t)

	rel := domain.NewRegisteredSelfTarget("addon-cluster-mgmt", "api.kind.cluster")

	// Create
	def, err := svc.Create(ctx, application.CreateTypeInput{
		ResourceType:   "test.fleetshift.io/Cluster",
		Relation:       rel,
		APIServiceName: "test.fleetshift.io",
		APIVersion:     "v1",
		CollectionID:   "clusters",
		Signature: domain.Signature{
			Signer:         domain.FederatedIdentity{Subject: "addon-svc", Issuer: "https://issuer.test"},
			ContentHash:    []byte("hash"),
			SignatureBytes: []byte("sig"),
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if def.ResourceType != "test.fleetshift.io/Cluster" {
		t.Errorf("ResourceType = %q, want %q", def.ResourceType, "test.fleetshift.io/Cluster")
	}
	if def.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}

	// Get
	got, err := svc.Get(ctx, "test.fleetshift.io/Cluster")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ResourceType != "test.fleetshift.io/Cluster" {
		t.Errorf("Get: ResourceType = %q", got.ResourceType)
	}
	rst, ok := got.Relation.(domain.RegisteredSelfTarget)
	if !ok {
		t.Fatalf("Relation type = %T, want RegisteredSelfTarget", got.Relation)
	}
	if rst.AddonTarget() != "addon-cluster-mgmt" {
		t.Errorf("AddonTarget = %q", rst.AddonTarget())
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
	if err := svc.Delete(ctx, "test.fleetshift.io/Cluster"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, err = svc.Get(ctx, "test.fleetshift.io/Cluster")
	if !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("Get after delete: got %v, want ErrNotFound", err)
	}
}

func TestManagedResourceTypeService_CreateNilRelation(t *testing.T) {
	ctx := context.Background()
	svc := newTypeService(t)

	_, err := svc.Create(ctx, application.CreateTypeInput{
		ResourceType:   "test.fleetshift.io/X",
		APIServiceName: "test.fleetshift.io",
		APIVersion:     "v1",
		CollectionID:   "xs",
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("Create nil relation: got %v, want ErrInvalidArgument", err)
	}
}

func TestManagedResourceTypeService_CreateDuplicate(t *testing.T) {
	ctx := context.Background()
	svc := newTypeService(t)

	in := application.CreateTypeInput{
		ResourceType:   "test.fleetshift.io/Cluster",
		Relation:       domain.NewRegisteredSelfTarget("addon", "api.kind.cluster"),
		APIServiceName: "test.fleetshift.io",
		APIVersion:     "v1",
		CollectionID:   "clusters",
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
