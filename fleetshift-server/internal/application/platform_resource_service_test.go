package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestPlatformResourceService_CreatePrecreatesIdentity(t *testing.T) {
	store := newStore(t)
	svc := application.NewPlatformResourceService(store)
	ctx := context.Background()

	pr, err := svc.Create(ctx, application.CreatePlatformResourceInput{
		Name:   "clusters/prod-us-east-1",
		Labels: map[string]string{"env": "prod", "region": "us-east-1"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	if pr.Collection() != "clusters" {
		t.Errorf("Collection = %q, want %q", pr.Collection(), "clusters")
	}
	if pr.Name() != "clusters/prod-us-east-1" {
		t.Errorf("Name = %q, want %q", pr.Name(), "clusters/prod-us-east-1")
	}
	if pr.Labels()["env"] != "prod" {
		t.Errorf("Labels[env] = %q, want %q", pr.Labels()["env"], "prod")
	}
	if pr.Labels()["region"] != "us-east-1" {
		t.Errorf("Labels[region] = %q, want %q", pr.Labels()["region"], "us-east-1")
	}
	if len(pr.Representations()) != 0 {
		t.Errorf("Representations len = %d, want 0", len(pr.Representations()))
	}
}

func TestPlatformResourceService_CreateRejectsExistingResource(t *testing.T) {
	store := newStore(t)
	svc := application.NewPlatformResourceService(store)
	ctx := context.Background()

	_, err := svc.Create(ctx, application.CreatePlatformResourceInput{
		Name:   "clusters/prod-us-east-1",
		Labels: map[string]string{"env": "prod"},
	})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}

	_, err = svc.Create(ctx, application.CreatePlatformResourceInput{
		Name:   "clusters/prod-us-east-1",
		Labels: map[string]string{"env": "staging"},
	})
	if !errors.Is(err, domain.ErrAlreadyExists) {
		t.Fatalf("second Create err = %v, want %v", err, domain.ErrAlreadyExists)
	}
}

func TestPlatformResourceService_GetReturnsRepresentations(t *testing.T) {
	store := newStore(t)
	svc := application.NewPlatformResourceService(store)
	ctx := context.Background()

	pr, err := svc.Create(ctx, application.CreatePlatformResourceInput{
		Name:   "clusters/prod",
		Labels: map[string]string{"env": "prod"},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	fetched, err := tx.ResourceIdentities().GetByName(ctx, pr.Name())
	if err != nil {
		tx.Rollback()
		t.Fatalf("GetByName: %v", err)
	}
	if err := fetched.AttachRepresentation(domain.AttachRepresentationInput{
		ServiceName: "kind.fleetshift.io",
		Version:     "v1alpha1",
		Roles:       []domain.RepresentationRole{domain.RepresentationRoleManaged},
		Labels:      map[string]string{"provider": "kind"},
	}, time.Now().UTC()); err != nil {
		tx.Rollback()
		t.Fatalf("AttachRepresentation: %v", err)
	}
	if err := tx.ResourceIdentities().Update(ctx, fetched); err != nil {
		tx.Rollback()
		t.Fatalf("Update: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	got, err := svc.Get(ctx, "clusters/prod")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	reps := got.Representations()
	if len(reps) != 1 {
		t.Fatalf("Representations len = %d, want 1", len(reps))
	}
	if reps[0].ServiceName() != "kind.fleetshift.io" {
		t.Errorf("ServiceName = %q, want %q", reps[0].ServiceName(), "kind.fleetshift.io")
	}
	if reps[0].Version() != "v1alpha1" {
		t.Errorf("Version = %q, want %q", reps[0].Version(), "v1alpha1")
	}
	if reps[0].Roles()[0] != domain.RepresentationRoleManaged {
		t.Errorf("Role = %q, want %q", reps[0].Roles()[0], domain.RepresentationRoleManaged)
	}
}

func TestPlatformResourceService_ListByCollection(t *testing.T) {
	store := newStore(t)
	svc := application.NewPlatformResourceService(store)
	ctx := context.Background()

	// Create two resources in the same collection.
	_, err := svc.Create(ctx, application.CreatePlatformResourceInput{
		Name: "clusters/alpha",
	})
	if err != nil {
		t.Fatalf("Create alpha: %v", err)
	}
	_, err = svc.Create(ctx, application.CreatePlatformResourceInput{
		Name: "clusters/beta",
	})
	if err != nil {
		t.Fatalf("Create beta: %v", err)
	}

	// Create one in a different collection to verify isolation.
	_, err = svc.Create(ctx, application.CreatePlatformResourceInput{
		Name: "namespaces/default",
	})
	if err != nil {
		t.Fatalf("Create namespaces/default: %v", err)
	}

	resources, err := svc.List(ctx, "clusters")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(resources) != 2 {
		t.Fatalf("List len = %d, want 2", len(resources))
	}

	// Verify stable ordering (alphabetical by relative name).
	names := make([]domain.ResourceName, len(resources))
	for i, r := range resources {
		names[i] = r.Name()
	}
	if names[0] != "clusters/alpha" {
		t.Errorf("resources[0].Name = %q, want %q", names[0], "clusters/alpha")
	}
	if names[1] != "clusters/beta" {
		t.Errorf("resources[1].Name = %q, want %q", names[1], "clusters/beta")
	}
}
