package application_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/queryrepotest"
)

func TestResourceQueryService_RejectsNonWildcardScope(t *testing.T) {
	store := newStore(t)
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	_, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope: "clusters",
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("QueryResources(scope=clusters): err = %v, want ErrInvalidArgument", err)
	}
}

func TestResourceQueryService_RejectsEmptyScope(t *testing.T) {
	store := newStore(t)
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	_, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope: "",
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("QueryResources(scope=\"\"): err = %v, want ErrInvalidArgument", err)
	}
}

func TestResourceQueryService_WildcardReturnsExtensionRows(t *testing.T) {
	store := newStore(t)
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	fx := queryrepotest.SeedCoreFixtures(t, tx)
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	page, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope:    "-",
		PageSize: 50,
	})
	if err != nil {
		t.Fatalf("QueryResources: %v", err)
	}
	if len(page.Resources) == 0 {
		t.Fatal("QueryResources: got 0 resources, want seeded extension rows")
	}

	foundManaged := false
	for _, r := range page.Resources {
		if r.ResourceType == fx.ManagedType && r.Extension != nil {
			foundManaged = true
			if r.Name == "" {
				t.Error("managed row Name is empty")
			}
			if r.Kind != domain.QueryResourceKindExtension {
				t.Errorf("Kind = %q, want %q", r.Kind, domain.QueryResourceKindExtension)
			}
		}
	}
	if !foundManaged {
		t.Fatalf("managed fixture type %q not found in results", fx.ManagedType)
	}
}

func TestResourceQueryService_PassesFilterThrough(t *testing.T) {
	store := newStore(t)
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	fx := queryrepotest.SeedCoreFixtures(t, tx)
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	page, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope:    "-",
		Filter:   `resource_type == "` + string(fx.ManagedType) + `"`,
		PageSize: 50,
	})
	if err != nil {
		t.Fatalf("QueryResources: %v", err)
	}
	if len(page.Resources) != 1 {
		t.Fatalf("len(resources) = %d, want 1", len(page.Resources))
	}
	if page.Resources[0].ResourceType != fx.ManagedType {
		t.Errorf("ResourceType = %q, want %q", page.Resources[0].ResourceType, fx.ManagedType)
	}
}

func TestResourceQueryService_PassesInvalidOrderByThrough(t *testing.T) {
	store := newStore(t)
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	_, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope:   "-",
		OrderBy: "not_a_supported_order",
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("QueryResources(bad order_by): err = %v, want ErrInvalidArgument", err)
	}
}

func TestResourceQueryService_PassesInvalidFilterThrough(t *testing.T) {
	store := newStore(t)
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	_, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope:  "-",
		Filter: `kind == "Cluster"`, // legacy POC field; rejected by repository
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("QueryResources(bad filter): err = %v, want ErrInvalidArgument", err)
	}
}
