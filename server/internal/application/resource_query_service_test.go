package application_test

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/queryrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

// staticQuerySchemas is a minimal [domain.QuerySchemaProvider] for
// ResourceQueryService tests. Prefer a real registry in transport
// tests; this keeps application tests free of transport imports.
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

func schemasFor(types ...domain.ResourceType) staticQuerySchemas {
	out := make(staticQuerySchemas, len(types))
	for _, rt := range types {
		out[rt] = domain.ResourceQuerySchema{ResourceType: rt}
	}
	return out
}

func newQueryStore(t *testing.T, schemas domain.QuerySchemaProvider) *sqlite.Store {
	t.Helper()
	return &sqlite.Store{DB: sqlite.OpenTestDB(t), SchemaProvider: schemas}
}

func seedQueryFixtures(t *testing.T, store domain.Store) queryrepotest.Fixture {
	t.Helper()
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	fx := queryrepotest.SeedCoreFixtures(t, tx)
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	return fx
}

func TestResourceQueryService_RejectsNonWildcardScope(t *testing.T) {
	store := newQueryStore(t, schemasFor())
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
	store := newQueryStore(t, schemasFor())
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	_, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope: "",
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("QueryResources(scope=\"\"): err = %v, want ErrInvalidArgument", err)
	}
}

func TestResourceQueryService_EmptyActivatedReturnsEmptyPage(t *testing.T) {
	store := newQueryStore(t, schemasFor())
	_ = seedQueryFixtures(t, store)
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	page, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope:    "-",
		PageSize: 50,
	})
	if err != nil {
		t.Fatalf("QueryResources: %v", err)
	}
	if len(page.Resources) != 0 {
		t.Fatalf("len(resources) = %d, want 0 (empty activated set)", len(page.Resources))
	}
}

func TestResourceQueryService_WildcardReturnsExtensionRows(t *testing.T) {
	store := newQueryStore(t, nil)
	fx := seedQueryFixtures(t, store)
	// Wire activation after seeding so the store sees the fixture types.
	store.SchemaProvider = schemasFor(fx.ManagedType, fx.InventoryType)
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

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
	store := newQueryStore(t, nil)
	fx := seedQueryFixtures(t, store)
	store.SchemaProvider = schemasFor(fx.ManagedType, fx.InventoryType)
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	page, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope:    "-",
		Filter:   `resourceType == "` + string(fx.ManagedType) + `"`,
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
	store := newQueryStore(t, schemasFor(domain.ResourceType("kind.fleetshift.io/Cluster")))
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
	store := newQueryStore(t, schemasFor(domain.ResourceType("kind.fleetshift.io/Cluster")))
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

func TestResourceQueryService_ActivatedTypesStableOrder(t *testing.T) {
	store := newQueryStore(t, nil)
	fx := seedQueryFixtures(t, store)
	// Map iteration order is randomized; activated types must still
	// sort stably so page-token filter hashes match across calls.
	schemas := schemasFor(fx.InventoryType, fx.ManagedType)
	store.SchemaProvider = schemas
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	var firstToken string
	for i := 0; i < 20; i++ {
		page, err := svc.QueryResources(ctx, application.QueryResourcesInput{
			Scope:    "-",
			PageSize: 1,
		})
		if err != nil {
			t.Fatalf("QueryResources[%d]: %v", i, err)
		}
		if page.NextPageToken == "" {
			t.Fatal("expected NextPageToken with PageSize=1 over 2 activated types")
		}
		if i == 0 {
			firstToken = page.NextPageToken
			continue
		}
		// Resume with the first call's token — must not fail even if
		// ListResourceQuerySchemas returned a different map order.
		_, err = svc.QueryResources(ctx, application.QueryResourcesInput{
			Scope:     "-",
			PageSize:  1,
			PageToken: firstToken,
		})
		if err != nil {
			t.Fatalf("resume with page token after reorder attempt %d: %v", i, err)
		}
	}
}

func TestResourceQueryService_DefaultsToActivatedTypes(t *testing.T) {
	store := newQueryStore(t, nil)
	fx := seedQueryFixtures(t, store)
	// Activate only managed; inventory rows remain in the store.
	store.SchemaProvider = schemasFor(fx.ManagedType)
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	page, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope:    "-",
		PageSize: 50,
	})
	if err != nil {
		t.Fatalf("QueryResources: %v", err)
	}
	if len(page.Resources) != 1 {
		t.Fatalf("len(resources) = %d, want 1 (managed only)", len(page.Resources))
	}
	if page.Resources[0].ResourceType != fx.ManagedType {
		t.Errorf("ResourceType = %q, want %q", page.Resources[0].ResourceType, fx.ManagedType)
	}
}

func TestResourceQueryService_RejectsInactiveResourceTypeFilter(t *testing.T) {
	store := newQueryStore(t, nil)
	fx := seedQueryFixtures(t, store)
	store.SchemaProvider = schemasFor(fx.ManagedType)
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	_, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope:  "-",
		Filter: `resourceType == "` + string(fx.InventoryType) + `"`,
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("QueryResources(inactive type): err = %v, want ErrInvalidArgument", err)
	}
}

func TestResourceQueryService_AfterDeactivateTypeLeavesDefaultSet(t *testing.T) {
	store := newQueryStore(t, nil)
	fx := seedQueryFixtures(t, store)
	schemas := schemasFor(fx.ManagedType, fx.InventoryType)
	store.SchemaProvider = schemas
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	page, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope:    "-",
		PageSize: 50,
	})
	if err != nil {
		t.Fatalf("QueryResources before deactivate: %v", err)
	}
	if len(page.Resources) != 2 {
		t.Fatalf("before deactivate: len(resources) = %d, want 2", len(page.Resources))
	}

	delete(schemas, fx.InventoryType)

	page, err = svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope:    "-",
		PageSize: 50,
	})
	if err != nil {
		t.Fatalf("QueryResources after deactivate: %v", err)
	}
	if len(page.Resources) != 1 {
		t.Fatalf("after deactivate: len(resources) = %d, want 1", len(page.Resources))
	}
	if page.Resources[0].ResourceType != fx.ManagedType {
		t.Errorf("ResourceType = %q, want %q", page.Resources[0].ResourceType, fx.ManagedType)
	}
}

func TestResourceQueryService_ResourceTypeInListValidated(t *testing.T) {
	store := newQueryStore(t, nil)
	fx := seedQueryFixtures(t, store)
	store.SchemaProvider = schemasFor(fx.ManagedType)
	svc := application.NewResourceQueryService(store)
	ctx := context.Background()

	_, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope:  "-",
		Filter: `resourceType in ["` + string(fx.ManagedType) + `", "` + string(fx.InventoryType) + `"]`,
	})
	if !errors.Is(err, domain.ErrInvalidArgument) {
		t.Fatalf("QueryResources(in with inactive): err = %v, want ErrInvalidArgument", err)
	}

	page, err := svc.QueryResources(ctx, application.QueryResourcesInput{
		Scope:    "-",
		Filter:   `resourceType in ["` + string(fx.ManagedType) + `"]`,
		PageSize: 50,
	})
	if err != nil {
		t.Fatalf("QueryResources(in activated only): %v", err)
	}
	if len(page.Resources) != 1 || page.Resources[0].ResourceType != fx.ManagedType {
		t.Fatalf("got %#v, want single managed row", page.Resources)
	}
}
