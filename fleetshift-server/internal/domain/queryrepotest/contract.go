// Package queryrepotest provides contract tests for
// [domain.QueryRepository] implementations.
package queryrepotest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Factory creates a fresh [domain.Tx] for each test. The Tx is needed
// because fixtures span [domain.Tx.ExtensionResources],
// [domain.Tx.Fulfillments], and [domain.Tx.ResourceIdentities] in
// addition to [domain.Tx.Queries] itself.
type Factory func(t *testing.T) domain.Tx

// RunUnimplemented exercises the contract for a backend that has not
// yet implemented QueryResources. It asserts the method exists and
// fails closed with [domain.ErrUnimplemented] rather than silently
// returning an empty page, so callers can distinguish "no results"
// from "not supported yet". Both Postgres and SQLite implement
// QueryResources; keep this helper for any future stub backend.
func RunUnimplemented(t *testing.T, factory Factory) {
	t.Run("QueryResourcesReturnsErrUnimplemented", func(t *testing.T) {
		tx := factory(t)
		defer tx.Rollback()

		_, err := tx.Queries().QueryResources(context.Background(), domain.QueryResourcesRequest{})
		if !errors.Is(err, domain.ErrUnimplemented) {
			t.Fatalf("QueryResources: got %v, want ErrUnimplemented", err)
		}
	})
}

// Run exercises the full [domain.QueryRepository] contract for a
// backend that implements CEL filtering over extension resources.
// Platform aggregate rows are out of scope for QueryResources today.
func Run(t *testing.T, factory Factory) {
	t.Run("EmptyFilter", func(t *testing.T) { runEmptyFilterTests(t, factory) })
	t.Run("HydrationEquivalence", func(t *testing.T) { runHydrationEquivalenceTests(t, factory) })
	t.Run("Pagination", func(t *testing.T) { runPaginationTests(t, factory) })
	t.Run("EnvelopeFieldFilters", func(t *testing.T) { runEnvelopeFieldFilterTests(t, factory) })
	t.Run("ResourceFieldFilters", func(t *testing.T) { runResourceFieldFilterTests(t, factory) })
	t.Run("ResourceTypesConstraint", func(t *testing.T) { runNilSchemaProviderTypeScopeTests(t, factory) })
	t.Run("CaseSensitivity", func(t *testing.T) { runCaseSensitivityFilterTests(t, factory) })
	t.Run("InvalidFilters", func(t *testing.T) { runInvalidFilterTests(t, factory) })
	t.Run("Hardening", func(t *testing.T) { runHardeningTests(t, factory) })
}

// newFixtureTx begins a fresh transaction via factory and seeds
// [SeedCoreFixtures] into it. Callers must `defer tx.Rollback()`.
func newFixtureTx(t *testing.T, factory Factory) (domain.Tx, Fixture) {
	t.Helper()
	tx := factory(t)
	fx := SeedCoreFixtures(t, tx)
	return tx, fx
}

// queryAll runs filter with a page size large enough to return every
// fixture row in one page, and fails the test on error.
func queryAll(t *testing.T, tx domain.Tx, filter string) []domain.QueryResourceResult {
	t.Helper()
	page, err := tx.Queries().QueryResources(context.Background(), domain.QueryResourcesRequest{
		Filter:   filter,
		PageSize: 500,
	})
	if err != nil {
		t.Fatalf("QueryResources(filter=%q): unexpected error: %v", filter, err)
	}
	return page.Resources
}

// queryErr runs filter and returns the error, failing the test if
// QueryResources unexpectedly succeeds.
func queryErr(t *testing.T, tx domain.Tx, req domain.QueryResourcesRequest) error {
	t.Helper()
	_, err := tx.Queries().QueryResources(context.Background(), req)
	if err == nil {
		t.Fatalf("QueryResources(%+v): got nil error, want an error", req)
	}
	return err
}

func findByName(results []domain.QueryResourceResult, name string) (domain.QueryResourceResult, bool) {
	for _, r := range results {
		if r.Name == name {
			return r, true
		}
	}
	return domain.QueryResourceResult{}, false
}

// extensionEnvelopeName builds the "//{service}/{name}" envelope name
// an extension row is expected to report.
func extensionEnvelopeName(rt domain.ResourceType, name domain.ResourceName) string {
	return string(rt.FullName(name))
}

func runEmptyFilterTests(t *testing.T, factory Factory) {
	t.Run("ReturnsOnlyExtensionRows", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, "")
		if len(results) != 2 {
			t.Fatalf("len(results) = %d, want 2 (managed + inventory-only extension rows)", len(results))
		}
		for _, r := range results {
			if r.Kind != domain.QueryResourceKindExtension {
				t.Errorf("result %q has Kind %q, want extension", r.Name, r.Kind)
			}
			if r.Platform != nil {
				t.Errorf("result %q: Platform is non-nil, want nil", r.Name)
			}
			if r.Extension == nil {
				t.Errorf("result %q: Extension is nil", r.Name)
			}
		}

		for _, name := range []string{
			extensionEnvelopeName(fx.ManagedType, fx.ManagedName),
			extensionEnvelopeName(fx.InventoryType, fx.InventoryName),
		} {
			if _, ok := findByName(results, name); !ok {
				t.Errorf("missing result %q", name)
			}
		}
	})

	t.Run("IgnoresPlatformOnlyResources", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		// Platform-only fixtures remain seeded for other repositories,
		// but QueryResources must not surface them.
		results := queryAll(t, tx, "")
		platformName := string(domain.NewFullResourceName("fleetshift.io", fx.PlatformOnlyName))
		if _, ok := findByName(results, platformName); ok {
			t.Errorf("unexpected platform-only result %q", platformName)
		}
	})
}

func runHydrationEquivalenceTests(t *testing.T, factory Factory) {
	t.Run("ExtensionProjectionEqualsGetView", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		want, err := tx.ExtensionResources().GetView(ctx, fx.ManagedType.FullName(fx.ManagedName))
		if err != nil {
			t.Fatalf("GetView: %v", err)
		}

		name := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		results := queryAll(t, tx, fmt.Sprintf("name == %q", name))
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1 for filter on %q", len(results), name)
		}
		got := results[0]
		if got.Extension == nil {
			t.Fatalf("result Extension is nil")
		}
		assertExtensionViewEqual(t, *got.Extension, want)
	})

	t.Run("InventoryOnlyProjectionEqualsGetView", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		want, err := tx.ExtensionResources().GetView(ctx, fx.InventoryType.FullName(fx.InventoryName))
		if err != nil {
			t.Fatalf("GetView: %v", err)
		}

		name := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		results := queryAll(t, tx, fmt.Sprintf("name == %q", name))
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1 for filter on %q", len(results), name)
		}
		got := results[0]
		if got.Extension == nil {
			t.Fatalf("result Extension is nil")
		}
		assertExtensionViewEqual(t, *got.Extension, want)
	})
}
