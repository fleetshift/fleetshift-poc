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
// yet implemented QueryResources -- see the QueryRepository POC plan's
// "SQLite" section. It asserts the method exists and fails closed with
// [domain.ErrUnimplemented] rather than silently returning an empty
// page, so callers can distinguish "no results" from "not supported
// yet".
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

// Run exercises the full [domain.QueryRepository] contract: a
// backend that implements CEL filtering over
// [SeedCoreFixtures]'s platform/extension/virtual resource fixtures.
// See the QueryRepository POC plan's "Tests" section for the full
// required-test-case list this mirrors.
func Run(t *testing.T, factory Factory) {
	t.Run("EmptyFilter", func(t *testing.T) { runEmptyFilterTests(t, factory) })
	t.Run("HydrationEquivalence", func(t *testing.T) { runHydrationEquivalenceTests(t, factory) })
	t.Run("Pagination", func(t *testing.T) { runPaginationTests(t, factory) })
	t.Run("EnvelopeFieldFilters", func(t *testing.T) { runEnvelopeFieldFilterTests(t, factory) })
	t.Run("ResourceFieldFilters", func(t *testing.T) { runResourceFieldFilterTests(t, factory) })
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

// platformEnvelopeName builds the "//fleetshift.io/{name}" envelope
// name a platform row (physical or virtual) is expected to report.
func platformEnvelopeName(name domain.ResourceName) string {
	return string(domain.NewFullResourceName("fleetshift.io", name))
}

// extensionEnvelopeName builds the "//{service}/{name}" envelope name
// an extension row is expected to report.
func extensionEnvelopeName(rt domain.ResourceType, name domain.ResourceName) string {
	return string(rt.FullName(name))
}

func runEmptyFilterTests(t *testing.T, factory Factory) {
	t.Run("ReturnsBothKinds", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, "")

		var platformCount, extensionCount int
		for _, r := range results {
			switch r.Kind {
			case domain.QueryResourceKindPlatform:
				platformCount++
			case domain.QueryResourceKindExtension:
				extensionCount++
			default:
				t.Errorf("result %q has unexpected Kind %q", r.Name, r.Kind)
			}
		}
		// 2 extension rows (managed + inventory-only) and 3 platform
		// rows (the physical platform-only resource, plus a virtual
		// platform resource derived from each of the two extension
		// resources -- neither has a physical platform_resources row).
		if extensionCount != 2 {
			t.Errorf("extensionCount = %d, want 2", extensionCount)
		}
		if platformCount != 3 {
			t.Errorf("platformCount = %d, want 3", platformCount)
		}

		for _, name := range []string{
			platformEnvelopeName(fx.PlatformOnlyName),
			extensionEnvelopeName(fx.ManagedType, fx.ManagedName),
			extensionEnvelopeName(fx.InventoryType, fx.InventoryName),
		} {
			if _, ok := findByName(results, name); !ok {
				t.Errorf("missing result %q", name)
			}
		}
	})

	t.Run("IncludesVirtualPlatformResources", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, `kind == "platform"`)
		for _, name := range []string{
			platformEnvelopeName(fx.ManagedName),
			platformEnvelopeName(fx.InventoryName),
		} {
			r, ok := findByName(results, name)
			if !ok {
				t.Errorf("missing virtual platform result %q", name)
				continue
			}
			if r.Platform == nil {
				t.Errorf("result %q: Platform is nil", name)
			}
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

	t.Run("PlatformProjectionEqualsGetByName", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		want, err := tx.ResourceIdentities().GetByName(ctx, fx.PlatformOnlyName)
		if err != nil {
			t.Fatalf("GetByName: %v", err)
		}

		name := platformEnvelopeName(fx.PlatformOnlyName)
		results := queryAll(t, tx, fmt.Sprintf("name == %q", name))
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1 for filter on %q", len(results), name)
		}
		got := results[0]
		if got.Platform == nil {
			t.Fatalf("result Platform is nil")
		}
		assertPlatformResourceEqual(t, got.Platform, want)
	})

	t.Run("VirtualPlatformProjectionEqualsGetByName", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		want, err := tx.ResourceIdentities().GetByName(ctx, fx.ManagedName)
		if err != nil {
			t.Fatalf("GetByName (virtual): %v", err)
		}

		name := platformEnvelopeName(fx.ManagedName)
		results := queryAll(t, tx, fmt.Sprintf(`kind == "platform" && name == %q`, name))
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1 for filter on %q", len(results), name)
		}
		got := results[0]
		if got.Platform == nil {
			t.Fatalf("result Platform is nil")
		}
		assertPlatformResourceEqual(t, got.Platform, want)
	})
}
