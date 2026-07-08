package queryrepotest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func runEnvelopeFieldFilterTests(t *testing.T, factory Factory) {
	t.Run("KindExtensionReturnsOnlyExtensionRows", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, `kind == "extension"`)
		if len(results) != 2 {
			t.Fatalf("len(results) = %d, want 2", len(results))
		}
		for _, r := range results {
			if r.Kind != domain.QueryResourceKindExtension {
				t.Errorf("result %q has Kind %q, want extension", r.Name, r.Kind)
			}
		}
	})

	t.Run("KindPlatformReturnsOnlyPlatformRows", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, `kind == "platform"`)
		if len(results) != 3 {
			t.Fatalf("len(results) = %d, want 3", len(results))
		}
		for _, r := range results {
			if r.Kind != domain.QueryResourceKindPlatform {
				t.Errorf("result %q has Kind %q, want platform", r.Name, r.Kind)
			}
		}
	})

	t.Run("ResourceTypeReturnsManagedCluster", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, fmt.Sprintf("resource_type == %q", string(fx.ManagedType)))
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != extensionEnvelopeName(fx.ManagedType, fx.ManagedName) {
			t.Errorf("result Name = %q, want the managed cluster's envelope name", results[0].Name)
		}
	})

	t.Run("NameReturnsThatExtensionResource", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		name := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		results := queryAll(t, tx, fmt.Sprintf("name == %q", name))
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Kind != domain.QueryResourceKindExtension {
			t.Errorf("Kind = %q, want extension", results[0].Kind)
		}
	})

	t.Run("PlatformRowsHaveEmptyResourceTypeAndAPIVersion", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		// Platform rows project resource_type/api_version as SQL ''
		// (not NULL), matching QueryResourceResult's documented
		// empty-string convention for "not applicable to this kind"
		// (see ResourceType's doc). If the CTE ever regresses to NULL,
		// this comparison goes to SQL's three-valued NULL rather than
		// TRUE and silently drops every platform row.
		results := queryAll(t, tx, `resource_type == "" && api_version == ""`)
		if len(results) != 3 {
			t.Fatalf("len(results) = %d, want 3 (all platform rows)", len(results))
		}
		for _, r := range results {
			if r.Kind != domain.QueryResourceKindPlatform {
				t.Errorf("result %q has Kind %q, want platform", r.Name, r.Kind)
			}
		}
	})

	t.Run("ResourceTypeNotEqualIncludesPlatformRows", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		// A negative resource_type filter should include rows that
		// have no resource_type at all (platform rows), not just
		// extension rows of a different type. Under NULL-based
		// projection, `NULL != 'literal'` is itself NULL (not TRUE),
		// so this previously dropped every platform row too.
		filter := fmt.Sprintf("resource_type != %q", string(fx.ManagedType))
		results := queryAll(t, tx, filter)

		var sawPlatform, sawOtherExtension bool
		for _, r := range results {
			switch r.Kind {
			case domain.QueryResourceKindPlatform:
				sawPlatform = true
			case domain.QueryResourceKindExtension:
				if r.Name == extensionEnvelopeName(fx.ManagedType, fx.ManagedName) {
					t.Errorf("result %q is the excluded resource_type, should not be present", r.Name)
				}
				sawOtherExtension = true
			}
		}
		if !sawPlatform {
			t.Error("expected platform rows to be included in a resource_type != ... filter")
		}
		if !sawOtherExtension {
			t.Error("expected the other extension type (inventory-only node) to be included")
		}
	})

	t.Run("PlatformNameReturnsBothPlatformAndExtensionRow", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, fmt.Sprintf("platform_name == %q", string(fx.ManagedName)))
		if len(results) != 2 {
			t.Fatalf("len(results) = %d, want 2 (virtual platform row + extension row)", len(results))
		}
		var sawPlatform, sawExtension bool
		for _, r := range results {
			switch r.Kind {
			case domain.QueryResourceKindPlatform:
				sawPlatform = true
			case domain.QueryResourceKindExtension:
				sawExtension = true
			}
		}
		if !sawPlatform || !sawExtension {
			t.Errorf("sawPlatform=%v sawExtension=%v, want both true", sawPlatform, sawExtension)
		}
	})
}

func runResourceFieldFilterTests(t *testing.T, factory Factory) {
	t.Run("ExtensionLabelsFilter", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, `resource.labels["team"] == "platform"`)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != extensionEnvelopeName(fx.ManagedType, fx.ManagedName) {
			t.Errorf("result Name = %q, want the managed cluster", results[0].Name)
		}
	})

	t.Run("PlatformLabelsFilter", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, `resource.labels["env"] == "prod"`)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != platformEnvelopeName(fx.PlatformOnlyName) {
			t.Errorf("result Name = %q, want the platform-only resource", results[0].Name)
		}
	})

	t.Run("SpecFilterGuardedByResourceType", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		filter := fmt.Sprintf(`resource_type == %q && resource.spec.provider == "aws"`, string(fx.ManagedType))
		results := queryAll(t, tx, filter)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != extensionEnvelopeName(fx.ManagedType, fx.ManagedName) {
			t.Errorf("result Name = %q, want the managed cluster", results[0].Name)
		}
	})

	t.Run("InventoryLabelsFilter", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, `resource.inventory.labels["node-role"] == "worker"`)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != extensionEnvelopeName(fx.InventoryType, fx.InventoryName) {
			t.Errorf("result Name = %q, want the inventory-only node", results[0].Name)
		}
	})

	t.Run("InventoryConditionsFilter", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, `resource.inventory.conditions["Ready"].status == "True"`)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != extensionEnvelopeName(fx.InventoryType, fx.InventoryName) {
			t.Errorf("result Name = %q, want the inventory-only node", results[0].Name)
		}
	})

	t.Run("NumericObservationComparison", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		filter := fmt.Sprintf(`resource_type == %q && resource.inventory.observation.capacity.cpu > 4`, string(fx.InventoryType))
		results := queryAll(t, tx, filter)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != extensionEnvelopeName(fx.InventoryType, fx.InventoryName) {
			t.Errorf("result Name = %q, want the inventory-only node", results[0].Name)
		}

		// A threshold the fixture's cpu=8 observation does not clear
		// should exclude the row, proving the comparison is a real
		// numeric one and not e.g. a always-true text comparison.
		filter = fmt.Sprintf(`resource_type == %q && resource.inventory.observation.capacity.cpu > 100`, string(fx.InventoryType))
		results = queryAll(t, tx, filter)
		if len(results) != 0 {
			t.Fatalf("len(results) = %d, want 0 for an unmet numeric threshold", len(results))
		}
	})

	t.Run("NumericComparisonSafeAcrossConflictingResourceTypes", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		// Seed a second resource type sharing the exact same
		// observation JSON path (capacity.cpu) as fx.InventoryType,
		// but with a non-numeric value at that path. A plain
		// WHERE-clause AND gives Postgres no evaluation-order
		// guarantee, so an unconditional ::numeric cast on this
		// column can still be attempted against this row even when
		// the query is guarded by resource_type == fx.InventoryType;
		// see querysql's safeJSONCast doc.
		conflictType := domain.ResourceType("kubernetes.fleetshift.io/BrokenNode")
		seedInventoryType(t, tx, conflictType)
		conflictUID := domain.NewExtensionResourceUID()
		conflictName := domain.ResourceName("nodes/broken-node")
		conflict := domain.NewExtensionResource(conflictUID, conflictType, conflictName, fixedTime)
		if err := tx.ExtensionResources().Create(ctx, conflict); err != nil {
			t.Fatalf("seed conflicting-type resource: %v", err)
		}
		conflictObs := json.RawMessage(`{"capacity":{"cpu":"unknown"}}`)
		if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
			ResourceType: conflictType,
			Name:         conflictName,
			CandidateUID: conflictUID,
			Observation:  &conflictObs,
			ObservedAt:   fixedTime,
			ReceivedAt:   fixedTime,
		}}); err != nil {
			t.Fatalf("seed conflicting-type inventory: %v", err)
		}

		filter := fmt.Sprintf(`resource_type == %q && resource.inventory.observation.capacity.cpu > 4`, string(fx.InventoryType))
		results := queryAll(t, tx, filter)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != extensionEnvelopeName(fx.InventoryType, fx.InventoryName) {
			t.Errorf("result Name = %q, want the guarded inventory-only node", results[0].Name)
		}
	})

	t.Run("BooleanJSONComparison", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		ctx := context.Background()

		rt := domain.ResourceType("kind.fleetshift.io/Cluster")
		obs := `{"healthy":true}`
		if err := tx.ExtensionResources().ReplaceInventory(ctx, []domain.InventoryReplacement{{
			ResourceType: rt,
			Name:         fx.ManagedName,
			CandidateUID: fx.ManagedUID,
			Observation:  jsonPtr(obs),
			ObservedAt:   fixedTime,
			ReceivedAt:   fixedTime,
		}}); err != nil {
			t.Fatalf("seed boolean observation: %v", err)
		}

		filter := fmt.Sprintf(`resource_type == %q && resource.inventory.observation.healthy == true`, string(rt))
		results := queryAll(t, tx, filter)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != extensionEnvelopeName(fx.ManagedType, fx.ManagedName) {
			t.Errorf("result Name = %q, want the managed cluster", results[0].Name)
		}
	})
}

func runInvalidFilterTests(t *testing.T, factory Factory) {
	t.Run("UnsupportedFieldIsInvalid", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		err := queryErr(t, tx, domain.QueryResourcesRequest{Filter: `resource.aliases == "x"`})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("InvalidSyntaxIsInvalid", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		err := queryErr(t, tx, domain.QueryResourcesRequest{Filter: `kind ==`})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("UnsupportedMacroIsInvalid", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		err := queryErr(t, tx, domain.QueryResourcesRequest{Filter: `["a","b"].exists(x, x == "a")`})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Errorf("err = %v, want ErrInvalidArgument", err)
		}
	})
}

func jsonPtr(s string) *json.RawMessage {
	m := json.RawMessage(s)
	return &m
}
