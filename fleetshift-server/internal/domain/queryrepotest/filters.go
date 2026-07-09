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
		if results[0].Kind != domain.QueryResourceKindExtension {
			t.Errorf("Kind = %q, want extension", results[0].Kind)
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
		if results[0].Platform != nil {
			t.Errorf("Platform is non-nil, want nil")
		}
	})

	t.Run("ResourceTypeNotEqualExcludesOnlyThatType", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		filter := fmt.Sprintf("resource_type != %q", string(fx.ManagedType))
		results := queryAll(t, tx, filter)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1 (inventory-only node)", len(results))
		}
		if results[0].Name != extensionEnvelopeName(fx.InventoryType, fx.InventoryName) {
			t.Errorf("result Name = %q, want the inventory-only node", results[0].Name)
		}
	})

	t.Run("OldPOCEnvelopeFieldsAreInvalid", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		for _, filter := range []string{
			`kind == "extension"`,
			`platform_name == "clusters/managed"`,
			`service_name == "kind.fleetshift.io"`,
			`api_version == "v1"`,
			`collection_name == "clusters"`,
			fmt.Sprintf(`resource_id == %q`, string(fx.ManagedName.ID())),
		} {
			err := queryErr(t, tx, domain.QueryResourcesRequest{Filter: filter})
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
			}
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

	t.Run("PlatformOnlyBodyFieldsAreInvalid", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		for _, filter := range []string{
			`resource.effective_labels["env"] == "prod"`,
			`resource.representations == "x"`,
			`resource.aliases == "x"`,
			`resource.relationships == "x"`,
		} {
			err := queryErr(t, tx, domain.QueryResourcesRequest{Filter: filter})
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
			}
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

		err := queryErr(t, tx, domain.QueryResourcesRequest{Filter: `name ==`})
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
