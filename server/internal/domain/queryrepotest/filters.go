package queryrepotest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func runEnvelopeFieldFilterTests(t *testing.T, factory Factory) {
	t.Run("ResourceTypeReturnsManagedCluster", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, fmt.Sprintf("resourceType == %q", string(fx.ManagedType)))
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

	t.Run("NameStartsWithMatchesPrefix", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		wantName := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		results := queryAll(t, tx, fmt.Sprintf(`name.startsWith(%q)`, "//"+string(fx.ManagedType.ServiceName())+"/"))
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != wantName {
			t.Errorf("Name = %q, want %q", results[0].Name, wantName)
		}

		results = queryAll(t, tx, `name.startsWith("//does-not-exist/")`)
		if len(results) != 0 {
			t.Fatalf("len(results) = %d, want 0 for a non-matching prefix", len(results))
		}
	})

	t.Run("ResourceTypeNotEqualExcludesOnlyThatType", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		filter := fmt.Sprintf("resourceType != %q", string(fx.ManagedType))
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

	t.Run("ExtensionLabelsStartsWith", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		wantName := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		results := queryAll(t, tx, `resource.labels["team"].startsWith("plat")`)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != wantName {
			t.Errorf("result Name = %q, want the managed cluster", results[0].Name)
		}

		results = queryAll(t, tx, `resource.labels["team"].startsWith("ops")`)
		if len(results) != 0 {
			t.Fatalf("len(results) = %d, want 0 for a non-matching prefix", len(results))
		}
	})

	t.Run("SpecFilterGuardedByResourceType", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		filter := fmt.Sprintf(`resourceType == %q && resource.spec.provider == "aws"`, string(fx.ManagedType))
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

		results := queryAll(t, tx, `resource.localLabels["node-role"] == "worker"`)
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

		results := queryAll(t, tx, `resource.conditions["Ready"].status == "True"`)
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

		filter := fmt.Sprintf(`resourceType == %q && resource.observation.capacity.cpu > 4`, string(fx.InventoryType))
		results := queryAll(t, tx, filter)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != extensionEnvelopeName(fx.InventoryType, fx.InventoryName) {
			t.Errorf("result Name = %q, want the inventory-only node", results[0].Name)
		}

		filter = fmt.Sprintf(`resourceType == %q && resource.observation.capacity.cpu > 100`, string(fx.InventoryType))
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

		filter := fmt.Sprintf(`resourceType == %q && resource.observation.capacity.cpu > 4`, string(fx.InventoryType))
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

		filter := fmt.Sprintf(`resourceType == %q && resource.observation.healthy == true`, string(rt))
		results := queryAll(t, tx, filter)
		if len(results) != 1 {
			t.Fatalf("len(results) = %d, want 1", len(results))
		}
		if results[0].Name != extensionEnvelopeName(fx.ManagedType, fx.ManagedName) {
			t.Errorf("result Name = %q, want the managed cluster", results[0].Name)
		}
	})

	t.Run("StateFilterMatchesAPIEnumSpelling", func(t *testing.T) {
		// fulfillments.state is stored lowercase ("creating"); Get/List
		// and QueryResources expose the ProtoJSON enum name
		// ("CREATING"). Filters match the response spelling only.
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		wantName := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		results := queryAll(t, tx, `resource.state == "CREATING"`)
		if len(results) != 1 {
			t.Fatalf(`resource.state == "CREATING": len(results) = %d, want 1`, len(results))
		}
		if results[0].Name != wantName {
			t.Errorf("Name = %q, want %q", results[0].Name, wantName)
		}

		results = queryAll(t, tx, `resource.state == "creating"`)
		if len(results) != 0 {
			t.Fatalf(`resource.state == "creating": len(results) = %d, want 0 (storage spelling is not an alias)`, len(results))
		}

		results = queryAll(t, tx, `resource.state == "ACTIVE"`)
		if len(results) != 0 {
			t.Fatalf(`resource.state == "ACTIVE": len(results) = %d, want 0 (fixture is creating)`, len(results))
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

// runCaseSensitivityFilterTests locks the cross-backend case contract:
// filter matching is case-sensitive over the ProtoJSON response
// spelling for identity strings, labels, and resource.state.
func runCaseSensitivityFilterTests(t *testing.T, factory Factory) {
	t.Run("NameEqualityIsCaseSensitive", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		name := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		results := queryAll(t, tx, fmt.Sprintf("name == %q", name))
		if len(results) != 1 {
			t.Fatalf("exact name: len(results) = %d, want 1", len(results))
		}

		results = queryAll(t, tx, fmt.Sprintf("name == %q", strings.ToUpper(name)))
		if len(results) != 0 {
			t.Fatalf("uppercased name: len(results) = %d, want 0 (equality is case-sensitive)", len(results))
		}
	})

	t.Run("NameStartsWithIsCaseSensitive", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		prefix := "//" + string(fx.ManagedType.ServiceName()) + "/"
		results := queryAll(t, tx, fmt.Sprintf(`name.startsWith(%q)`, prefix))
		if len(results) != 1 {
			t.Fatalf("matching prefix: len(results) = %d, want 1", len(results))
		}

		results = queryAll(t, tx, fmt.Sprintf(`name.startsWith(%q)`, strings.ToUpper(prefix)))
		if len(results) != 0 {
			t.Fatalf("uppercased prefix: len(results) = %d, want 0 (startsWith is case-sensitive)", len(results))
		}
	})

	t.Run("LabelEqualityIsCaseSensitive", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, `resource.labels["team"] == "platform"`)
		if len(results) != 1 {
			t.Fatalf(`== "platform": len(results) = %d, want 1`, len(results))
		}

		results = queryAll(t, tx, `resource.labels["team"] == "PLATFORM"`)
		if len(results) != 0 {
			t.Fatalf(`== "PLATFORM": len(results) = %d, want 0`, len(results))
		}
	})

	t.Run("LabelStartsWithIsCaseSensitive", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		results := queryAll(t, tx, `resource.labels["team"].startsWith("plat")`)
		if len(results) != 1 {
			t.Fatalf(`startsWith("plat"): len(results) = %d, want 1`, len(results))
		}

		results = queryAll(t, tx, `resource.labels["team"].startsWith("PLAT")`)
		if len(results) != 0 {
			t.Fatalf(`startsWith("PLAT"): len(results) = %d, want 0`, len(results))
		}
	})

	t.Run("StateEqualityIsCaseSensitive", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		wantName := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		results := queryAll(t, tx, `resource.state == "CREATING"`)
		if len(results) != 1 {
			t.Fatalf(`== "CREATING": len(results) = %d, want 1`, len(results))
		}
		if results[0].Name != wantName {
			t.Errorf("Name = %q, want %q", results[0].Name, wantName)
		}

		results = queryAll(t, tx, `resource.state == "creating"`)
		if len(results) != 0 {
			t.Fatalf(`== "creating": len(results) = %d, want 0`, len(results))
		}
	})

	t.Run("StateStartsWithIsCaseSensitive", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		wantName := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		results := queryAll(t, tx, `resource.state.startsWith("CRE")`)
		if len(results) != 1 {
			t.Fatalf(`startsWith("CRE"): len(results) = %d, want 1`, len(results))
		}
		if results[0].Name != wantName {
			t.Errorf("Name = %q, want %q", results[0].Name, wantName)
		}

		results = queryAll(t, tx, `resource.state.startsWith("cre")`)
		if len(results) != 0 {
			t.Fatalf(`startsWith("cre"): len(results) = %d, want 0`, len(results))
		}

		results = queryAll(t, tx, `resource.state.startsWith("ACT")`)
		if len(results) != 0 {
			t.Fatalf(`startsWith("ACT"): len(results) = %d, want 0`, len(results))
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

	t.Run("NestedInventoryPathsRejected", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		for _, filter := range []string{
			`resource.inventory.labels["node-role"] == "worker"`,
			`resource.inventory.conditions["Ready"].status == "True"`,
			`resourceType == "kubernetes.fleetshift.io/Node" && resource.inventory.observation.capacity.cpu > 4`,
		} {
			err := queryErr(t, tx, domain.QueryResourcesRequest{Filter: filter})
			if !errors.Is(err, domain.ErrInvalidArgument) {
				t.Errorf("filter %q: err = %v, want ErrInvalidArgument", filter, err)
			}
		}
	})
}

func runNilSchemaProviderTypeScopeTests(t *testing.T, factory Factory) {
	t.Run("NilSchemaProviderMeansNoActivationScope", func(t *testing.T) {
		tx, _ := newFixtureTx(t, factory)
		defer tx.Rollback()

		page, err := tx.Queries().QueryResources(context.Background(), domain.QueryResourcesRequest{
			PageSize: 500,
		})
		if err != nil {
			t.Fatalf("QueryResources: %v", err)
		}
		if len(page.Resources) != 2 {
			t.Fatalf("len(resources) = %d, want 2 (nil SchemaProvider does not invent a type scope)", len(page.Resources))
		}
	})
}

func jsonPtr(s string) *json.RawMessage {
	m := json.RawMessage(s)
	return &m
}
