package queryrepotest

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// runPresenceFilterTests locks projection-faithful has() / container
// membership semantics against production-like writers (Create,
// RecordIntent, ReplaceInventory). Backend SQL shape is out of scope.
func runPresenceFilterTests(t *testing.T, factory Factory) {
	t.Run("ConditionDefaultOmission", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		// Valid status, empty reason/message, zero transition time —
		// inventory JSON stores those keys; ProtoJSON omits the defaults.
		ready, err := domain.NewCondition("Ready", domain.ConditionTrue, "", "", time.Time{})
		if err != nil {
			t.Fatalf("NewCondition: %v", err)
		}
		replaceInventory(t, tx, fx, map[string]string{"node-role": "worker"},
			json.RawMessage(`{"capacity":{"cpu":8}}`), []domain.Condition{ready})

		wantName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		assertResultNames(t, queryAll(t, tx, `has(resource.conditions.Ready)`), wantName)
		assertResultNames(t, queryAll(t, tx, `"Ready" in resource.conditions`), wantName)
		assertResultNames(t, queryAll(t, tx, `has(resource.conditions.Ready.status)`), wantName)
		assertResultNames(t, queryAll(t, tx, `"status" in resource.conditions.Ready`), wantName)
		assertResultNames(t, queryAll(t, tx, `has(resource.conditions.Ready.reason)`))
		assertResultNames(t, queryAll(t, tx, `"reason" in resource.conditions.Ready`))
		assertResultNames(t, queryAll(t, tx, `has(resource.conditions.Ready.message)`))
		assertResultNames(t, queryAll(t, tx, `"message" in resource.conditions.Ready`))
		assertResultNames(t, queryAll(t, tx, `has(resource.conditions.Ready.lastTransitionTime)`))
		assertResultNames(t, queryAll(t, tx, `"lastTransitionTime" in resource.conditions.Ready`))

		populated, err := domain.NewCondition("Ready", domain.ConditionTrue, "NodeReady", "node is ready", fixedTime)
		if err != nil {
			t.Fatalf("NewCondition populated: %v", err)
		}
		replaceInventory(t, tx, fx, map[string]string{"node-role": "worker"},
			json.RawMessage(`{"capacity":{"cpu":8}}`), []domain.Condition{populated})

		assertResultNames(t, queryAll(t, tx, `has(resource.conditions.Ready.reason)`), wantName)
		assertResultNames(t, queryAll(t, tx, `has(resource.conditions.Ready.message)`), wantName)
		assertResultNames(t, queryAll(t, tx, `has(resource.conditions.Ready.lastTransitionTime)`), wantName)
		assertResultNames(t, queryAll(t, tx, `"reason" in resource.conditions.Ready`), wantName)
		assertResultNames(t, queryAll(t, tx, `"message" in resource.conditions.Ready`), wantName)
		assertResultNames(t, queryAll(t, tx, `"lastTransitionTime" in resource.conditions.Ready`), wantName)
	})

	t.Run("CapabilityMissingInventory", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		managedName := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		// Managed-only: no inventory capability / row.
		scope := fmt.Sprintf(`name == %q && `, managedName)
		assertResultNames(t, queryAll(t, tx, scope+`has(resource.observation)`))
		assertResultNames(t, queryAll(t, tx, scope+`has(resource.localLabels)`))
		assertResultNames(t, queryAll(t, tx, scope+`has(resource.conditions)`))
		assertResultNames(t, queryAll(t, tx, scope+`"Ready" in resource.conditions`))
		assertResultNames(t, queryAll(t, tx, scope+`has(resource.localUpdateTime)`))
		assertResultNames(t, queryAll(t, tx, scope+`has(resource.indexUpdateTime)`))
		assertResultNames(t, queryAll(t, tx, scope+`!has(resource.observation)`), managedName)
		// Soft-or: managed still matches by type when observation is absent.
		assertResultNames(t, queryAll(t, tx,
			fmt.Sprintf(`name == %q && (has(resource.observation) || resourceType == %q)`, managedName, string(fx.ManagedType))),
			managedName)

		// Inventory-capable type, resource exists, no report yet.
		unreportedUID := domain.NewExtensionResourceUID()
		unreportedName := domain.ResourceName("nodes/unreported")
		unreported := domain.NewExtensionResource(unreportedUID, fx.InventoryType, unreportedName, fixedTime)
		if err := tx.ExtensionResources().Create(context.Background(), unreported); err != nil {
			t.Fatalf("Create unreported: %v", err)
		}
		assertResultNames(t, queryAll(t, tx,
			fmt.Sprintf(`name == %q && has(resource.observation)`, extensionEnvelopeName(fx.InventoryType, unreportedName))))
		assertResultNames(t, queryAll(t, tx,
			fmt.Sprintf(`name == %q && has(resource.localLabels)`, extensionEnvelopeName(fx.InventoryType, unreportedName))))
		assertResultNames(t, queryAll(t, tx,
			fmt.Sprintf(`name == %q && has(resource.conditions)`, extensionEnvelopeName(fx.InventoryType, unreportedName))))

		// Empty observation object remains present.
		emptyObs := json.RawMessage(`{}`)
		if err := tx.ExtensionResources().ReplaceInventory(context.Background(), []domain.InventoryReplacement{{
			ResourceType: fx.InventoryType,
			Name:         unreportedName,
			CandidateUID: unreportedUID,
			Observation:  &emptyObs,
			ObservedAt:   fixedTime,
			ReceivedAt:   fixedTime,
		}}); err != nil {
			t.Fatalf("ReplaceInventory empty obs: %v", err)
		}
		assertResultNames(t, queryAll(t, tx,
			fmt.Sprintf(`name == %q && has(resource.observation)`, extensionEnvelopeName(fx.InventoryType, unreportedName))),
			extensionEnvelopeName(fx.InventoryType, unreportedName))
	})

	t.Run("ContainersMapsAndScalars", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		managedName := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		invName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)

		assertResultNames(t, queryAll(t, tx, `has(resource.spec)`), managedName)
		assertResultNames(t, queryAll(t, tx, `"spec" in resource`), managedName)
		assertResultNames(t, queryAll(t, tx, `has(resource.labels)`), managedName)
		assertResultNames(t, queryAll(t, tx, `has(resource.labels.team)`), managedName)
		assertResultNames(t, queryAll(t, tx, `"team" in resource.labels`), managedName)
		assertResultNames(t, queryAll(t, tx, `has(resource.labels.missing)`))
		assertResultNames(t, queryAll(t, tx, `has(resource.observation)`), invName)
		assertResultNames(t, queryAll(t, tx, `has(resource.localLabels)`), invName)
		assertResultNames(t, queryAll(t, tx, `has(resource.conditions)`), invName)
		assertResultNames(t, queryAll(t, tx, `"node-role" in resource.localLabels`), invName)
		assertResultNames(t, queryAll(t, tx, `has(resource.spec) && has(resource.observation)`))

		// Nested observation path + hyphenated key via in.
		replaceInventory(t, tx, fx, map[string]string{"node-role": "worker"},
			json.RawMessage(`{"capacity":{"cpu":8},"tags":{"a":1},"list":["k","x"],"nullKey":null}`),
			nil)
		assertResultNames(t, queryAll(t, tx, `has(resource.observation.capacity)`), invName)
		assertResultNames(t, queryAll(t, tx, `has(resource.observation.nullKey)`), invName)
		assertResultNames(t, queryAll(t, tx, `"a" in resource.observation.tags`), invName)
		assertResultNames(t, queryAll(t, tx, `"k" in resource.observation.list`), invName)
		assertResultNames(t, queryAll(t, tx, `"missing" in resource.observation.tags`))
	})

	t.Run("InvariantBodyFields", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()
		managedName := extensionEnvelopeName(fx.ManagedType, fx.ManagedName)
		invName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		assertResultNames(t, queryAll(t, tx, `has(resource.name)`), managedName, invName)
		assertResultNames(t, queryAll(t, tx, `has(resource.uid)`), managedName, invName)
	})
}

func replaceInventory(
	t *testing.T,
	tx domain.Tx,
	fx Fixture,
	labels map[string]string,
	obs json.RawMessage,
	conds []domain.Condition,
) {
	t.Helper()
	var obsPtr *json.RawMessage
	if obs != nil {
		obsPtr = &obs
	}
	if err := tx.ExtensionResources().ReplaceInventory(context.Background(), []domain.InventoryReplacement{{
		ResourceType: fx.InventoryType,
		Name:         fx.InventoryName,
		CandidateUID: fx.InventoryUID,
		Labels:       labels,
		Observation:  obsPtr,
		Conditions:   conds,
		ObservedAt:   fixedTime,
		ReceivedAt:   fixedTime,
	}}); err != nil {
		t.Fatalf("ReplaceInventory: %v", err)
	}
}
