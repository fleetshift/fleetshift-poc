package queryrepotest

import (
	"encoding/json"
	"fmt"
	"testing"
)

// runMembershipFilterTests locks the schema-free (dynamic) membership
// truth table for `"k" in <json path>`. Schema-specialized lowering
// (descriptorContainerKind + ResolveMembership SQL shape for known
// message/map/list, scalar rejection, and open-Struct parity with this
// dynamic path) is covered in backend query_filter_test.go; for
// schema-conforming values those branches must match this table.
func runMembershipFilterTests(t *testing.T, factory Factory) {
	t.Run("DynamicObjectKeyMembership", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		wantName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		replaceInventory(t, tx, fx, nil,
			json.RawMessage(`{"foo":{"k":1,"nullKey":null,"other":2}}`), nil)

		scope := fmt.Sprintf(`resourceType == %q && `, string(fx.InventoryType))
		assertResultNames(t, queryAll(t, tx, scope+`"k" in resource.observation.foo`), wantName)
		assertResultNames(t, queryAll(t, tx, scope+`"nullKey" in resource.observation.foo`), wantName)
		assertResultNames(t, queryAll(t, tx, scope+`"missing" in resource.observation.foo`))
	})

	t.Run("DynamicListValueMembership", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		wantName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		replaceInventory(t, tx, fx, nil,
			json.RawMessage(`{"foo":["k", 1, true, "x"]}`), nil)

		scope := fmt.Sprintf(`resourceType == %q && `, string(fx.InventoryType))
		assertResultNames(t, queryAll(t, tx, scope+`"k" in resource.observation.foo`), wantName)
		assertResultNames(t, queryAll(t, tx, scope+`"x" in resource.observation.foo`), wantName)
		assertResultNames(t, queryAll(t, tx, scope+`"missing" in resource.observation.foo`))
		// Non-string list elements do not satisfy string membership.
		assertResultNames(t, queryAll(t, tx, scope+`"1" in resource.observation.foo`))
	})

	t.Run("NestedOnlyListOccurrencesDoNotMatch", func(t *testing.T) {
		// "k" appears only inside nested array/object elements, never as
		// a top-level string element. CEL list membership (and both
		// store lowerings) must not treat nested occurrences as hits —
		// including Postgres @> containment, which must not deep-match.
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		replaceInventory(t, tx, fx, nil,
			json.RawMessage(`{"foo":[["k"], {"value":"k"}]}`), nil)

		scope := fmt.Sprintf(`resourceType == %q && `, string(fx.InventoryType))
		assertResultNames(t, queryAll(t, tx, scope+`"k" in resource.observation.foo`))
		assertResultNames(t, queryAll(t, tx, scope+`"value" in resource.observation.foo`))
	})

	t.Run("InvalidContainerShapesAreSQLNull", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		wantName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		replaceInventory(t, tx, fx, nil,
			json.RawMessage(`{"scalar":"s","num":1,"flag":true,"n":null}`), nil)

		scope := fmt.Sprintf(`resourceType == %q && `, string(fx.InventoryType))
		for _, expr := range []string{
			`"k" in resource.observation.scalar`,
			`"k" in resource.observation.num`,
			`"k" in resource.observation.flag`,
			`"k" in resource.observation.n`,
			`"k" in resource.observation.absent`,
		} {
			assertResultNames(t, queryAll(t, tx, scope+expr))
			// Negation of unknown is also non-matching (scoped so other
			// resource types are not pulled in by !(false && null)).
			assertResultNames(t, queryAll(t, tx, scope+"!("+expr+")"))
		}

		// Soft-or still matches through the true sibling.
		assertResultNames(t, queryAll(t, tx,
			scope+`("k" in resource.observation.scalar) || resource.observation.num == 1`),
			wantName)
	})

	t.Run("KnownMapContainerKeyIn", func(t *testing.T) {
		tx, fx := newFixtureTx(t, factory)
		defer tx.Rollback()

		wantName := extensionEnvelopeName(fx.InventoryType, fx.InventoryName)
		assertResultNames(t, queryAll(t, tx, `"node-role" in resource.localLabels`), wantName)
		assertResultNames(t, queryAll(t, tx, `"Ready" in resource.conditions`), wantName)
		assertResultNames(t, queryAll(t, tx, `"missing" in resource.localLabels`))
	})
}
