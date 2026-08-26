//go:build e2e

package scenarios

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/steps"
)

// TestKindResourceQuery asserts fleetctl resource query can find Kind and
// kubernetes inventory on both shared Kind clusters: indexed objects, query vs
// get, pagination, and dual Node types. The pair is required so cluster-scoped
// filters can fail if they leak the other cluster.
func TestKindResourceQuery(t *testing.T) {
	var a, b string
	steps.RunStep(t, "ensure a shared kind pair", func(t *testing.T) {
		a, b = steps.SharedKindPair(t, suite)
	})
	steps.RunStep(t, "find indexed kubernetes objects on the first cluster", func(t *testing.T) {
		steps.WaitForIndexedKubernetesObjects(t, suite, a)
	})
	steps.RunStep(t, "find indexed kubernetes objects on the second cluster", func(t *testing.T) {
		steps.WaitForIndexedKubernetesObjects(t, suite, b)
	})
	steps.RunStep(t, "kind cluster query matches get on the first cluster", func(t *testing.T) {
		steps.AssertKindClusterQueryMatchesGet(t, suite, a)
	})
	steps.RunStep(t, "object query paginates on the first cluster", func(t *testing.T) {
		steps.AssertKubernetesObjectQueryPaginates(t, suite, a)
	})
	steps.RunStep(t, "both node types are indexed on the first cluster", func(t *testing.T) {
		steps.WaitForDualIndexedNodes(t, suite, a)
	})
}
