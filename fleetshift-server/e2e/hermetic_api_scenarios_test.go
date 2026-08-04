// Package e2e holds Go end-to-end scenario tests for FleetShift.
//
// Public API product scenarios should read as a linear sequence of domain
// verbs on [apiScenario]. Environment plumbing, auth, clients, and polling
// live on that fixture — not in the scenario body.
package e2e

import (
	"testing"
)

func TestHermeticAPI_GatedProgressInventoryDelete(t *testing.T) {
	s := newAPIScenario(t, "ops-user")

	s.GateDelivery()
	s.ExpectCreating(s.CreateDeployment("gated-1"))
	s.WaitUntilCreatingAndReconciling("gated-1")
	s.ReportDeliveryProgress("applying manifests")
	s.WaitUntilProgressObserved("applying manifests")
	s.ReleaseDelivery()
	s.WaitUntilActive("gated-1")

	s.LabelInventory("widgets/w1", map[string]string{"env": "e2e"})
	s.WaitUntilQueryFinds(
		`resourceType == "hermetic.fleetshift.io/Widget" && resource.localLabels["env"] == "e2e"`,
		"widgets/w1",
	)

	s.DeleteDeployment("gated-1")
	s.WaitUntilGone("gated-1")
}

func TestHermeticAPI_TransientDeliveryFailureRetry(t *testing.T) {
	s := newAPIScenario(t, "retry-user")

	s.InjectTransientDeliveryFailure(1)
	s.CreateDeployment("retry-1")
	s.WaitUntilActive("retry-1")
	s.ExpectDeliverAttemptsAtLeast(2)
}
