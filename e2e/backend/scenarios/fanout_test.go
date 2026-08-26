//go:build e2e

package scenarios

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/steps"
)

func TestFanOutToTwoClusters(t *testing.T) {
	ns := steps.UniqueID(t, "e2e")
	nsID := steps.UniqueID(t, "ns")
	cmID := steps.UniqueID(t, "cm")
	t.Cleanup(func() {
		steps.CleanupDeployment(t, suite, cmID)
		steps.CleanupDeployment(t, suite, nsID)
	})
	var a, b string
	steps.RunStep(t, "ensure a shared kind pair", func(t *testing.T) {
		a, b = steps.SharedKindPair(t, suite)
	})
	steps.RunStep(t, "deploy a namespace to both clusters", func(t *testing.T) {
		steps.CreateNamespaceDeploymentOn(t, suite, nsID, ns, a, b)
	})
	steps.RunStep(t, "wait until the namespace is active", func(t *testing.T) {
		steps.WaitForDeploymentActive(t, suite, nsID)
	})
	steps.RunStep(t, "see the namespace on the first cluster", func(t *testing.T) {
		steps.AssertNamespaceOnKindCluster(t, suite, a, ns)
	})
	steps.RunStep(t, "see the namespace on the second cluster", func(t *testing.T) {
		steps.AssertNamespaceOnKindCluster(t, suite, b, ns)
	})
	steps.RunStep(t, "deploy a configmap to both clusters", func(t *testing.T) {
		steps.CreateConfigMapDeploymentOn(t, suite, cmID, ns, a, b)
	})
	steps.RunStep(t, "wait until the configmap is active", func(t *testing.T) {
		steps.WaitForDeploymentActive(t, suite, cmID)
	})
	steps.RunStep(t, "see the configmap on the first cluster", func(t *testing.T) {
		steps.AssertConfigMapInNamespaceOnKindCluster(t, suite, a, ns)
	})
	steps.RunStep(t, "see the configmap on the second cluster", func(t *testing.T) {
		steps.AssertConfigMapInNamespaceOnKindCluster(t, suite, b, ns)
	})
	steps.RunStep(t, "delete the configmap deployment", func(t *testing.T) {
		steps.DeleteDeployment(t, suite, cmID)
	})
	steps.RunStep(t, "wait until the configmap deployment is gone", func(t *testing.T) {
		steps.WaitForDeploymentGone(t, suite, cmID)
	})
	steps.RunStep(t, "confirm the configmap is gone on the first cluster", func(t *testing.T) {
		steps.AssertConfigMapGoneOnKindCluster(t, suite, a, ns)
	})
	steps.RunStep(t, "confirm the configmap is gone on the second cluster", func(t *testing.T) {
		steps.AssertConfigMapGoneOnKindCluster(t, suite, b, ns)
	})
	steps.RunStep(t, "delete the namespace deployment", func(t *testing.T) {
		steps.DeleteDeployment(t, suite, nsID)
	})
	steps.RunStep(t, "wait until the namespace deployment is gone", func(t *testing.T) {
		steps.WaitForDeploymentGone(t, suite, nsID)
	})
	steps.RunStep(t, "confirm the namespace is gone on the first cluster", func(t *testing.T) {
		steps.AssertNamespaceGoneOnKindCluster(t, suite, a, ns)
	})
	steps.RunStep(t, "confirm the namespace is gone on the second cluster", func(t *testing.T) {
		steps.AssertNamespaceGoneOnKindCluster(t, suite, b, ns)
	})
}
