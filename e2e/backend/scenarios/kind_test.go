//go:build e2e

package scenarios

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/steps"
)

func TestKindClusterLifecycle(t *testing.T) {
	name := steps.UniqueKindClusterID(t)
	t.Cleanup(func() {
		if !t.Failed() {
			return
		}
		steps.CleanupKindCluster(t, suite, name)
	})
	steps.RunStep(t, "create the cluster", func(t *testing.T) {
		t.Logf("kind cluster %s", name)
		steps.CreateKindCluster(t, suite, name)
	})
	steps.RunStep(t, "wait until the cluster is ready", func(t *testing.T) {
		steps.WaitForKindClusterReady(t, suite, name)
	})
	steps.RunStep(t, "wait until OIDC authentication works", func(t *testing.T) {
		steps.WaitForKindOIDC(t, suite, name)
	})
	steps.RunStep(t, "delete the cluster", func(t *testing.T) {
		steps.DeleteKindCluster(t, suite, name)
	})
	steps.RunStep(t, "wait until the cluster is gone", func(t *testing.T) {
		steps.WaitForKindClusterGone(t, suite, name)
	})
	steps.RunStep(t, "confirm the host cluster is gone", func(t *testing.T) {
		steps.AssertHostKindClusterGone(t, suite, name)
	})
}

func TestOIDCWriteToKindCluster(t *testing.T) {
	ns := steps.UniqueID(t, "oidc")
	var name string
	steps.RunStep(t, "ensure a shared kind cluster", func(t *testing.T) {
		name = steps.SharedKind(t, suite)
	})
	steps.RunStep(t, "create a namespace via OIDC", func(t *testing.T) {
		steps.CreateNamespaceViaOIDC(t, suite, name, ns)
	})
	steps.RunStep(t, "see the namespace via OIDC", func(t *testing.T) {
		steps.AssertNamespaceViaOIDC(t, suite, name, ns)
	})
}

func TestKindDeliveryRoundTrip(t *testing.T) {
	ns := steps.UniqueID(t, "e2e")
	nsID := steps.UniqueID(t, "ns")
	cmID := steps.UniqueID(t, "cm")
	t.Cleanup(func() {
		steps.CleanupDeployment(t, suite, cmID)
		steps.CleanupDeployment(t, suite, nsID)
	})
	var name string
	steps.RunStep(t, "ensure a shared kind cluster", func(t *testing.T) {
		name = steps.SharedKind(t, suite)
	})
	steps.RunStep(t, "deploy a namespace", func(t *testing.T) {
		steps.CreateNamespaceDeploymentOn(t, suite, nsID, ns, name)
	})
	steps.RunStep(t, "wait until the namespace is active", func(t *testing.T) {
		steps.WaitForDeploymentActive(t, suite, nsID)
	})
	steps.RunStep(t, "see the namespace on the cluster", func(t *testing.T) {
		steps.AssertNamespaceOnKindCluster(t, suite, name, ns)
	})
	steps.RunStep(t, "deploy a configmap", func(t *testing.T) {
		steps.CreateConfigMapDeploymentOn(t, suite, cmID, ns, name)
	})
	steps.RunStep(t, "wait until the configmap is active", func(t *testing.T) {
		steps.WaitForDeploymentActive(t, suite, cmID)
	})
	steps.RunStep(t, "see the configmap on the cluster", func(t *testing.T) {
		steps.AssertConfigMapInNamespaceOnKindCluster(t, suite, name, ns)
	})
	steps.RunStep(t, "find the configmap in the resource index", func(t *testing.T) {
		steps.WaitForIndexedConfigMap(t, suite, name, ns)
	})
	steps.RunStep(t, "delete the configmap deployment", func(t *testing.T) {
		steps.DeleteDeployment(t, suite, cmID)
	})
	steps.RunStep(t, "wait until the configmap deployment is gone", func(t *testing.T) {
		steps.WaitForDeploymentGone(t, suite, cmID)
	})
	steps.RunStep(t, "confirm the configmap is gone", func(t *testing.T) {
		steps.AssertConfigMapGoneOnKindCluster(t, suite, name, ns)
	})
	steps.RunStep(t, "delete the namespace deployment", func(t *testing.T) {
		steps.DeleteDeployment(t, suite, nsID)
	})
	steps.RunStep(t, "wait until the namespace deployment is gone", func(t *testing.T) {
		steps.WaitForDeploymentGone(t, suite, nsID)
	})
	steps.RunStep(t, "confirm the namespace is gone", func(t *testing.T) {
		steps.AssertNamespaceGoneOnKindCluster(t, suite, name, ns)
	})
}
