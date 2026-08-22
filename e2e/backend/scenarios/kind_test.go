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
	steps.RunStep(t, "create", func(t *testing.T) {
		t.Logf("kind cluster %s", name)
		steps.CreateKindCluster(t, suite, name)
	})
	steps.RunStep(t, "wait ready", func(t *testing.T) {
		steps.WaitForKindClusterReady(t, suite, name)
	})
	steps.RunStep(t, "wait OIDC", func(t *testing.T) {
		steps.WaitForKindOIDC(t, suite, name)
	})
	steps.RunStep(t, "deploy configmap", func(t *testing.T) {
		steps.CreateConfigMapDeployment(t, suite, name)
	})
	steps.RunStep(t, "wait deployment active", func(t *testing.T) {
		steps.WaitForDeploymentActive(t, suite, steps.ConfigMapDeploymentID(name))
	})
	steps.RunStep(t, "check configmap", func(t *testing.T) {
		steps.AssertConfigMapOnKindCluster(t, suite, name)
	})
	steps.RunStep(t, "delete", func(t *testing.T) {
		steps.DeleteKindCluster(t, suite, name)
	})
	steps.RunStep(t, "wait gone", func(t *testing.T) {
		steps.WaitForKindClusterGone(t, suite, name)
	})
	steps.RunStep(t, "check host cluster gone", func(t *testing.T) {
		steps.AssertHostKindClusterGone(t, suite, name)
	})
}
