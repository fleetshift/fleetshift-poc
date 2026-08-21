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
	steps.CreateKindCluster(t, suite, name)
	steps.WaitForKindClusterReady(t, suite, name)
	steps.WaitForKindOIDC(t, suite, name)
	steps.CreateConfigMapDeployment(t, suite, name)
	steps.WaitForDeploymentActive(t, suite, steps.ConfigMapDeploymentID(name))
	steps.AssertConfigMapOnKindCluster(t, suite, name)
	steps.DeleteKindCluster(t, suite, name)
	steps.WaitForKindClusterGone(t, suite, name)
	steps.AssertHostKindClusterGone(t, suite, name)
}
