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
	steps.WaitForClusterReady(t, suite, name)
	steps.WaitForKindOIDC(t, suite, name)
	steps.CreateConfigMapDeployment(t, suite, name)
	steps.WaitForDeploymentActive(t, suite, steps.ConfigMapDeploymentID(name))
	steps.AssertConfigMapOnCluster(t, suite, name)
	steps.DeleteKindCluster(t, suite, name)
	steps.WaitForClusterGone(t, suite, name)
	steps.AssertHostKindClusterGone(t, suite, name)
}
