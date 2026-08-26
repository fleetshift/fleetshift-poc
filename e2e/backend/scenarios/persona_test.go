//go:build e2e

package scenarios

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/steps"
)

func TestDeveloperTargetIsolationAndResume(t *testing.T) {
	ns := steps.UniqueID(t, "e2e")
	nsID := steps.UniqueID(t, "ns")
	cmID := steps.UniqueID(t, "cm")
	t.Cleanup(func() {
		steps.CleanupDeployment(t, suite, cmID)
		steps.CleanupDeployment(t, suite, nsID)
	})
	var devDir, name string
	steps.RunStep(t, "log in as the developer", func(t *testing.T) {
		devDir = steps.LoginAsDev(t, suite)
	})
	steps.RunStep(t, "token belongs to the developer", func(t *testing.T) {
		steps.AssertInspectTokenEmail(t, suite, devDir, steps.DevEmail)
	})
	steps.RunStep(t, "developer can list deployments", func(t *testing.T) {
		steps.AssertDeploymentListAs(t, suite, devDir)
	})
	steps.RunStep(t, "ops credentials stay isolated", func(t *testing.T) {
		steps.AssertCredentialsIsolated(t, suite)
	})
	steps.RunStep(t, "ensure a shared kind cluster", func(t *testing.T) {
		name = steps.SharedKind(t, suite)
	})
	steps.RunStep(t, "developer token is forbidden on the cluster", func(t *testing.T) {
		steps.AssertKindOIDCForbidden(t, suite, name, devDir)
	})
	steps.RunStep(t, "ops creates a namespace", func(t *testing.T) {
		steps.CreateNamespaceDeploymentOn(t, suite, nsID, ns, name)
	})
	steps.RunStep(t, "wait until the namespace is active", func(t *testing.T) {
		steps.WaitForDeploymentActive(t, suite, nsID)
	})
	steps.RunStep(t, "developer creates a configmap", func(t *testing.T) {
		steps.CreateConfigMapDeploymentAs(t, suite, devDir, cmID, ns, name)
	})
	steps.RunStep(t, "deployment pauses after delivery auth failure", func(t *testing.T) {
		steps.WaitForDeploymentPaused(t, suite, cmID)
	})
	steps.RunStep(t, "ops resumes the deployment", func(t *testing.T) {
		steps.ResumeDeployment(t, suite, cmID)
	})
	steps.RunStep(t, "wait until the deployment is active", func(t *testing.T) {
		steps.WaitForDeploymentResumedActive(t, suite, cmID)
	})
	steps.RunStep(t, "see the configmap on the cluster", func(t *testing.T) {
		steps.AssertConfigMapInNamespaceOnKindCluster(t, suite, name, ns)
	})
	steps.RunStep(t, "delete the deployment", func(t *testing.T) {
		steps.DeleteDeployment(t, suite, cmID)
	})
	steps.RunStep(t, "wait until the deployment is gone", func(t *testing.T) {
		steps.WaitForDeploymentGone(t, suite, cmID)
	})
	steps.RunStep(t, "confirm the configmap is gone", func(t *testing.T) {
		steps.AssertConfigMapGoneOnKindCluster(t, suite, name, ns)
	})
}
