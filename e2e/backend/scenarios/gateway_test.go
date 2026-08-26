//go:build e2e

package scenarios

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/steps"
)

func TestHTTPGateway(t *testing.T) {
	steps.RunStep(t, "serves the livez probe without a token", func(t *testing.T) {
		steps.AssertLivezOK(t, suite)
	})
	steps.RunStep(t, "serves the readyz probe without a token", func(t *testing.T) {
		steps.AssertReadyzOK(t, suite)
	})
	steps.RunStep(t, "rejects listing deployments without a token", func(t *testing.T) {
		steps.AssertDeploymentsUnauthorized(t, suite)
	})
	steps.RunStep(t, "rejects listing deployments with a bad token", func(t *testing.T) {
		steps.AssertDeploymentsUnauthorizedBadToken(t, suite)
	})
	steps.RunStep(t, "lists the bootstrap deployment when authenticated", func(t *testing.T) {
		steps.AssertDeploymentsIncludeBootstrap(t, suite, bootstrapDeploymentID)
	})
}
