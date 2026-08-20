//go:build e2e

package scenarios

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/steps"
)

// bootstrapDeploymentID is the AIO bootstrap IdP-trust deployment.
const bootstrapDeploymentID = "idp-trust-default"

// TestBootstrapDeploymentListed waits until the bootstrap deployment appears in list.
func TestBootstrapDeploymentListed(t *testing.T) {
	steps.WaitForListedDeployment(t, suite, bootstrapDeploymentID)
}

// TestBootstrapDeploymentActive waits until the bootstrap deployment is Active.
func TestBootstrapDeploymentActive(t *testing.T) {
	steps.WaitForDeploymentActive(t, suite, bootstrapDeploymentID)
}
