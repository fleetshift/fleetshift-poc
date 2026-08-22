//go:build e2e

package scenarios

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/steps"
)

// TestOpsLoginAndCredentialIsolation asserts TestMain logged in as ops and that
// a command with an empty --config-dir cannot use the suite tokens.
func TestOpsLoginAndCredentialIsolation(t *testing.T) {
	steps.RunStep(t, "credentials exist", func(t *testing.T) {
		steps.LoginAsOps(t, suite)
	})
	steps.RunStep(t, "empty config dir is unauthenticated", func(t *testing.T) {
		steps.AssertCredentialsIsolated(t, suite)
	})
}
