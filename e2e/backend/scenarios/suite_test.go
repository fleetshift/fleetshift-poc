//go:build e2e

package scenarios

import (
	"fmt"
	"os"
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/steps"
)

// suite is the process-wide AIO fixture started in TestMain.
var suite *harness.Fixture

// TestMain starts AIO, logs in as ops, runs tests, releases the shared Kind
// pool on success while the AIO is up, then stops the fixture.
func TestMain(m *testing.M) {
	os.Exit(runSuite(m))
}

// runSuite owns fixture lifetime. failed starts true so a panic after Start dumps logs.
func runSuite(m *testing.M) int {
	f, err := harness.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "e2e/backend: start: %v\n", err)
		return 1
	}
	suite = f
	failed := true
	defer func() { f.Stop(failed) }()

	if err := f.Login(harness.PersonaOps); err != nil {
		fmt.Fprintf(os.Stderr, "e2e/backend: login: %v\n", err)
		return 1
	}

	code := m.Run()
	failed = code != 0
	// On success, delete the pool through fleetctl while the AIO is up.
	// That wait is after go test prints PASS and can take tens of seconds;
	// ReleaseSharedKindClusters logs so runners do not interrupt it. On
	// failure, leave clusters for Stop's kind evidence dump; leftover node
	// sweep still removes them.
	if !failed {
		steps.ReleaseSharedKindClusters(f)
	}
	return code
}
