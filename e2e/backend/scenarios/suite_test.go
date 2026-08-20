//go:build e2e

// Package scenarios contains backend E2E tests that share one AIO fixture.
package scenarios

import (
	"fmt"
	"os"
	"testing"

	"github.com/fleetshift/fleetshift-poc/e2e/backend/internal/harness"
)

// suite is the process-wide AIO fixture started in TestMain.
var suite *harness.Fixture

// TestMain starts AIO, logs in as ops, runs tests, then stops the fixture.
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

	if err := f.Login("ops"); err != nil {
		fmt.Fprintf(os.Stderr, "e2e/backend: login: %v\n", err)
		return 1
	}

	code := m.Run()
	failed = code != 0
	return code
}
