package postgres_test

import (
	"os"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
)

func TestMain(m *testing.M) {
	code := m.Run()
	postgres.TerminateTestContainer()
	os.Exit(code)
}
