package postgres_test

import (
	"os"
	"testing"

	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/postgres"
)

func TestMain(m *testing.M) {
	code := m.Run()
	postgres.TerminateTestContainer()
	os.Exit(code)
}
