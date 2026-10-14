package postgres_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/signerenrollmentrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
)

func TestSignerEnrollmentRepo(t *testing.T) {
	t.Parallel()
	signerenrollmentrepotest.Run(t, func(t *testing.T) domain.Store {
		return &postgres.Store{DB: postgres.OpenTestDB(t)}
	})
}
