package sqlite_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/signerenrollmentrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func TestSignerEnrollmentRepo(t *testing.T) {
	signerenrollmentrepotest.Run(t, func(t *testing.T) domain.Store {
		return &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	})
}
