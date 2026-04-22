package postgres_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/vaulttest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
)

func TestVault(t *testing.T) {
	vaulttest.Run(t, func(t *testing.T) domain.Vault {
		return &postgres.VaultStore{DB: postgres.OpenTestDB(t)}
	})
}
