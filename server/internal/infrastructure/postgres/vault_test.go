package postgres_test

import (
	"testing"

	"github.com/stolostron/ocm-fleet/server/internal/domain"
	"github.com/stolostron/ocm-fleet/server/internal/domain/vaulttest"
	"github.com/stolostron/ocm-fleet/server/internal/infrastructure/postgres"
)

func TestVault(t *testing.T) {
	t.Parallel()
	vaulttest.Run(t, func(t *testing.T) domain.Vault {
		return &postgres.VaultStore{DB: postgres.OpenTestDB(t)}
	})
}
