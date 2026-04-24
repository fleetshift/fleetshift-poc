package postgres_test

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/authmethodrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/deliveryrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/deploymentrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/inventoryrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/storetest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/targetrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
)

func newStore(t *testing.T) *postgres.Store {
	t.Helper()
	db := postgres.OpenTestDB(t)
	return &postgres.Store{DB: db}
}

func newTxRepo[T any](t *testing.T, accessor func(domain.Tx) T) T {
	t.Helper()
	store := newStore(t)
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	t.Cleanup(func() { tx.Rollback() })
	return accessor(tx)
}

func TestTargetRepo(t *testing.T) {
	t.Parallel()
	targetrepotest.Run(t, func(t *testing.T) domain.TargetRepository {
		return newTxRepo(t, domain.Tx.Targets)
	})
}

func TestDeploymentRepo(t *testing.T) {
	t.Parallel()
	deploymentrepotest.Run(t, func(t *testing.T) domain.DeploymentRepository {
		return newTxRepo(t, domain.Tx.Deployments)
	})
}

func TestDeliveryRepo(t *testing.T) {
	t.Parallel()
	deliveryrepotest.Run(t, func(t *testing.T) domain.DeliveryRepository {
		return newTxRepo(t, domain.Tx.Deliveries)
	})
}

func TestInventoryRepo(t *testing.T) {
	t.Parallel()
	inventoryrepotest.Run(t, func(t *testing.T) domain.InventoryRepository {
		return newTxRepo(t, domain.Tx.Inventory)
	})
}

func TestStore(t *testing.T) {
	t.Parallel()
	storetest.Run(t, func(t *testing.T) domain.Store {
		return newStore(t)
	})
}

func TestAuthMethodRepo(t *testing.T) {
	t.Parallel()
	authmethodrepotest.Run(t, func(t *testing.T) domain.AuthMethodRepository {
		db := postgres.OpenTestDB(t)
		return &postgres.AuthMethodRepo{DB: db}
	})
}
