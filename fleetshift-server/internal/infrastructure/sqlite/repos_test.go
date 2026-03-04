package sqlite_test

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/deliveryrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/deploymentrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/inventoryrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/storetest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/targetrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func beginTestTx(t *testing.T) *sqlite.Store {
	t.Helper()
	db := sqlite.OpenTestDB(t)
	return &sqlite.Store{DB: db}
}

func TestTargetRepo(t *testing.T) {
	targetrepotest.Run(t, func(t *testing.T) domain.TargetRepository {
		store := beginTestTx(t)
		tx, err := store.Begin(context.Background())
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		t.Cleanup(func() { tx.Rollback() })
		return tx.Targets()
	})
}

func TestDeploymentRepo(t *testing.T) {
	deploymentrepotest.Run(t, func(t *testing.T) domain.DeploymentRepository {
		store := beginTestTx(t)
		tx, err := store.Begin(context.Background())
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		t.Cleanup(func() { tx.Rollback() })
		return tx.Deployments()
	})
}

func TestDeliveryRepo(t *testing.T) {
	deliveryrepotest.Run(t, func(t *testing.T) domain.DeliveryRepository {
		store := beginTestTx(t)
		tx, err := store.Begin(context.Background())
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		t.Cleanup(func() { tx.Rollback() })
		return tx.Deliveries()
	})
}

func TestInventoryRepo(t *testing.T) {
	inventoryrepotest.Run(t, func(t *testing.T) domain.InventoryRepository {
		store := beginTestTx(t)
		tx, err := store.Begin(context.Background())
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		t.Cleanup(func() { tx.Rollback() })
		return tx.Inventory()
	})
}

func TestStore(t *testing.T) {
	storetest.Run(t, func(t *testing.T) domain.Store {
		db := sqlite.OpenTestDB(t)
		return &sqlite.Store{DB: db}
	})
}
