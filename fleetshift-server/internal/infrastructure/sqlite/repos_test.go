package sqlite_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/deliveryrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/deploymentrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/inventoryrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/targetrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func TestTargetRepo(t *testing.T) {
	targetrepotest.Run(t, func(t *testing.T) domain.TargetRepository {
		db := sqlite.OpenTestDB(t)
		return &sqlite.TargetRepo{DB: db}
	})
}

func TestDeploymentRepo(t *testing.T) {
	deploymentrepotest.Run(t, func(t *testing.T) domain.DeploymentRepository {
		db := sqlite.OpenTestDB(t)
		return &sqlite.DeploymentRepo{DB: db}
	})
}

func TestDeliveryRepo(t *testing.T) {
	deliveryrepotest.Run(t, func(t *testing.T) domain.DeliveryRepository {
		db := sqlite.OpenTestDB(t)
		return &sqlite.DeliveryRepo{DB: db}
	})
}

func TestInventoryRepo(t *testing.T) {
	inventoryrepotest.Run(t, func(t *testing.T) domain.InventoryRepository {
		db := sqlite.OpenTestDB(t)
		return &sqlite.InventoryRepo{DB: db}
	})
}
