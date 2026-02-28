package sqlite_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/deliveryrecordrepotest"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/deploymentrepotest"
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

func TestDeliveryRecordRepo(t *testing.T) {
	deliveryrecordrepotest.Run(t, func(t *testing.T) domain.DeliveryRecordRepository {
		db := sqlite.OpenTestDB(t)
		return &sqlite.DeliveryRecordRepo{DB: db}
	})
}
