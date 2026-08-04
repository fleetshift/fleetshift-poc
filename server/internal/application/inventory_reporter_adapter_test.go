package application_test

import (
	"context"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestInventoryReporterAdapter_ApplyDeltaBatch(t *testing.T) {
	store := newStore(t)
	seedInventoryType(t, store)
	svc := application.NewInventoryReportService(store)
	reporter := application.NewInventoryReporterAdapter(svc)
	ctx := context.Background()

	name := domain.ResourceName("clusters/c1")
	if err := reporter.ApplyDeltaBatch(ctx, domain.InventoryDeltaBatch{
		Reports: []domain.InventoryDeltaReport{{
			ResourceType:  inventoryReportTestType,
			Name:          name,
			ReplaceLabels: map[string]string{"env": "prod"},
			ObservedAt:    time.Now(),
		}},
	}); err != nil {
		t.Fatalf("ApplyDeltaBatch: %v", err)
	}

	er := getExtensionResource(t, store, name)
	if er.Inventory().Labels()["env"] != "prod" {
		t.Errorf("Labels[env] = %q, want prod", er.Inventory().Labels()["env"])
	}
}
