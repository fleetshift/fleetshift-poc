package testenv_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/testenv"
)

func TestInventoryController_ReplaceLabels(t *testing.T) {
	store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
	ctx := context.Background()
	tx, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	def := domain.NewExtensionResourceType(testenv.HermeticInventoryType, "v1", "widgets", time.Now(), domain.WithInventory())
	if err := tx.ExtensionResources().CreateType(ctx, def); err != nil {
		t.Fatalf("Create type: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	svc := application.NewInventoryReportService(store)
	reporter := application.NewInventoryReporterAdapter(svc)
	ctrl := testenv.NewInventoryController(reporter, testenv.HermeticInventoryType)

	name := domain.ResourceName("widgets/w1")
	if err := ctrl.ReplaceLabels(ctx, name, map[string]string{"env": "e2e"}); err != nil {
		t.Fatalf("ReplaceLabels: %v", err)
	}

	tx, err = store.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin read: %v", err)
	}
	defer tx.Rollback()
	er, err := tx.ExtensionResources().Get(ctx, testenv.HermeticInventoryType.FullName(name))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if er.Inventory().Labels()["env"] != "e2e" {
		t.Fatalf("labels = %v, want env=e2e", er.Inventory().Labels())
	}
}

func TestInventoryController_ReplaceLabels_Validation(t *testing.T) {
	ctx := context.Background()

	t.Run("nilController", func(t *testing.T) {
		var ctrl *testenv.InventoryController
		err := ctrl.ReplaceLabels(ctx, "widgets/w1", map[string]string{"env": "e2e"})
		if err == nil {
			t.Fatal("expected error for nil controller")
		}
	})

	t.Run("nilReporter", func(t *testing.T) {
		ctrl := testenv.NewInventoryController(nil, testenv.HermeticInventoryType)
		err := ctrl.ReplaceLabels(ctx, "widgets/w1", map[string]string{"env": "e2e"})
		if err == nil {
			t.Fatal("expected error for nil reporter")
		}
	})

	t.Run("emptyName", func(t *testing.T) {
		store := &sqlite.Store{DB: sqlite.OpenTestDB(t)}
		svc := application.NewInventoryReportService(store)
		reporter := application.NewInventoryReporterAdapter(svc)
		ctrl := testenv.NewInventoryController(reporter, testenv.HermeticInventoryType)
		err := ctrl.ReplaceLabels(ctx, "", map[string]string{"env": "e2e"})
		if !errors.Is(err, domain.ErrInvalidArgument) {
			t.Fatalf("error = %v, want ErrInvalidArgument", err)
		}
	})
}
