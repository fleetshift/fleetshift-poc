package sqlite_test

import (
	"context"
	"testing"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/queryrepotest"
)

// TestQueryRepo exercises the full QueryRepository contract against
// the real SQLite implementation, including SemanticFilterMatrix
// (see queryrepotest.Run).
func TestQueryRepo(t *testing.T) {
	t.Parallel()
	queryrepotest.Run(t, func(t *testing.T) domain.Tx {
		store := beginTestTx(t)
		tx, err := store.Begin(context.Background())
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		t.Cleanup(func() { _ = tx.Rollback() })
		return tx
	})
}

// TestQueryRepo_StringLabelFilterReusesNumberedBinds covers the
// json_type/json_extract + questionParams interaction: the path key
// parameter is repeated, so bare "?" would mis-bind. Numbered ?N
// keeps the key bound once. Labels are ProtoJSON strings, so the
// filter compares as a string (numeric coercion is intentionally gone).
func TestQueryRepo_StringLabelFilterReusesNumberedBinds(t *testing.T) {
	t.Parallel()
	store := beginTestTx(t)
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()

	at := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	rt := domain.ResourceType("kind.fleetshift.io/Cluster")
	if err := tx.ExtensionResources().CreateType(context.Background(), domain.NewExtensionResourceType(
		rt, "v1", "clusters", at, domain.WithInventory(),
	)); err != nil {
		t.Fatalf("CreateType: %v", err)
	}
	for _, row := range []struct {
		id       string
		priority string
	}{
		{"high", "8"},
		{"low", "2"},
	} {
		if err := tx.ExtensionResources().Create(context.Background(), domain.NewExtensionResource(
			domain.NewExtensionResourceUID(), rt, domain.ResourceName("clusters/"+row.id), at,
			domain.WithExtensionLabels(map[string]string{"priority": row.priority}),
		)); err != nil {
			t.Fatalf("Create %s: %v", row.id, err)
		}
	}

	page, err := tx.Queries().QueryResources(context.Background(), domain.QueryResourcesRequest{
		Filter:   `resource.labels["priority"] == "8"`,
		PageSize: 50,
	})
	if err != nil {
		t.Fatalf("QueryResources: %v", err)
	}
	if len(page.Resources) != 1 {
		t.Fatalf("len(results) = %d, want 1 (only priority=\"8\")", len(page.Resources))
	}
	if got := string(page.Resources[0].ResourceID); got != "high" {
		t.Errorf("ResourceID = %q, want high", got)
	}
}
