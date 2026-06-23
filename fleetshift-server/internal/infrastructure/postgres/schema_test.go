package postgres_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/postgres"
)

func TestResourceRepresentationsSchema_DoesNotExposeLegacyDeletedAt(t *testing.T) {
	t.Parallel()

	db := postgres.OpenTestDB(t)

	var exists bool
	if err := db.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM information_schema.columns
			WHERE table_schema = 'public'
			  AND table_name = 'resource_representations'
			  AND column_name = 'deleted_at'
		)`).
		Scan(&exists); err != nil {
		t.Fatalf("query information_schema.columns: %v", err)
	}
	if exists {
		t.Fatal("resource_representations.deleted_at should have been removed")
	}
}
