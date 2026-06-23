package sqlite_test

import (
	"database/sql"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func TestResourceRepresentationsSchema_DoesNotExposeLegacyDeletedAt(t *testing.T) {
	db := sqlite.OpenTestDB(t)

	rows, err := db.Query(`PRAGMA table_info(resource_representations)`)
	if err != nil {
		t.Fatalf("PRAGMA table_info(resource_representations): %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			cid        int
			name       string
			columnType string
			notNull    int
			defaultVal sql.NullString
			pk         int
		)
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultVal, &pk); err != nil {
			t.Fatalf("scan table_info row: %v", err)
		}
		if name == "deleted_at" {
			t.Fatal("resource_representations.deleted_at should have been removed")
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table_info rows: %v", err)
	}
}
