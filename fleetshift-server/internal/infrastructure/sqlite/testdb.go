package sqlite

import (
	"database/sql"
	"testing"
)

// OpenTestDB opens an in-memory SQLite database with all migrations applied.
// The database is closed when the test finishes.
func OpenTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}
