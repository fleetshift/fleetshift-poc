package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
)

// OpenTestDB opens an in-memory SQLite database with all migrations applied.
// The database is closed when the test finishes. Shared-cache mode is used
// so that all connections from the pool share the same in-memory database,
// which is necessary when the database is accessed from multiple goroutines.
//
// A sentinel connection is held open for the lifetime of the test to prevent
// the shared-cache database from being destroyed if the pool momentarily
// drops to zero active connections (e.g. due to context cancellation in a
// background goroutine).
func OpenTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := Open(dsn)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	sentinel, err := db.Conn(context.Background())
	if err != nil {
		db.Close()
		t.Fatalf("open sentinel connection: %v", err)
	}
	t.Cleanup(func() {
		sentinel.Close()
		db.Close()
	})
	return db
}
