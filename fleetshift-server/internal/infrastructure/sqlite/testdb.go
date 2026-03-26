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
//
// MaxOpenConns is set to 2: one for the sentinel and one for actual work.
// This funnels all transactions through a single active connection,
// serializing writes at the pool level. Without this, concurrent
// BEGIN IMMEDIATE from different goroutines would hit SQLITE_LOCKED
// in shared-cache mode (the busy_timeout pragma does not help with
// SQLITE_LOCKED, only SQLITE_BUSY on file-backed databases).
func OpenTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := Open(dsn)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	db.SetMaxOpenConns(2)
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
