package sqlite

import (
	"context"
	"database/sql"
	"strings"
	"testing"
)

// OpenTestDB opens an in-memory SQLite database with the current schema
// applied via goose migrations. The database is closed when the test
// finishes.
//
// It delegates to [OpenMemory] for DSN, pool, and sentinel setup.
func OpenTestDB(t *testing.T) *sql.DB {
	t.Helper()
	return openTestMemory(t, t.Name())
}

// OpenTestDSN is like [OpenTestDB] but accepts an explicit DSN. Use this
// when a test needs multiple independent in-memory databases (e.g. by
// appending a sequence suffix to the DSN). Shared-cache memory DSNs are
// opened through [OpenMemory]; other DSNs keep the prior open path.
func OpenTestDSN(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	if isSharedMemoryDSN(dsn) {
		rest := strings.TrimPrefix(dsn, "file:")
		name := rest
		if i := strings.IndexByte(rest, '?'); i >= 0 {
			name = rest[:i]
		}
		return openTestMemory(t, name)
	}
	db, err := Open(dsn)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	db.SetMaxOpenConns(2)
	sentinel, err := db.Conn(context.Background())
	if err != nil {
		_ = db.Close()
		t.Fatalf("open sentinel connection: %v", err)
	}
	t.Cleanup(func() {
		_ = sentinel.Close()
		_ = db.Close()
	})
	return db
}

func openTestMemory(t *testing.T, name string) *sql.DB {
	t.Helper()
	db, sentinel, err := OpenMemory(name)
	if err != nil {
		t.Fatalf("open test db: %v", err)
	}
	t.Cleanup(func() {
		_ = sentinel.Close()
		_ = db.Close()
	})
	return db
}
