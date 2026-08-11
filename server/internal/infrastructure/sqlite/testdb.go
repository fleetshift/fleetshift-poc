package sqlite

import (
	"context"
	"database/sql"
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
// appending a sequence suffix to the DSN). Shared-cache memory DSNs keep
// their full query string (via [openSharedMemoryDSN]); other DSNs use
// the prior open path with the same pool/sentinel setup.
func OpenTestDSN(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	var (
		db       *sql.DB
		sentinel *sql.Conn
		err      error
	)
	if isSharedMemoryDSN(dsn) {
		db, sentinel, err = openSharedMemoryDSN(dsn)
	} else {
		db, err = Open(dsn)
		if err != nil {
			t.Fatalf("open test db: %v", err)
		}
		db.SetMaxOpenConns(2)
		sentinel, err = db.Conn(context.Background())
	}
	if err != nil {
		if db != nil {
			_ = db.Close()
		}
		t.Fatalf("open test db: %v", err)
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
