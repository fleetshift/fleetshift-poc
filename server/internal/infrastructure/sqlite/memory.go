package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// OpenMemory opens a named shared-cache in-memory SQLite database with
// migrations applied.
//
// MaxOpenConns is set to 2: one for the returned sentinel and one for work.
// The sentinel must stay open for the lifetime of the database; close it
// before or as part of shutting down db. Without a live sentinel, the
// shared-cache store can be destroyed if the pool drops to zero connections.
//
// The single work connection serializes BEGIN IMMEDIATE writers and avoids
// SQLITE_LOCKED under shared-cache concurrency (busy_timeout does not help
// with SQLITE_LOCKED).
func OpenMemory(name string) (*sql.DB, *sql.Conn, error) {
	return openSharedMemoryDSN(sharedMemoryDSN(name))
}

// sharedMemoryDSN returns a modernc.org/sqlite DSN for a named shared-cache
// in-memory database.
func sharedMemoryDSN(name string) string {
	return fmt.Sprintf("file:%s?mode=memory&cache=shared", name)
}

// isSharedMemoryDSN reports whether dsn selects shared-cache in-memory SQLite.
func isSharedMemoryDSN(dsn string) bool {
	return strings.Contains(dsn, "mode=memory") && strings.Contains(dsn, "cache=shared")
}

// openSharedMemoryDSN opens dsn as a shared-cache memory database and
// returns a held sentinel connection.
func openSharedMemoryDSN(dsn string) (*sql.DB, *sql.Conn, error) {
	if !isSharedMemoryDSN(dsn) {
		return nil, nil, fmt.Errorf("sqlite: not a shared-cache memory DSN: %q", dsn)
	}
	db, err := Open(dsn)
	if err != nil {
		return nil, nil, err
	}
	db.SetMaxOpenConns(2)
	sentinel, err := db.Conn(context.Background())
	if err != nil {
		_ = db.Close()
		return nil, nil, fmt.Errorf("open sentinel connection: %w", err)
	}
	return db, sentinel, nil
}
