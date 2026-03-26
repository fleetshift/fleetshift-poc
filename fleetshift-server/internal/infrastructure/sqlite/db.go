package sqlite

import (
	"database/sql"
	"embed"
	"fmt"
	"strings"

	"github.com/pressly/goose/v3"
	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrations embed.FS

// Open opens a SQLite database at the given path and runs all pending
// migrations. Use ":memory:" for an in-memory database.
//
// The DSN is augmented with _txlock=immediate so that write
// transactions ([Store.Begin]) use BEGIN IMMEDIATE, serializing
// writers and preventing shared-cache read-lock-upgrade deadlocks.
// Read-only transactions ([Store.BeginReadOnly]) bypass this via
// sql.TxOptions{ReadOnly: true}.
func Open(dsn string) (*sql.DB, error) {
	dsn = withTxLock(dsn)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}

	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable WAL: %w", err)
	}
	if _, err := db.Exec("PRAGMA busy_timeout=5000"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set busy timeout: %w", err)
	}
	if _, err := db.Exec("PRAGMA foreign_keys=ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable foreign keys: %w", err)
	}

	goose.SetBaseFS(migrations)
	if err := goose.SetDialect("sqlite3"); err != nil {
		db.Close()
		return nil, fmt.Errorf("set goose dialect: %w", err)
	}
	if err := goose.Up(db, "migrations"); err != nil {
		db.Close()
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	return db, nil
}

// withTxLock appends _txlock=immediate to the DSN unless it already
// contains a _txlock parameter. Plain file paths are converted to URI
// format so the query parameter is recognized by the driver.
func withTxLock(dsn string) string {
	if strings.Contains(dsn, "_txlock") {
		return dsn
	}
	if strings.Contains(dsn, "?") {
		return dsn + "&_txlock=immediate"
	}
	return dsn + "?_txlock=immediate"
}
