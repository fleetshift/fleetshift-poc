package sqlite

import (
	"database/sql"
	"embed"
	"fmt"
	"strings"
	"sync"

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
	db, err := openWithPragmas(dsn)
	if err != nil {
		return nil, err
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

// openWithPragmas opens a SQLite connection with standard pragmas
// but without running migrations. Used by [Open] for the full
// migration path and by [openWithSchema] for the fast snapshot path.
func openWithPragmas(dsn string) (*sql.DB, error) {
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

	return db, nil
}

var (
	schemaOnce sync.Once
	schemaDDL  []string
	schemaErr  error
)

// initSchema runs all goose migrations against a throwaway in-memory
// database and captures the resulting DDL from sqlite_master. This
// lets [openWithSchema] replay the final schema without goose
// overhead (version tracking, migration file parsing, dialect
// negotiation) on every call.
func initSchema() {
	db, err := Open(":memory:")
	if err != nil {
		schemaErr = fmt.Errorf("init schema template: %w", err)
		return
	}
	defer db.Close()

	rows, err := db.Query(
		"SELECT sql FROM sqlite_master " +
			"WHERE sql IS NOT NULL " +
			"AND name NOT LIKE 'sqlite_%' " +
			"AND name NOT LIKE 'goose_%' " +
			"ORDER BY rowid",
	)
	if err != nil {
		schemaErr = fmt.Errorf("read schema: %w", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var stmt string
		if err := rows.Scan(&stmt); err != nil {
			schemaErr = fmt.Errorf("scan schema row: %w", err)
			return
		}
		schemaDDL = append(schemaDDL, stmt)
	}
	schemaErr = rows.Err()
}

// openWithSchema opens a SQLite database and applies the schema
// snapshot captured by [initSchema], bypassing goose entirely.
// The snapshot is initialized once per process via [sync.Once].
func openWithSchema(dsn string) (*sql.DB, error) {
	schemaOnce.Do(initSchema)
	if schemaErr != nil {
		return nil, schemaErr
	}

	db, err := openWithPragmas(dsn)
	if err != nil {
		return nil, err
	}

	for _, stmt := range schemaDDL {
		if _, err := db.Exec(stmt); err != nil {
			db.Close()
			return nil, fmt.Errorf("apply schema: %w", err)
		}
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
