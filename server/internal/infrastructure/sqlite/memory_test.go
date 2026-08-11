package sqlite_test

import (
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/infrastructure/sqlite"
)

func TestOpenMemory_RoundTripAndIsolation(t *testing.T) {
	db1, sentinel1, err := sqlite.OpenMemory(t.Name() + "-a")
	if err != nil {
		t.Fatalf("OpenMemory a: %v", err)
	}
	t.Cleanup(func() {
		_ = sentinel1.Close()
		_ = db1.Close()
	})

	if _, err := db1.Exec(`CREATE TABLE mem_probe (id INTEGER PRIMARY KEY, note TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db1.Exec(`INSERT INTO mem_probe (note) VALUES ('alpha')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Same name shares the in-memory store.
	db1b, sentinel1b, err := sqlite.OpenMemory(t.Name() + "-a")
	if err != nil {
		t.Fatalf("OpenMemory a again: %v", err)
	}
	t.Cleanup(func() {
		_ = sentinel1b.Close()
		_ = db1b.Close()
	})
	var note string
	if err := db1b.QueryRow(`SELECT note FROM mem_probe WHERE id = 1`).Scan(&note); err != nil {
		t.Fatalf("shared-name query: %v", err)
	}
	if note != "alpha" {
		t.Fatalf("shared-name note = %q, want alpha", note)
	}

	// Different name is an isolated database.
	db2, sentinel2, err := sqlite.OpenMemory(t.Name() + "-b")
	if err != nil {
		t.Fatalf("OpenMemory b: %v", err)
	}
	t.Cleanup(func() {
		_ = sentinel2.Close()
		_ = db2.Close()
	})
	if err := db2.QueryRow(`SELECT note FROM mem_probe WHERE id = 1`).Scan(&note); err == nil {
		t.Fatalf("isolated DB unexpectedly has mem_probe row: %q", note)
	}
}
