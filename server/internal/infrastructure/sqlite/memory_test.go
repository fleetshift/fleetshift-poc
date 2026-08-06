package sqlite_test

import (
	"path/filepath"
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

func TestDumpToFile_RoundTrip(t *testing.T) {
	db := sqlite.OpenTestDB(t)

	if _, err := db.Exec(`CREATE TABLE dump_probe (id INTEGER PRIMARY KEY, note TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO dump_probe (note) VALUES ('hello')`); err != nil {
		t.Fatalf("insert: %v", err)
	}

	path := filepath.Join(t.TempDir(), "snapshot.db")
	if err := sqlite.DumpToFile(db, path); err != nil {
		t.Fatalf("DumpToFile: %v", err)
	}

	restored, err := sqlite.Open(path)
	if err != nil {
		t.Fatalf("open dump: %v", err)
	}
	t.Cleanup(func() { _ = restored.Close() })

	var note string
	if err := restored.QueryRow(`SELECT note FROM dump_probe WHERE id = 1`).Scan(&note); err != nil {
		t.Fatalf("query dump: %v", err)
	}
	if note != "hello" {
		t.Fatalf("note = %q, want hello", note)
	}
}

func TestDumpToFile_Validation(t *testing.T) {
	db := sqlite.OpenTestDB(t)
	path := filepath.Join(t.TempDir(), "exists.db")

	t.Run("nil database", func(t *testing.T) {
		if err := sqlite.DumpToFile(nil, path); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("empty path", func(t *testing.T) {
		if err := sqlite.DumpToFile(db, ""); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("whitespace path", func(t *testing.T) {
		if err := sqlite.DumpToFile(db, "   "); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("existing path", func(t *testing.T) {
		if err := sqlite.DumpToFile(db, path); err != nil {
			t.Fatalf("first DumpToFile: %v", err)
		}
		if err := sqlite.DumpToFile(db, path); err == nil {
			t.Fatal("expected error dumping onto existing path")
		}
	})
}
