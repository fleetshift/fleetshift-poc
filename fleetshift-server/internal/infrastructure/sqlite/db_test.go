package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestOpenWithPragmas_BusyTimeoutAppliedToEveryConnection(t *testing.T) {
	db, err := openWithPragmas(filepath.Join(t.TempDir(), "fleetshift.db"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(2)
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close database: %v", err)
		}
	})

	// Holding both connections at once forces database/sql to open two
	// distinct physical connections instead of reusing one from the pool.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	connections := make([]*sql.Conn, 0, 2)
	for i := 0; i < 2; i++ {
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatalf("open connection %d: %v", i+1, err)
		}
		connections = append(connections, conn)
		defer conn.Close()
	}

	for i, conn := range connections {
		var timeoutMS int
		if err := conn.QueryRowContext(ctx, "PRAGMA busy_timeout").Scan(&timeoutMS); err != nil {
			t.Fatalf("query connection %d busy timeout: %v", i+1, err)
		}
		if timeoutMS != 5000 {
			t.Errorf("connection %d busy timeout = %dms, want 5000ms", i+1, timeoutMS)
		}
	}
}

// TestOpen_CaseSensitiveLikeEnabled proves Open applies
// case_sensitive_like so ASCII LIKE matches Postgres semantics.
// modernc.org/sqlite does not return rows for
// `PRAGMA case_sensitive_like`, so we probe via LIKE itself.
func TestOpen_CaseSensitiveLikeEnabled(t *testing.T) {
	db := OpenTestDB(t)

	var matched int
	if err := db.QueryRow(`SELECT 'Creating' LIKE 'cre%'`).Scan(&matched); err != nil {
		t.Fatalf("LIKE probe: %v", err)
	}
	if matched != 0 {
		t.Fatalf("Creating LIKE cre%% = %d, want 0 (ASCII case-sensitive LIKE)", matched)
	}

	if err := db.QueryRow(`SELECT 'creating' LIKE 'cre%'`).Scan(&matched); err != nil {
		t.Fatalf("LIKE probe (same case): %v", err)
	}
	if matched != 1 {
		t.Fatalf("creating LIKE cre%% = %d, want 1", matched)
	}
}
