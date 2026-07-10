package sqlite

import (
	"testing"
)

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
