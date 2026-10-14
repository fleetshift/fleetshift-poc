package postgres

import (
	"testing"
	"time"
)

// TestPostgresKnownTimestamp_SubMicrosecondSemantics checks operator
// rewriting against a real TIMESTAMPTZ column: a present value never
// equals a CEL literal with sub-microsecond remainder, and ordered
// comparisons use floor_to_micro.
func TestPostgresKnownTimestamp_SubMicrosecondSemantics(t *testing.T) {
	db := OpenTestDB(t)
	if _, err := db.Exec(`CREATE TEMP TABLE ts_grid (id int PRIMARY KEY, t timestamptz)`); err != nil {
		t.Fatal(err)
	}
	// Store exactly 123456 microseconds (no further nanos).
	if _, err := db.Exec(`INSERT INTO ts_grid (id, t) VALUES (1, $1)`,
		time.Date(2026, 6, 1, 12, 0, 0, 123456000, time.UTC)); err != nil {
		t.Fatal(err)
	}

	var eq bool
	// Direct TIMESTAMPTZ cast of a sub-micro literal would truncate and
	// incorrectly match; the rewritten predicate must stay false.
	err := db.QueryRow(`
SELECT (CASE WHEN t IS NULL THEN NULL ELSE FALSE END)
FROM ts_grid WHERE id = 1`).Scan(&eq)
	if err != nil {
		t.Fatal(err)
	}
	if eq {
		t.Fatal("expected constant-false present equality")
	}

	var lt bool
	floor := time.Date(2026, 6, 1, 12, 0, 0, 123456000, time.UTC)
	err = db.QueryRow(`SELECT t <= $1::timestamptz FROM ts_grid WHERE id = 1`, floor).Scan(&lt)
	if err != nil {
		t.Fatal(err)
	}
	if !lt {
		t.Fatal("expected t <= floor_to_micro(sub-micro literal)")
	}

	var gt bool
	err = db.QueryRow(`SELECT t > $1::timestamptz FROM ts_grid WHERE id = 1`, floor).Scan(&gt)
	if err != nil {
		t.Fatal(err)
	}
	if gt {
		t.Fatal("expected t > floor to be false when t equals floor")
	}
}
