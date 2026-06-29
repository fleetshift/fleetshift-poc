package postgres

import (
	"context"
	"errors"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestBeginUsesSerializableIsolation(t *testing.T) {
	db := OpenTestDB(t)
	store := &Store{DB: db}

	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback()

	stx := tx.(*storeTx)
	var level string
	if err := stx.tx.QueryRow("SHOW transaction_isolation").Scan(&level); err != nil {
		t.Fatalf("SHOW transaction_isolation: %v", err)
	}
	if level != "serializable" {
		t.Errorf("transaction_isolation = %q, want %q", level, "serializable")
	}
}

func TestBeginReadOnlyUsesSerializableIsolation(t *testing.T) {
	db := OpenTestDB(t)
	store := &Store{DB: db}

	tx, err := store.BeginReadOnly(context.Background())
	if err != nil {
		t.Fatalf("BeginReadOnly: %v", err)
	}
	defer tx.Rollback()

	stx := tx.(*storeTx)
	var level string
	if err := stx.tx.QueryRow("SHOW transaction_isolation").Scan(&level); err != nil {
		t.Fatalf("SHOW transaction_isolation: %v", err)
	}
	if level != "serializable" {
		t.Errorf("transaction_isolation = %q, want %q", level, "serializable")
	}
}

// TestCommitReturnsSerializationFailureOnWriteSkew provokes a
// classic write-skew anomaly that SSI is designed to detect.
//
// Two rows (x=1, y=1). Tx1 reads y then writes x; tx2 reads x
// then writes y. If both committed, the result (x=0, y=0) could
// not have been produced by any serial ordering — exactly the
// scenario SSI must reject. The first committer wins; the second
// must receive [domain.ErrSerializationFailure].
func TestCommitReturnsSerializationFailureOnWriteSkew(t *testing.T) {
	db := OpenTestDB(t)
	store := &Store{DB: db}
	ctx := context.Background()

	if _, err := db.ExecContext(ctx, `
		CREATE TABLE ssi_test (id TEXT PRIMARY KEY, val INT NOT NULL);
		INSERT INTO ssi_test VALUES ('x', 1), ('y', 1);
	`); err != nil {
		t.Fatalf("setup: %v", err)
	}

	tx1, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx1: %v", err)
	}
	defer tx1.Rollback()

	tx2, err := store.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx2: %v", err)
	}
	defer tx2.Rollback()

	raw1 := tx1.(*storeTx).tx
	raw2 := tx2.(*storeTx).tx

	// Tx1: read y, write x.
	var v int
	if err := raw1.QueryRowContext(ctx, "SELECT val FROM ssi_test WHERE id = 'y'").Scan(&v); err != nil {
		t.Fatalf("tx1 read y: %v", err)
	}
	if _, err := raw1.ExecContext(ctx, "UPDATE ssi_test SET val = $1 WHERE id = 'x'", v-1); err != nil {
		t.Fatalf("tx1 update x: %v", err)
	}

	// Tx2: read x, write y — the cross-dependency.
	if err := raw2.QueryRowContext(ctx, "SELECT val FROM ssi_test WHERE id = 'x'").Scan(&v); err != nil {
		t.Fatalf("tx2 read x: %v", err)
	}
	if _, err := raw2.ExecContext(ctx, "UPDATE ssi_test SET val = $1 WHERE id = 'y'", v-1); err != nil {
		t.Fatalf("tx2 update y: %v", err)
	}

	// First committer wins.
	if err := tx1.Commit(); err != nil {
		t.Fatalf("tx1 commit should succeed: %v", err)
	}

	// Second committer hits the SSI conflict.
	err = tx2.Commit()
	if !errors.Is(err, domain.ErrSerializationFailure) {
		t.Fatalf("tx2.Commit() = %v, want ErrSerializationFailure", err)
	}
}
