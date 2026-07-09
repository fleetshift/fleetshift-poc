package sqlite_test

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/queryrepotest"
)

// TestQueryRepo exercises the full QueryRepository contract against
// the real SQLite implementation -- see queryrepotest.Run's doc.
func TestQueryRepo(t *testing.T) {
	t.Parallel()
	queryrepotest.Run(t, func(t *testing.T) domain.Tx {
		store := beginTestTx(t)
		tx, err := store.Begin(context.Background())
		if err != nil {
			t.Fatalf("Begin: %v", err)
		}
		t.Cleanup(func() { _ = tx.Rollback() })
		return tx
	})
}
