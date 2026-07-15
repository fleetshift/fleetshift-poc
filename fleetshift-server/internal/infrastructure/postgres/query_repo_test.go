package postgres_test

import (
	"context"
	"testing"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain/queryrepotest"
)

func queryRepoTestTx(t *testing.T) domain.Tx {
	t.Helper()
	store := newStore(t)
	tx, err := store.Begin(context.Background())
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	t.Cleanup(func() { _ = tx.Rollback() })
	return tx
}

// TestQueryRepo exercises the full QueryRepository contract against
// the real Postgres implementation, including SemanticFilterMatrix
// (see queryrepotest.Run).
func TestQueryRepo(t *testing.T) {
	t.Parallel()
	queryrepotest.Run(t, queryRepoTestTx)
}
