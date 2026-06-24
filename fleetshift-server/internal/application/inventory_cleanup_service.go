package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// InventoryCleanup deletes observed inventory data for a target.
// Used by [AddonManager] during addon Disable to remove stale
// inventory that no indexer will refresh.
type InventoryCleanup interface {
	DeleteByTarget(ctx context.Context, targetID domain.TargetID) error
}

// InventoryCleanupService implements [InventoryCleanup] by deleting
// edges and inventory items for a target in a single transaction.
type InventoryCleanupService struct {
	Store domain.Store
}

// DeleteByTarget deletes all edges and inventory items for the given
// target in a single transaction.
func (s *InventoryCleanupService) DeleteByTarget(ctx context.Context, targetID domain.TargetID) error {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx for inventory cleanup: %w", err)
	}
	defer tx.Rollback()

	if err := tx.Edges().DeleteByTarget(ctx, targetID); err != nil {
		return fmt.Errorf("delete edges for target %s: %w", targetID, err)
	}
	if err := tx.Inventory().DeleteByTarget(ctx, targetID); err != nil {
		return fmt.Errorf("delete inventory for target %s: %w", targetID, err)
	}
	return tx.Commit()
}
