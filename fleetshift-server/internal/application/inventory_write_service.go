package application

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// InventoryWriteService implements [domain.InventoryWriter] as an
// application-layer service. It handles addon-to-platform inventory
// writes: upserts, deletes, and full resyncs.
type InventoryWriteService struct {
	store domain.Store
}

// NewInventoryWriteService creates an InventoryWriteService.
func NewInventoryWriteService(store domain.Store) *InventoryWriteService {
	return &InventoryWriteService{store: store}
}

// ApplyDelta upserts and deletes inventory items in a single transaction.
func (s *InventoryWriteService) ApplyDelta(ctx context.Context, targetID domain.TargetID, upserts []domain.InventoryItem, deletedIDs []domain.InventoryItemID, edgeAdds []domain.InventoryEdge, edgeDels []domain.InventoryEdge) error {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	itemRepo := tx.Inventory()
	edgeRepo := tx.Edges()

	for _, item := range upserts {
		if err := itemRepo.CreateOrUpdate(ctx, item); err != nil {
			return fmt.Errorf("upsert %s: %w", item.ID(), err)
		}
	}

	for _, id := range deletedIDs {
		if err := itemRepo.Delete(ctx, id); err != nil && !errors.Is(err, domain.ErrNotFound) {
			return fmt.Errorf("delete %s: %w", id, err)
		}
	}

	if len(edgeAdds) > 0 {
		if err := edgeRepo.UpsertEdges(ctx, targetID, edgeAdds); err != nil {
			return fmt.Errorf("upsert edges: %w", err)
		}
	}

	if len(edgeDels) > 0 {
		if err := edgeRepo.DeleteEdges(ctx, targetID, edgeDels); err != nil {
			return fmt.Errorf("delete edges: %w", err)
		}
	}

	return tx.Commit()
}

// Resync atomically replaces all items for a target+type.
func (s *InventoryWriteService) Resync(ctx context.Context, targetID domain.TargetID, inventoryType domain.InventoryType, items []domain.InventoryItem, edges []domain.InventoryEdge) error {
	tx, err := s.store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	itemRepo := tx.Inventory()
	edgeRepo := tx.Edges()

	if err := itemRepo.ReplaceByTargetAndType(ctx, targetID, inventoryType, items); err != nil {
		return fmt.Errorf("replace by target and type: %w", err)
	}

	// Scope edge replacement to the source UIDs of the resynced items.
	if len(items) > 0 {
		sourceUIDs := make([]string, 0, len(items))
		for _, item := range items {
			parts := strings.SplitN(string(item.ID()), "/", 2)
			if len(parts) == 2 {
				sourceUIDs = append(sourceUIDs, parts[1])
			}
		}
		if err := edgeRepo.DeleteEdgesBySourceUIDs(ctx, targetID, sourceUIDs); err != nil {
			return fmt.Errorf("delete edges for resync: %w", err)
		}
	}

	if len(edges) > 0 {
		if err := edgeRepo.UpsertEdges(ctx, targetID, edges); err != nil {
			return fmt.Errorf("upsert edges for resync: %w", err)
		}
	}

	return tx.Commit()
}
