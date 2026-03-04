package application

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TargetService manages target registration and queries.
type TargetService struct {
	Store domain.Store
}

// Register creates a target and a corresponding inventory item
// atomically within a single transaction. The target's
// [domain.InventoryItemID] is set to "target:<TargetID>".
func (s *TargetService) Register(ctx context.Context, target domain.TargetInfo) error {
	if target.ID == "" {
		return fmt.Errorf("%w: target ID is required", domain.ErrInvalidArgument)
	}
	if target.Name == "" {
		return fmt.Errorf("%w: target name is required", domain.ErrInvalidArgument)
	}

	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	invID := domain.InventoryItemID("target:" + string(target.ID))
	target.InventoryItemID = invID

	props, _ := json.Marshal(target.Properties)
	now := time.Now()
	if err := tx.Inventory().Create(ctx, domain.InventoryItem{
		ID:         invID,
		Type:       domain.InventoryType(target.Type),
		Name:       target.Name,
		Properties: props,
		Labels:     target.Labels,
		CreatedAt:  now,
		UpdatedAt:  now,
	}); err != nil {
		return fmt.Errorf("create inventory item for target: %w", err)
	}

	if err := tx.Targets().Create(ctx, target); err != nil {
		return err
	}
	return tx.Commit()
}

// Get retrieves a target by ID.
func (s *TargetService) Get(ctx context.Context, id domain.TargetID) (domain.TargetInfo, error) {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return domain.TargetInfo{}, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	t, err := tx.Targets().Get(ctx, id)
	if err != nil {
		return domain.TargetInfo{}, err
	}
	return t, tx.Commit()
}

// List returns all registered targets.
func (s *TargetService) List(ctx context.Context) ([]domain.TargetInfo, error) {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	targets, err := tx.Targets().List(ctx)
	if err != nil {
		return nil, err
	}
	return targets, tx.Commit()
}
