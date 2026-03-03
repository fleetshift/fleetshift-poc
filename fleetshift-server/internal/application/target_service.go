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
	Targets   domain.TargetRepository
	Inventory domain.InventoryRepository
}

// Register creates a target and, when an [InventoryRepository] is
// configured, a corresponding inventory item. The target's
// [InventoryItemID] is set to "target:<TargetID>".
func (s *TargetService) Register(ctx context.Context, target domain.TargetInfo) error {
	if target.ID == "" {
		return fmt.Errorf("%w: target ID is required", domain.ErrInvalidArgument)
	}
	if target.Name == "" {
		return fmt.Errorf("%w: target name is required", domain.ErrInvalidArgument)
	}

	if s.Inventory != nil {
		invID := domain.InventoryItemID("target:" + string(target.ID))
		target.InventoryItemID = invID

		props, _ := json.Marshal(target.Properties)
		now := time.Now()
		if err := s.Inventory.Create(ctx, domain.InventoryItem{
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
	}

	return s.Targets.Create(ctx, target)
}

func (s *TargetService) Get(ctx context.Context, id domain.TargetID) (domain.TargetInfo, error) {
	return s.Targets.Get(ctx, id)
}

func (s *TargetService) List(ctx context.Context) ([]domain.TargetInfo, error) {
	return s.Targets.List(ctx)
}
