package domain

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// TargetRegistrar creates a target and its corresponding inventory
// item. Callers provide the repository instances, which may come from
// a transaction or be used directly. Transaction handling is external.
type TargetRegistrar struct {
	Targets   TargetRepository
	Inventory InventoryRepository
	Now       func() time.Time
}

func (r *TargetRegistrar) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Register validates and persists a target along with a corresponding
// inventory item (when an [InventoryRepository] is configured). The
// target's [InventoryItemID] is derived as "target:<TargetID>" by
// [NewTargetInfo].
func (r *TargetRegistrar) Register(ctx context.Context, target TargetInfo) error {
	if target.ID() == "" {
		return fmt.Errorf("%w: target ID is required", ErrInvalidArgument)
	}
	if target.Name() == "" {
		return fmt.Errorf("%w: target name is required", ErrInvalidArgument)
	}

	target = NewTargetInfo(
		target.ID(),
		target.Type(),
		target.Name(),
		target.State(),
		target.Labels(),
		target.Properties(),
		target.AcceptedManifestTypes(),
	)

	if r.Inventory != nil {
		props, _ := json.Marshal(target.Properties())
		now := r.now()
		if err := r.Inventory.Create(ctx, NewInventoryItem(
			target.InventoryItemID(),
			InventoryType(target.Type()),
			target.Name(),
			props,
			target.Labels(),
			nil,
			now,
		)); err != nil {
			return fmt.Errorf("create inventory item for target: %w", err)
		}
	}

	return r.Targets.Create(ctx, target)
}
