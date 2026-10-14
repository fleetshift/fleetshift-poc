package application

import (
	"context"
	"errors"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TargetService manages target registration and queries.
type TargetService struct {
	Store domain.Store
}

// Register creates a target and a corresponding inventory item
// atomically within a single transaction. Delegates to
// [domain.TargetRegistrar] for the core registration logic.
func (s *TargetService) Register(ctx context.Context, target domain.TargetInfo) error {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	reg := &domain.TargetRegistrar{
		Targets:   tx.Targets(),
		Inventory: tx.Inventory(),
	}
	if err := reg.Register(ctx, target); err != nil {
		return err
	}
	return tx.Commit()
}

// Get retrieves a target by ID.
func (s *TargetService) Get(ctx context.Context, id domain.TargetID) (domain.TargetInfo, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
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

// RegisterOrVerify atomically registers a target and its registrar-
// derived inventory item. If the target already exists, it verifies
// that the existing target's semantic fields match the new one and that
// the corresponding "target:<id>" inventory item also exists. Returns
// true when a new target was created, false when an existing target was
// verified as matching. Returns an error when:
//   - the existing target's fields drift from the new declaration,
//   - either the target or its inventory item is missing (broken pair),
//   - or an unexpected I/O error occurs.
func (s *TargetService) RegisterOrVerify(ctx context.Context, target domain.TargetInfo) (created bool, err error) {
	registerErr := s.Register(ctx, target)
	if registerErr == nil {
		return true, nil
	}
	if !errors.Is(registerErr, domain.ErrAlreadyExists) {
		return false, registerErr
	}

	// Normalize the expected target so InventoryItemID is derived.
	expected := domain.NewTargetInfo(
		target.ID(), target.Type(), target.Name(), target.State(),
		target.Labels(), target.Properties(), target.AcceptedManifestTypes(),
	)

	tx, err := s.Store.BeginReadOnly(ctx)
	if err != nil {
		return false, fmt.Errorf("begin read tx for target verification: %w", err)
	}
	defer tx.Rollback()

	existing, targetErr := tx.Targets().Get(ctx, target.ID())
	if targetErr != nil {
		if errors.Is(targetErr, domain.ErrNotFound) {
			return false, fmt.Errorf(
				"%w: target %q: registrar-derived inventory item exists but target row is missing",
				domain.ErrInvalidArgument, target.ID(),
			)
		}
		return false, fmt.Errorf("get existing target for verification: %w", targetErr)
	}

	_, invErr := tx.Inventory().Get(ctx, expected.InventoryItemID())
	if invErr != nil {
		if errors.Is(invErr, domain.ErrNotFound) {
			return false, fmt.Errorf(
				"%w: target %q: target row exists but registrar-derived inventory item %q is missing",
				domain.ErrInvalidArgument, target.ID(), expected.InventoryItemID(),
			)
		}
		return false, fmt.Errorf("get inventory item for target verification: %w", invErr)
	}

	if err := domain.VerifyTargetMatch(expected, existing); err != nil {
		return false, err
	}
	return false, tx.Commit()
}

// Deregister removes a target and its registrar-derived inventory item.
func (s *TargetService) Deregister(ctx context.Context, id domain.TargetID) error {
	tx, err := s.Store.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	if err := tx.Targets().Delete(ctx, id); err != nil {
		if !errors.Is(err, domain.ErrNotFound) {
			return fmt.Errorf("delete target %q: %w", id, err)
		}
	}
	invID := domain.InventoryItemID("target:" + string(id))
	if err := tx.Inventory().Delete(ctx, invID); err != nil {
		if !errors.Is(err, domain.ErrNotFound) {
			return fmt.Errorf("delete inventory item %q: %w", invID, err)
		}
	}
	return tx.Commit()
}

// List returns all registered targets.
func (s *TargetService) List(ctx context.Context) ([]domain.TargetInfo, error) {
	tx, err := s.Store.BeginReadOnly(ctx)
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
