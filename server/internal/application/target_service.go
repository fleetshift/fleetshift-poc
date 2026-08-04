package application

import (
	"context"
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
