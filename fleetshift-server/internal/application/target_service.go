package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// TargetService manages target registration and queries.
type TargetService struct {
	Targets domain.TargetRepository
}

func (s *TargetService) Register(ctx context.Context, target domain.TargetInfo) error {
	if target.ID == "" {
		return fmt.Errorf("%w: target ID is required", domain.ErrInvalidArgument)
	}
	if target.Name == "" {
		return fmt.Errorf("%w: target name is required", domain.ErrInvalidArgument)
	}
	return s.Targets.Create(ctx, target)
}

func (s *TargetService) Get(ctx context.Context, id domain.TargetID) (domain.TargetInfo, error) {
	return s.Targets.Get(ctx, id)
}

func (s *TargetService) List(ctx context.Context) ([]domain.TargetInfo, error) {
	return s.Targets.List(ctx)
}
