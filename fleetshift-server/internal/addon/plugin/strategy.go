package plugin

import (
	"context"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// CreateStrategy implements domain.AddonManifestStrategyFactory.
func (m *Manager) CreateStrategy(addonName string) domain.ManifestStrategy {
	return &AddonManifestStrategy{AddonName: addonName, Manager: m}
}

// AddonManifestStrategy generates manifests by calling a go-plugin addon subprocess.
type AddonManifestStrategy struct {
	AddonName string
	Manager   *Manager
}

func (s *AddonManifestStrategy) Generate(ctx context.Context, gctx domain.GenerateContext) ([]domain.Manifest, error) {
	return s.Manager.GenerateManifests(ctx, s.AddonName, gctx.Target.ID)
}

func (s *AddonManifestStrategy) OnRemoved(_ context.Context, _ domain.TargetID) error {
	return nil
}
