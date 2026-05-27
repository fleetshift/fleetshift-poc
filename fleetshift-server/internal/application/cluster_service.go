package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ClusterService handles cluster connection info and credential minting
// for provisioned guest clusters.
type ClusterService struct {
	Targets   *TargetService
	Providers *ClusterAccessRegistry
}

func (s *ClusterService) resolveTarget(ctx context.Context, resourceID string) (domain.TargetInfo, error) {
	targetID := domain.TargetID("k8s-" + resourceID)
	target, err := s.Targets.Get(ctx, targetID)
	if err != nil {
		return domain.TargetInfo{}, fmt.Errorf("cluster %q: %w", resourceID, domain.ErrNotFound)
	}
	return target, nil
}

// GetConnectionInfo returns the API endpoint and CA cert for a cluster.
func (s *ClusterService) GetConnectionInfo(ctx context.Context, resourceID string) (endpoint, caCert string, err error) {
	target, err := s.resolveTarget(ctx, resourceID)
	if err != nil {
		return "", "", err
	}

	endpoint = target.Properties["api_server"]
	if endpoint == "" {
		return "", "", fmt.Errorf("cluster %q: no api_server in target properties: %w", resourceID, domain.ErrNotFound)
	}

	return endpoint, target.Properties["ca_cert"], nil
}

// GetCredential mints a short-lived credential for accessing a cluster.
// It follows the provenance chain from the emitted kubernetes target back
// to the seeded target that provisioned it, then dispatches to the
// credential provider registered for that seeded target's type.
func (s *ClusterService) GetCredential(ctx context.Context, resourceID, callerToken string) (*domain.ClusterCredential, error) {
	emittedTarget, err := s.resolveTarget(ctx, resourceID)
	if err != nil {
		return nil, err
	}

	if emittedTarget.ProvisioningTargetID == "" {
		return nil, fmt.Errorf("cluster %q: no provisioning target linked", resourceID)
	}

	seededTarget, err := s.Targets.Get(ctx, emittedTarget.ProvisioningTargetID)
	if err != nil {
		return nil, fmt.Errorf("cluster %q: provisioning target %q not found: %w", resourceID, emittedTarget.ProvisioningTargetID, err)
	}

	provider := s.Providers.ClusterAccessProvider(seededTarget.Type)
	if provider == nil {
		return nil, fmt.Errorf("cluster %q: no credential provider for target type %q", resourceID, seededTarget.Type)
	}

	return provider.MintCredential(ctx, callerToken, seededTarget)
}
