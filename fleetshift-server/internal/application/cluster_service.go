package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ClusterAccessProviderRegistry resolves a ClusterAccessProvider by target type.
type ClusterAccessProviderRegistry interface {
	ClusterAccessProvider(targetType domain.TargetType) domain.ClusterAccessProvider
}

// ClusterService handles cluster connection info and credential minting
// for provisioned guest clusters.
type ClusterService struct {
	Targets   *TargetService
	Providers ClusterAccessProviderRegistry
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
func (s *ClusterService) GetCredential(ctx context.Context, resourceID, callerToken string) (*domain.ClusterCredential, error) {
	target, err := s.resolveTarget(ctx, resourceID)
	if err != nil {
		return nil, err
	}

	provider := s.Providers.ClusterAccessProvider(target.Type)
	if provider == nil {
		return nil, fmt.Errorf("cluster %q: no credential provider for target type %q", resourceID, target.Type)
	}

	cred, err := provider.MintCredential(ctx, callerToken, target)
	if err != nil {
		return nil, fmt.Errorf("credential exchange failed: %w", err)
	}

	return cred, nil
}
