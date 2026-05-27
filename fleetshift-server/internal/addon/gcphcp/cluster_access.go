package gcphcp

import (
	"context"
	"fmt"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ClusterAccess implements domain.ClusterAccessProvider for gcphcp targets.
type ClusterAccess struct {
	gateway     GatewayConfig
	stsEndpoint string
	iamEndpoint string
}

func NewClusterAccess(gateway GatewayConfig) *ClusterAccess {
	return &ClusterAccess{gateway: gateway}
}

func (ca *ClusterAccess) SetTestEndpoints(sts, iam string) {
	ca.stsEndpoint = sts
	ca.iamEndpoint = iam
}

func (ca *ClusterAccess) MintCredential(ctx context.Context, callerToken string, target domain.TargetInfo) (*domain.ClusterCredential, error) {
	tc := TargetConfigFromProperties(target.Properties)

	auth := NewBrokerAuth(BrokerAuthConfig{
		WorkforcePool:     tc.WorkforcePool,
		WorkforceProvider: tc.WorkforceProvider,
		GCPProject:        tc.GCPProject,
		BrokerSAEmail:     tc.BrokerSAEmail,
		GatewayAudience:   ca.gateway.Audience,
		STSEndpoint:       ca.stsEndpoint,
		IAMEndpoint:       ca.iamEndpoint,
	})

	result, err := auth.Exchange(ctx, callerToken)
	if err != nil {
		return nil, fmt.Errorf("broker auth exchange: %w", err)
	}

	return &domain.ClusterCredential{
		Token:      result.BrokerToken,
		Expiration: time.Now().Add(55 * time.Minute),
	}, nil
}
