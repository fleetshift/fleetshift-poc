package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// AuthMethodService manages authentication methods at runtime.
// Create delegates to the [domain.ProvisionIdPWorkflow] which
// atomically persists the auth method and starts a trust-bundle
// deployment. Get and List read directly from the repository.
type AuthMethodService struct {
	Methods     domain.AuthMethodRepository
	ProvisionWF domain.ProvisionIdPWorkflow
}

// Create starts the ProvisionIdP workflow which resolves OIDC
// discovery, persists the auth method, and creates a trust-bundle
// deployment to distribute the IdP's trust configuration to agents.
func (s *AuthMethodService) Create(ctx context.Context, id domain.AuthMethodID, method domain.AuthMethod) (domain.AuthMethod, error) {
	if id == "" {
		return domain.AuthMethod{}, fmt.Errorf("%w: auth method ID is required", domain.ErrInvalidArgument)
	}

	exec, err := s.ProvisionWF.Start(ctx, domain.ProvisionIdPInput{
		AuthMethodID: id,
		AuthMethod:   method,
	})
	if err != nil {
		return domain.AuthMethod{}, fmt.Errorf("start provision-idp workflow: %w", err)
	}

	result, err := exec.AwaitResult(ctx)
	if err != nil {
		return domain.AuthMethod{}, fmt.Errorf("provision-idp workflow: %w", err)
	}
	return result, nil
}

// Get retrieves an auth method by ID.
func (s *AuthMethodService) Get(ctx context.Context, id domain.AuthMethodID) (domain.AuthMethod, error) {
	return s.Methods.Get(ctx, id)
}

// List returns all configured auth methods.
func (s *AuthMethodService) List(ctx context.Context) ([]domain.AuthMethod, error) {
	return s.Methods.List(ctx)
}
