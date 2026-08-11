package application

import (
	"context"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// AuthMethodService manages authentication methods at runtime.
// InstallFirst runs [domain.ProvisionIdPWorkflow] to persist the first auth
// method and start its trust-bundle deployment. Get and List read from the
// repository.
type AuthMethodService struct {
	Methods     domain.AuthMethodRepository
	ProvisionWF domain.ProvisionIdPWorkflow
}

// Create rejects all public AuthMethod creates. First AuthMethod install is
// [InstallFirst] only; after any method exists, public add/update/replace/delete
// remain disabled until a real IdP-update API exists.

// TODO: blocking this as a temporary measure, which blocks POST /v1/authMethods and
// gRPC CreateAuthMethod. Keeping this until we plan/implement IdP update / replace
// after Day One API
func (s *AuthMethodService) Create(_ context.Context, _ domain.AuthMethodID, _ domain.AuthMethod) (domain.AuthMethod, error) {
	return domain.AuthMethod{}, fmt.Errorf("public AuthMethod create is disabled; initial AuthMethod install is serve OIDC config only")
}

// InstallFirst runs ProvisionIdP for the initial AuthMethod. It is the sole
// insert path used by initial-AuthMethod install from serve OIDC config and is not
// exposed over the public API. The store must be empty.
func (s *AuthMethodService) InstallFirst(ctx context.Context, id domain.AuthMethodID, method domain.AuthMethod) (domain.AuthMethod, error) {
	if id == "" {
		return domain.AuthMethod{}, fmt.Errorf("%w: auth method ID is required", domain.ErrInvalidArgument)
	}

	existing, err := s.Methods.List(ctx)
	if err != nil {
		return domain.AuthMethod{}, fmt.Errorf("list auth methods: %w", err)
	}
	if len(existing) > 0 {
		return domain.AuthMethod{}, fmt.Errorf("AuthMethod store is not empty")
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
