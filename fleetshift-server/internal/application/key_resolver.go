package application

import (
	"context"
	"crypto"
	"fmt"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// KeyResolver resolves a user's signing public keys from an external
// registry. It loads the IdP configuration to find the registry
// reference and CEL claim mapping, then delegates to the appropriate
// [domain.RegistryClient].
type KeyResolver struct {
	Registries map[domain.KeyRegistryID]domain.KeyRegistry
	Clients    map[domain.KeyRegistryType]domain.RegistryClient
	// AuthMethods is used to resolve registry endpoints that depend on
	// runtime configuration (e.g. the Keycloak issuer URL).
	AuthMethods domain.AuthMethodRepository
}

// Resolve fetches the public keys for a registry subject.
func (r *KeyResolver) Resolve(ctx context.Context, registryID domain.KeyRegistryID, registrySubject domain.RegistrySubject) ([]crypto.PublicKey, error) {
	reg, ok := r.Registries[registryID]
	if !ok {
		return nil, fmt.Errorf("unknown key registry %q", registryID)
	}

	endpoint := reg.Endpoint
	if endpoint == "" && reg.Type == domain.KeyRegistryTypeKeycloak {
		ep, err := r.resolveKeycloakEndpoint(ctx)
		if err != nil {
			return nil, fmt.Errorf("resolve keycloak endpoint: %w", err)
		}
		endpoint = ep
	}

	client, ok := r.Clients[reg.Type]
	if !ok {
		return nil, fmt.Errorf("no client for registry type %q", reg.Type)
	}
	keys, err := client.FetchSigningKeys(ctx, endpoint, registrySubject)
	if err != nil {
		return nil, fmt.Errorf("fetch signing keys from %s for %q: %w", registryID, registrySubject, err)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no signing keys found for %q in registry %s", registrySubject, registryID)
	}
	return keys, nil
}

// resolveKeycloakEndpoint derives the Keycloak endpoint from the OIDC
// auth method's issuer URL stored in the database.
func (r *KeyResolver) resolveKeycloakEndpoint(ctx context.Context) (string, error) {
	if r.AuthMethods == nil {
		return "", fmt.Errorf("no auth method repository configured to resolve keycloak endpoint")
	}
	methods, err := r.AuthMethods.List(ctx)
	if err != nil {
		return "", err
	}
	for _, m := range methods {
		if m.Type == domain.AuthMethodTypeOIDC && m.OIDC != nil && m.OIDC.IssuerURL != "" {
			return string(m.OIDC.IssuerURL), nil
		}
	}
	return "", fmt.Errorf("no OIDC auth method with issuer URL found")
}
