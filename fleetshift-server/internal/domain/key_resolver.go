package domain

import (
	"context"
	"crypto"
	"fmt"
)

// KeyResolver resolves a user's signing public keys from an external
// registry. It loads the IdP configuration to find the registry
// reference and CEL claim mapping, then delegates to the appropriate
// [RegistryClient].
type KeyResolver struct {
	Registries map[KeyRegistryID]KeyRegistry
	Clients    map[KeyRegistryType]RegistryClient
}

// Resolve fetches the public keys for a registry subject.
func (r *KeyResolver) Resolve(ctx context.Context, registryID KeyRegistryID, registrySubject RegistrySubject) ([]crypto.PublicKey, error) {
	reg, ok := r.Registries[registryID]
	if !ok {
		return nil, fmt.Errorf("unknown key registry %q", registryID)
	}
	client, ok := r.Clients[reg.Type]
	if !ok {
		return nil, fmt.Errorf("no client for registry type %q", reg.Type)
	}
	keys, err := client.FetchSigningKeys(ctx, reg.Endpoint, registrySubject)
	if err != nil {
		return nil, fmt.Errorf("fetch signing keys from %s for %q: %w", registryID, registrySubject, err)
	}
	if len(keys) == 0 {
		return nil, fmt.Errorf("no signing keys found for %q in registry %s", registrySubject, registryID)
	}
	return keys, nil
}
