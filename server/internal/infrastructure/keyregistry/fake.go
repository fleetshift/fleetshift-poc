package keyregistry

import (
	"context"
	"crypto"
	"fmt"
	"sync"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// Fake is an in-memory [domain.RegistryClient] for tests. Keys are
// indexed by "{endpoint}/{subject}" so a single Fake can serve
// multiple registries.
type Fake struct {
	mu   sync.RWMutex
	keys map[string][]crypto.PublicKey
}

// NewFake returns a ready-to-use Fake.
func NewFake() *Fake {
	return &Fake{keys: make(map[string][]crypto.PublicKey)}
}

// Register adds public keys for a subject at a given endpoint.
func (f *Fake) Register(endpoint string, subject domain.RegistrySubject, keys ...crypto.PublicKey) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := endpoint + "/" + string(subject)
	f.keys[k] = append(f.keys[k], keys...)
}

func (f *Fake) FetchSigningKeys(_ context.Context, endpoint string, subject domain.RegistrySubject) ([]crypto.PublicKey, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	k := endpoint + "/" + string(subject)
	keys, ok := f.keys[k]
	if !ok || len(keys) == 0 {
		return nil, fmt.Errorf("no keys registered for %s at %s", subject, endpoint)
	}
	out := make([]crypto.PublicKey, len(keys))
	copy(out, keys)
	return out, nil
}
