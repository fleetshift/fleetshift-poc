package application

import (
	"sync"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ClusterAccessRegistry manages the mapping from [domain.TargetType] to
// [domain.ClusterAccessProvider]. The addon manager registers and
// deregisters providers during addon connect/disconnect; the
// [ClusterService] reads them at request time. Neither consumer
// depends on the other.
type ClusterAccessRegistry struct {
	mu        sync.RWMutex
	providers map[domain.TargetType]domain.ClusterAccessProvider
}

// NewClusterAccessRegistry returns a ready-to-use registry with no
// providers registered.
func NewClusterAccessRegistry() *ClusterAccessRegistry {
	return &ClusterAccessRegistry{
		providers: make(map[domain.TargetType]domain.ClusterAccessProvider),
	}
}

// Register associates a [domain.ClusterAccessProvider] with a
// [domain.TargetType]. Calling Register for an already-registered type
// replaces the previous provider.
func (r *ClusterAccessRegistry) Register(targetType domain.TargetType, provider domain.ClusterAccessProvider) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.providers[targetType] = provider
}

// Deregister removes the [domain.ClusterAccessProvider] for a
// [domain.TargetType]. No-op if no provider is registered for the type.
func (r *ClusterAccessRegistry) Deregister(targetType domain.TargetType) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.providers, targetType)
}

// ClusterAccessProvider returns the registered provider for a target
// type, or nil if none is registered.
func (r *ClusterAccessRegistry) ClusterAccessProvider(targetType domain.TargetType) domain.ClusterAccessProvider {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.providers[targetType]
}

// Compile-time assertions that the concrete registry still satisfies the
// separate write-side and read-side ports used by AddonManager and ClusterService.
var _ ClusterAccessRegistrar = (*ClusterAccessRegistry)(nil)
var _ ClusterAccessResolver = (*ClusterAccessRegistry)(nil)
