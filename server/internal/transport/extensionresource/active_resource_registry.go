package extensionresource

import (
	"context"
	"fmt"
	"maps"
	"sync"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// ActiveResourceType is one resource type's currently activated
// transport + query state. Today the prototype only allows a single
// API version per type (see [ActiveResourceRegistry.Register]);
// DefaultVersion is that version, and Versions has exactly one entry
// when the type is active.
type ActiveResourceType struct {
	ResourceType   domain.ResourceType
	DefaultVersion domain.APIVersion

	Versions map[domain.APIVersion]*ActiveResourceVersion
}

// ActiveResourceVersion holds the transport identity and query schema
// for one activated API version of a resource type.
type ActiveResourceVersion struct {
	APIVersion domain.APIVersion

	// Transport identity for this API version.
	GRPCServiceName string
	HTTPPrefix      string
	DescriptorPath  string

	// ContentHash is the activator's content fingerprint for this
	// registration. Used to skip recompilation when Activate is called
	// again with unchanged schema content.
	ContentHash [32]byte

	QuerySchema domain.ResourceQuerySchema

	// ExtensionServiceDescriptors are the activated dynamic extension
	// resource descriptors for this version. Used by QueryResources to
	// project ExtensionResourceView into the same body shape as typed
	// Get/List. Nil only for registry entries constructed in tests that
	// do not exercise body projection.
	ExtensionServiceDescriptors *ExtensionServiceDescriptors

	// Config is the resource type config used to activate this version.
	// Required for capability-aware body projection (labels / observed
	// state). Nil only for registry entries constructed in tests that
	// do not exercise body projection.
	Config *ResourceTypeConfig
}

// PlatformRegistrationKey identifies a shared platform-canonical
// registration. Platform services are keyed by collection only —
// platform API version is process config (see
// DynamicSchemaActivator.PlatformVersion), not per-activation state.
type PlatformRegistrationKey struct {
	Collection domain.CollectionName
}

// ActivePlatformRegistration holds transport identity for one
// platform-canonical collection service, plus the set of extension
// gRPC service names that currently own a reference to it.
// RefCount is derived as len(Owners).
type ActivePlatformRegistration struct {
	// APIVersion is the platform HTTP/gRPC version used when this
	// registration was first created (process config at acquire time).
	APIVersion domain.APIVersion

	GRPCServiceName string
	HTTPPrefix      string
	DescriptorPath  string

	// Owners are extension gRPC service names that hold a reference.
	// Empty after a full release (entry removed from the registry).
	Owners map[string]struct{}
}

// RefCount returns the number of extension owners.
func (p ActivePlatformRegistration) RefCount() int { return len(p.Owners) }

// HasOwner reports whether ownerGRPC is among the owners.
func (p ActivePlatformRegistration) HasOwner(ownerGRPC string) bool {
	_, ok := p.Owners[ownerGRPC]
	return ok
}

// ActiveResourceRegistry is the single in-memory store of activated
// extension and platform transport state. It lives in the transport
// layer because it owns gRPC/HTTP/descriptor bookkeeping; it also
// implements [domain.QuerySchemaProvider] so QueryRepository can
// validate resource.spec.* fields without depending on this package —
// wire the same instance as SchemaProvider (see cli/serve.go).
//
// Extension and platform registrations are related lifecycle state but
// distinct identities:
//
//   - Extensions: ResourceType + APIVersion → extension gRPC/HTTP/query
//   - Platforms: Collection → shared platform gRPC/HTTP, refcounted by
//     owning extension gRPC service names
//
// GRPCServiceName ownership is unique across both indexes.
//
// Per this repo's testing conventions (prefer a real, no-I/O
// implementation over a mock), this same type also serves as its own
// test double for activator tests.
type ActiveResourceRegistry struct {
	mu sync.RWMutex

	extensionsByType map[domain.ResourceType]*ActiveResourceType
	// extensionsByGRPC indexes the active version under its gRPC
	// service name so the activator can look up / tear down by
	// SchemaActivationID without a second map of its own.
	extensionsByGRPC map[string]extensionIndex

	platformsByKey map[PlatformRegistrationKey]*ActivePlatformRegistration
	// platformsByOwner maps extension gRPC service name → platform key
	// for O(1) lookup during Deactivate / rename reconciliation.
	platformsByOwner map[string]PlatformRegistrationKey
	// platformGRPC indexes platform gRPC names for cross-index
	// uniqueness checks against extension registrations.
	platformGRPC map[string]PlatformRegistrationKey
}

type extensionIndex struct {
	resourceType domain.ResourceType
	apiVersion   domain.APIVersion
}

var _ domain.QuerySchemaProvider = (*ActiveResourceRegistry)(nil)

// NewActiveResourceRegistry returns an empty registry.
func NewActiveResourceRegistry() *ActiveResourceRegistry {
	return &ActiveResourceRegistry{
		extensionsByType: make(map[domain.ResourceType]*ActiveResourceType),
		extensionsByGRPC: make(map[string]extensionIndex),
		platformsByKey:   make(map[PlatformRegistrationKey]*ActivePlatformRegistration),
		platformsByOwner: make(map[string]PlatformRegistrationKey),
		platformGRPC:     make(map[string]PlatformRegistrationKey),
	}
}

// Register records (or replaces) one API version of a resource type.
// Replacing the same APIVersion updates transport identity and query
// schema in place. Registering a different APIVersion while another is
// already present returns [domain.ErrInvalidArgument] — multi-version
// activation is not supported in this prototype.
//
// A GRPCServiceName already owned by a different (ResourceType,
// APIVersion) or by a platform registration is also rejected with
// [domain.ErrInvalidArgument].
//
// ver.QuerySchema.ResourceType and ver.APIVersion must be set; the
// registry uses them as keys. QuerySchema.APIVersion is overwritten
// from ver.APIVersion so the two stay aligned.
func (r *ActiveResourceRegistry) Register(ver ActiveResourceVersion) error {
	rt := ver.QuerySchema.ResourceType
	if rt == "" {
		return fmt.Errorf("%w: ActiveResourceVersion.QuerySchema.ResourceType is required", domain.ErrInvalidArgument)
	}
	if ver.APIVersion == "" {
		return fmt.Errorf("%w: ActiveResourceVersion.APIVersion is required", domain.ErrInvalidArgument)
	}
	if ver.GRPCServiceName == "" {
		return fmt.Errorf("%w: ActiveResourceVersion.GRPCServiceName is required", domain.ErrInvalidArgument)
	}

	ver.QuerySchema.APIVersion = ver.APIVersion
	// Copy so callers cannot mutate the stored version through the
	// pointer they passed (Versions stores *ActiveResourceVersion).
	stored := ver

	r.mu.Lock()
	defer r.mu.Unlock()

	allow := extensionIndex{resourceType: rt, apiVersion: ver.APIVersion}
	if err := r.checkGRPCNameAvailable(ver.GRPCServiceName, &allow, false); err != nil {
		return err
	}

	active, ok := r.extensionsByType[rt]
	if !ok {
		active = &ActiveResourceType{
			ResourceType:   rt,
			DefaultVersion: ver.APIVersion,
			Versions:       make(map[domain.APIVersion]*ActiveResourceVersion),
		}
		r.extensionsByType[rt] = active
	} else if _, exists := active.Versions[ver.APIVersion]; !exists {
		// Prototype limitation: only one API version per resource type.
		existing := active.DefaultVersion
		return fmt.Errorf("%w: resource type %q already has API version %q registered; multiple versions are not supported",
			domain.ErrInvalidArgument, rt, existing)
	}

	if prev, exists := active.Versions[ver.APIVersion]; exists {
		if prev.GRPCServiceName != ver.GRPCServiceName {
			delete(r.extensionsByGRPC, prev.GRPCServiceName)
		}
	}

	active.Versions[ver.APIVersion] = &stored
	active.DefaultVersion = ver.APIVersion
	r.extensionsByGRPC[ver.GRPCServiceName] = extensionIndex{resourceType: rt, apiVersion: ver.APIVersion}
	return nil
}

// UnregisterByGRPCServiceName removes the extension version indexed
// under grpcServiceName, if any. If that was the type's only version,
// the type entry is removed entirely. Platform ownership for this
// gRPC name is not cleared here — callers must
// [RemovePlatformOwner] separately (Activate/Deactivate do both).
func (r *ActiveResourceRegistry) UnregisterByGRPCServiceName(grpcServiceName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	idx, ok := r.extensionsByGRPC[grpcServiceName]
	if !ok {
		return
	}
	delete(r.extensionsByGRPC, grpcServiceName)

	active, ok := r.extensionsByType[idx.resourceType]
	if !ok {
		return
	}
	delete(active.Versions, idx.apiVersion)
	if len(active.Versions) == 0 {
		delete(r.extensionsByType, idx.resourceType)
		return
	}
	// Pick an arbitrary remaining version as default. Unreachable
	// today (Register rejects a second version), but keeps the map
	// consistent if that restriction is lifted later.
	for v := range active.Versions {
		active.DefaultVersion = v
		break
	}
}

// AddPlatformOwner records that ownerGRPC holds a reference to the
// platform registration for key. On the first owner (0→1), transport
// must be fully populated (GRPCServiceName required); subsequent
// acquires ignore transport and only add the owner.
//
// created is true when this call established the platform entry
// (activator should register the platform mux service). Idempotent
// re-add of an existing owner returns created=false with no error.
func (r *ActiveResourceRegistry) AddPlatformOwner(key PlatformRegistrationKey, ownerGRPC string, transport ActivePlatformRegistration) (created bool, err error) {
	if key.Collection == "" {
		return false, fmt.Errorf("%w: PlatformRegistrationKey.Collection is required", domain.ErrInvalidArgument)
	}
	if ownerGRPC == "" {
		return false, fmt.Errorf("%w: platform owner gRPC service name is required", domain.ErrInvalidArgument)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existingKey, ok := r.platformsByOwner[ownerGRPC]; ok {
		if existingKey == key {
			return false, nil // already an owner of this platform
		}
		return false, fmt.Errorf("%w: extension %q already owns platform collection %q",
			domain.ErrInvalidArgument, ownerGRPC, existingKey.Collection)
	}

	if plat, ok := r.platformsByKey[key]; ok {
		plat.Owners[ownerGRPC] = struct{}{}
		r.platformsByOwner[ownerGRPC] = key
		return false, nil
	}

	if transport.GRPCServiceName == "" {
		return false, fmt.Errorf("%w: ActivePlatformRegistration.GRPCServiceName is required on first acquire", domain.ErrInvalidArgument)
	}
	if err := r.checkGRPCNameAvailable(transport.GRPCServiceName, nil, true); err != nil {
		return false, err
	}

	stored := &ActivePlatformRegistration{
		APIVersion:      transport.APIVersion,
		GRPCServiceName: transport.GRPCServiceName,
		HTTPPrefix:      transport.HTTPPrefix,
		DescriptorPath:  transport.DescriptorPath,
		Owners:          map[string]struct{}{ownerGRPC: {}},
	}
	r.platformsByKey[key] = stored
	r.platformsByOwner[ownerGRPC] = key
	r.platformGRPC[transport.GRPCServiceName] = key
	return true, nil
}

// RemovePlatformOwner drops ownerGRPC's reference to key. If that was
// the last owner, the platform entry is removed and last is true; the
// returned registration carries the transport identity the activator
// should deregister from the muxes. No-op if the owner or key is absent.
func (r *ActiveResourceRegistry) RemovePlatformOwner(key PlatformRegistrationKey, ownerGRPC string) (dropped ActivePlatformRegistration, last bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	plat, ok := r.platformsByKey[key]
	if !ok {
		return ActivePlatformRegistration{}, false
	}
	if _, ok := plat.Owners[ownerGRPC]; !ok {
		return ActivePlatformRegistration{}, false
	}
	delete(plat.Owners, ownerGRPC)
	delete(r.platformsByOwner, ownerGRPC)

	if len(plat.Owners) > 0 {
		return ActivePlatformRegistration{}, false
	}

	delete(r.platformsByKey, key)
	delete(r.platformGRPC, plat.GRPCServiceName)
	out := *plat
	out.Owners = nil
	return out, true
}

// GetPlatform returns the platform registration for key, if any.
func (r *ActiveResourceRegistry) GetPlatform(key PlatformRegistrationKey) (ActivePlatformRegistration, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	plat, ok := r.platformsByKey[key]
	if !ok {
		return ActivePlatformRegistration{}, false
	}
	return cloneActivePlatformRegistration(plat), true
}

// PlatformKeyForOwner returns the platform key owned by ownerGRPC, if any.
func (r *ActiveResourceRegistry) PlatformKeyForOwner(ownerGRPC string) (PlatformRegistrationKey, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	key, ok := r.platformsByOwner[ownerGRPC]
	return key, ok
}

// Contains reports whether rt already has apiVersion registered
// with the same gRPC service name and content hash. Activate uses
// this for the pre-compile idempotent skip.
func (r *ActiveResourceRegistry) Contains(rt domain.ResourceType, apiVersion domain.APIVersion, grpcServiceName string, contentHash [32]byte) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	active, ok := r.extensionsByType[rt]
	if !ok {
		return false
	}
	ver, ok := active.Versions[apiVersion]
	if !ok {
		return false
	}
	return ver.GRPCServiceName == grpcServiceName && ver.ContentHash == contentHash
}

// Get returns the active type entry, if any.
func (r *ActiveResourceRegistry) Get(rt domain.ResourceType) (ActiveResourceType, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	active, ok := r.extensionsByType[rt]
	if !ok {
		return ActiveResourceType{}, false
	}
	return cloneActiveResourceType(active), true
}

// GetVersion returns the active registration for rt at apiVersion, if any.
func (r *ActiveResourceRegistry) GetVersion(rt domain.ResourceType, apiVersion domain.APIVersion) (ActiveResourceVersion, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	active, ok := r.extensionsByType[rt]
	if !ok {
		return ActiveResourceVersion{}, false
	}
	ver, ok := active.Versions[apiVersion]
	if !ok {
		return ActiveResourceVersion{}, false
	}
	return *ver, true
}

// GetByGRPCServiceName returns the resource type and version registered
// under grpcServiceName, if any.
func (r *ActiveResourceRegistry) GetByGRPCServiceName(grpcServiceName string) (domain.ResourceType, ActiveResourceVersion, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	idx, ok := r.extensionsByGRPC[grpcServiceName]
	if !ok {
		return "", ActiveResourceVersion{}, false
	}
	active := r.extensionsByType[idx.resourceType]
	ver := active.Versions[idx.apiVersion]
	return idx.resourceType, *ver, true
}

// GetResourceQuerySchema implements [domain.QuerySchemaProvider].
// Returns the DefaultVersion's query schema (versionless lookup).
func (r *ActiveResourceRegistry) GetResourceQuerySchema(_ context.Context, rt domain.ResourceType) (domain.ResourceQuerySchema, bool, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	active, ok := r.extensionsByType[rt]
	if !ok {
		return domain.ResourceQuerySchema{}, false, nil
	}
	ver, ok := active.Versions[active.DefaultVersion]
	if !ok {
		return domain.ResourceQuerySchema{}, false, nil
	}
	return ver.QuerySchema, true, nil
}

// ListResourceQuerySchemas implements [domain.QuerySchemaProvider].
// Returns one schema per active resource type (its DefaultVersion).
func (r *ActiveResourceRegistry) ListResourceQuerySchemas(_ context.Context) ([]domain.ResourceQuerySchema, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]domain.ResourceQuerySchema, 0, len(r.extensionsByType))
	for _, active := range r.extensionsByType {
		if ver, ok := active.Versions[active.DefaultVersion]; ok {
			out = append(out, ver.QuerySchema)
		}
	}
	return out, nil
}

// checkGRPCNameAvailable ensures grpcName is not owned by a conflicting
// extension or platform registration. Caller must hold r.mu.
//
// When allowExtension is non-nil, an existing extensionsByGRPC entry for
// that exact (resourceType, apiVersion) is allowed (Register
// replace-in-place). Pass nil when claiming a platform name.
// asPlatform selects the ErrInvalidArgument context string for each
// conflict so Register and AddPlatformOwner keep their existing wording.
func (r *ActiveResourceRegistry) checkGRPCNameAvailable(grpcName string, allowExtension *extensionIndex, asPlatform bool) error {
	if idx, ok := r.extensionsByGRPC[grpcName]; ok {
		sameExtension := allowExtension != nil &&
			idx.resourceType == allowExtension.resourceType &&
			idx.apiVersion == allowExtension.apiVersion
		if !sameExtension {
			if asPlatform {
				return fmt.Errorf("%w: gRPC service name %q is already registered as an extension service",
					domain.ErrInvalidArgument, grpcName)
			}
			return fmt.Errorf("%w: gRPC service name %q is already registered for resource type %q version %q",
				domain.ErrInvalidArgument, grpcName, idx.resourceType, idx.apiVersion)
		}
	}
	if other, ok := r.platformGRPC[grpcName]; ok {
		if asPlatform {
			return fmt.Errorf("%w: gRPC service name %q is already registered for platform collection %q",
				domain.ErrInvalidArgument, grpcName, other.Collection)
		}
		return fmt.Errorf("%w: gRPC service name %q is already registered as a platform service",
			domain.ErrInvalidArgument, grpcName)
	}
	return nil
}

func cloneActiveResourceType(src *ActiveResourceType) ActiveResourceType {
	out := ActiveResourceType{
		ResourceType:   src.ResourceType,
		DefaultVersion: src.DefaultVersion,
		Versions:       make(map[domain.APIVersion]*ActiveResourceVersion, len(src.Versions)),
	}
	for k, v := range src.Versions {
		cp := *v
		out.Versions[k] = &cp
	}
	return out
}

func cloneActivePlatformRegistration(src *ActivePlatformRegistration) ActivePlatformRegistration {
	out := *src
	out.Owners = maps.Clone(src.Owners)
	return out
}
