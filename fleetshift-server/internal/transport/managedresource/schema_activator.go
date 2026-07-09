package managedresource

import (
	"context"
	"crypto/sha256"
	"fmt"
	"slices"
	"strings"
	"sync"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/platformresource"
)

// DynamicSchemaActivator implements [application.SchemaActivator] by
// compiling proto from inline sources, building a dynamic gRPC service,
// and registering it in the [dynamicapi.DynamicServiceMux],
// [dynamicapi.DynamicHTTPMux], and [dynamicapi.DynamicFileRegistry]
// (for gRPC reflection).
//
// Activation state — extension transport identity, content hash, query
// schema, and platform ownership/refcount — lives in
// [ActiveResourceRegistry]. The activator owns mux handles and performs
// mux mutations when the registry reports 0→1 / last-owner transitions.
//
// It keeps a content hash per service in the registry so that repeated
// Activate calls with unchanged schemas skip recompilation. When the
// schema content changes, the mux entry is atomically replaced — no
// deregister/register gap. When the gRPC service name changes, the old
// registration stays live until the new one is fully registered, then
// the old transport is removed.
//
// # Platform API version
//
// Platform-canonical services use the activator-selected platform API
// version (defaulting to [platformresource.APIVersion]). Extension
// [domain.ExtensionResourceSchema.Version] applies only to the
// extension's own transport surface; it does not control the platform
// route or gRPC identity for the shared collection. Platform
// registrations are keyed by collection only (see
// [PlatformRegistrationKey]).
type DynamicSchemaActivator struct {
	GRPCMux      *dynamicapi.DynamicServiceMux
	HTTPMux      *dynamicapi.DynamicHTTPMux
	FileRegistry *dynamicapi.DynamicFileRegistry
	Deps         Deps
	PlatformDeps platformresource.Deps
	// PlatformVersion optionally overrides the platform HTTP API version
	// used for platform-canonical registrations. If empty,
	// [platformresource.APIVersion] is used.
	PlatformVersion string

	// Registry is the shared activation + query-schema store. Required:
	// Activate records transport identity, content hash, query schema,
	// and platform ownership here so QueryRepository can validate
	// resource.spec.* fields (see [domain.QuerySchemaProvider]). Wire
	// the same instance into the store's SchemaProvider (see cli/serve.go).
	Registry *ActiveResourceRegistry

	mu sync.Mutex
}

var _ application.SchemaActivator = (*DynamicSchemaActivator)(nil)

// Activate compiles the schema's inline proto, builds a dynamic gRPC
// service, and registers it in the mux. If the schema is already active
// with identical content, the existing registration ID is returned
// without recompilation. If the content has changed, the mux entry is
// atomically replaced.
func (a *DynamicSchemaActivator) Activate(ctx context.Context, schema domain.ExtensionResourceSchema) (application.SchemaActivationID, error) {
	if a.Registry == nil {
		return "", fmt.Errorf("DynamicSchemaActivator.Registry is required")
	}
	if schema.Management == nil {
		return "", fmt.Errorf("schema for %q has no management section; cannot activate transport", schema.ResourceType)
	}
	if len(schema.ProtoFiles) == 0 {
		return "", fmt.Errorf("schema for %q has no proto files", schema.ResourceType)
	}

	mgmt := schema.Management

	// Compute registration identity and content hash before expensive
	// compilation so we can short-circuit when the schema is unchanged.
	serviceName := schema.ProtoPackage + "." + schema.Singular + "Service"
	pkgPath := strings.ReplaceAll(schema.ProtoPackage, ".", "/")
	lower := strings.ToLower(schema.Singular[:1]) + schema.Singular[1:]
	descriptorPath := fmt.Sprintf("dynamic/%s/%s_service.proto", pkgPath, lower)
	canonicalPrefix := "/apis/" + string(schema.ResourceType.ServiceName()) + "/" + schema.Version + "/" + schema.CollectionID
	platformKey := platformKeyForCollection(schema.CollectionID)
	apiVersion := domain.APIVersion(schema.Version)

	hash := schemaContentHash(schema)
	id := application.SchemaActivationID(serviceName)

	// Cheap pre-compile skip when nothing material has changed.
	// Full classification (replace / rename / multi-version) happens
	// after compilation under the second lock.
	a.mu.Lock()
	unchanged := a.Registry.Contains(schema.ResourceType, apiVersion, serviceName, hash)
	a.mu.Unlock()
	if unchanged {
		return id, nil
	}

	entryFile, err := resolveEntryFile(schema)
	if err != nil {
		return "", err
	}

	specDesc, err := dynamicapi.CompileInline(
		ctx,
		schema.ProtoFiles,
		entryFile,
		protoreflect.FullName(mgmt.SpecMessage),
	)
	if err != nil {
		return "", fmt.Errorf("compile proto: %w", err)
	}

	cfg := &ResourceTypeConfig{
		CollectionConfig: dynamicapi.CollectionConfig{
			Version:      schema.Version,
			CollectionID: schema.CollectionID,
			Singular:     schema.Singular,
			Plural:       schema.Plural,
		},
		ResourceType:   schema.ResourceType,
		ProtoPackage:   schema.ProtoPackage,
		SpecMessage:    mgmt.SpecMessage,
		SpecDescriptor: specDesc.Message,
	}

	svc, err := Build(cfg, a.Deps)
	if err != nil {
		return "", fmt.Errorf("build service: %w", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Re-check after compilation in case a concurrent Activate completed.
	// Walk type → version → content once.
	var existing ActiveResourceVersion
	var replaceInPlace, grpcRename bool
	if active, hasType := a.Registry.Get(schema.ResourceType); hasType {
		ver, hasVersion := active.Versions[apiVersion]
		if !hasVersion {
			return "", fmt.Errorf("%w: resource type %q already has API version %q registered; multiple versions are not supported",
				domain.ErrInvalidArgument, schema.ResourceType, active.DefaultVersion)
		}
		if ver.GRPCServiceName == serviceName && ver.ContentHash == hash {
			return id, nil
		}
		existing = *ver
		replaceInPlace = ver.GRPCServiceName == serviceName
		grpcRename = !replaceInPlace
	}

	sameHTTPPrefix := false
	if replaceInPlace {
		// Reconcile platform ownership before mutating the live
		// extension mux. If acquire fails we must not tear down the
		// still-serving registration (ReplaceDesc / HTTP / file).
		if platformKey.Collection != "" {
			prevKey, hadPrev := a.Registry.PlatformKeyForOwner(serviceName)
			needAcquire := !hadPrev || prevKey != platformKey

			if hadPrev && prevKey != platformKey {
				a.releasePlatformOwner(prevKey, serviceName)
			}

			if needAcquire {
				if err := a.acquirePlatform(schema, platformKey, serviceName); err != nil {
					// Live extension transport was not mutated; only
					// new platform transport is rolled back inside
					// acquirePlatform.
					return "", fmt.Errorf("register platform service: %w", err)
				}
			}
		}

		a.GRPCMux.ReplaceDesc(svc.Desc)
		if a.HTTPMux != nil {
			handler := BuildHTTPHandler(svc, a.HTTPMux.Conn(), canonicalPrefix)
			a.HTTPMux.ReplacePrefixHandler(canonicalPrefix, handler)
			if existing.HTTPPrefix != "" && existing.HTTPPrefix != canonicalPrefix {
				a.HTTPMux.DeregisterByPrefix(existing.HTTPPrefix)
			}
		}
		if a.FileRegistry != nil {
			if existing.DescriptorPath != "" && existing.DescriptorPath != descriptorPath {
				a.FileRegistry.Deregister(existing.DescriptorPath)
			}
			a.FileRegistry.Replace(svc.Descriptors.File)
		}
	} else {
		// New gRPC name (first activation, or package rename). Register
		// the new service first; on rename the old service stays until
		// after Registry.Register succeeds.
		//
		// When the canonical HTTP prefix is unchanged (typical package
		// rename), RegisterPrefixHandler would fail on the already-
		// registered route. Defer the in-place HTTP replace until after
		// Registry.Register so a failed later step does not leave the
		// shared prefix pointing at a rolled-back gRPC service, and so
		// rollback does not DeregisterByPrefix the still-serving route.
		sameHTTPPrefix = grpcRename && existing.HTTPPrefix != "" && existing.HTTPPrefix == canonicalPrefix

		if err := a.GRPCMux.RegisterDesc(svc.Desc); err != nil {
			return "", fmt.Errorf("register gRPC: %w", err)
		}
		if a.HTTPMux != nil && !sameHTTPPrefix {
			handler := BuildHTTPHandler(svc, a.HTTPMux.Conn(), canonicalPrefix)
			if err := a.HTTPMux.RegisterPrefixHandler(canonicalPrefix, handler); err != nil {
				a.GRPCMux.Deregister(serviceName)
				return "", fmt.Errorf("register HTTP: %w", err)
			}
		}
		if a.FileRegistry != nil {
			if err := a.FileRegistry.Register(svc.Descriptors.File); err != nil {
				a.rollbackNewExtensionTransport(serviceName, canonicalPrefix, descriptorPath, sameHTTPPrefix)
				return "", fmt.Errorf("register file descriptor: %w", err)
			}
		}

		// Platform ownership — reconcile against the *new* gRPC name.
		// On rename, the old name's platform ownership is released after
		// the registry swap below so a failed new registration does not
		// drop the still-live old service's platform ref.
		if platformKey.Collection != "" {
			prevKey, hadPrev := a.Registry.PlatformKeyForOwner(serviceName)
			needAcquire := !hadPrev || prevKey != platformKey

			if hadPrev && prevKey != platformKey {
				a.releasePlatformOwner(prevKey, serviceName)
			}

			if needAcquire {
				if err := a.acquirePlatform(schema, platformKey, serviceName); err != nil {
					// Roll back only the *new* transport. On rename the
					// old registration is still intact.
					a.rollbackNewExtensionTransport(serviceName, canonicalPrefix, descriptorPath, sameHTTPPrefix)
					return "", fmt.Errorf("register platform service: %w", err)
				}
			}
		}
	}

	if err := a.Registry.Register(ActiveResourceVersion{
		APIVersion:         apiVersion,
		GRPCServiceName:    serviceName,
		HTTPPrefix:         canonicalPrefix,
		DescriptorPath:     descriptorPath,
		ContentHash:        hash,
		ServiceDescriptors: svc.Descriptors,
		QuerySchema: domain.ResourceQuerySchema{
			ResourceType:   schema.ResourceType,
			ServiceName:    schema.ResourceType.ServiceName(),
			TypeName:       schema.Singular,
			APIVersion:     apiVersion,
			CollectionName: domain.CollectionName(schema.CollectionID),
			SpecDescriptor: specDesc.Message,
		},
	}); err != nil {
		if !replaceInPlace {
			a.rollbackNewExtensionTransport(serviceName, canonicalPrefix, descriptorPath, sameHTTPPrefix)
			if platformKey.Collection != "" {
				a.releasePlatformOwner(platformKey, serviceName)
			}
		}
		return "", err
	}

	if sameHTTPPrefix {
		// Registry swap succeeded; point the shared prefix at the new
		// service, then retire the old gRPC/file identity without
		// touching the HTTP route.
		if a.HTTPMux != nil {
			handler := BuildHTTPHandler(svc, a.HTTPMux.Conn(), canonicalPrefix)
			a.HTTPMux.ReplacePrefixHandler(canonicalPrefix, handler)
		}
		a.GRPCMux.Deregister(existing.GRPCServiceName)
		if a.FileRegistry != nil && existing.DescriptorPath != "" && existing.DescriptorPath != descriptorPath {
			a.FileRegistry.Deregister(existing.DescriptorPath)
		}
	} else if grpcRename {
		// New registration is live; retire the previous identity
		// (distinct HTTP prefix / descriptor path).
		a.deregisterExtensionTransport(existing.GRPCServiceName, existing.HTTPPrefix, existing.DescriptorPath)
	}
	if grpcRename {
		if prevOldKey, ok := a.Registry.PlatformKeyForOwner(existing.GRPCServiceName); ok {
			a.releasePlatformOwner(prevOldKey, existing.GRPCServiceName)
		}
		// Registry.Register already dropped the old gRPC name from
		// extensionsByGRPC when the same type+version moved to the
		// new name.
	}

	return id, nil
}

// deregisterExtensionTransport removes gRPC/HTTP/file registrations for
// one extension service. Must be called with a.mu held.
func (a *DynamicSchemaActivator) deregisterExtensionTransport(grpcServiceName, httpPrefix, descriptorPath string) {
	a.GRPCMux.Deregister(grpcServiceName)
	if a.HTTPMux != nil {
		a.HTTPMux.DeregisterByPrefix(httpPrefix)
	}
	if a.FileRegistry != nil {
		a.FileRegistry.Deregister(descriptorPath)
	}
}

// rollbackNewExtensionTransport undoes a partially registered *new*
// extension transport. When skipHTTP is true the HTTP prefix is shared
// with the still-live previous registration and must not be removed.
func (a *DynamicSchemaActivator) rollbackNewExtensionTransport(grpcServiceName, httpPrefix, descriptorPath string, skipHTTP bool) {
	a.GRPCMux.Deregister(grpcServiceName)
	if a.HTTPMux != nil && !skipHTTP {
		a.HTTPMux.DeregisterByPrefix(httpPrefix)
	}
	if a.FileRegistry != nil {
		a.FileRegistry.Deregister(descriptorPath)
	}
}

func (a *DynamicSchemaActivator) platformAPIVersion() string {
	if a.PlatformVersion != "" {
		return a.PlatformVersion
	}
	return platformresource.APIVersion
}

// platformKeyForCollection returns the registry key for a collection's
// platform service, or a zero key if the collection is empty.
func platformKeyForCollection(collectionID string) PlatformRegistrationKey {
	if collectionID == "" {
		return PlatformRegistrationKey{}
	}
	return PlatformRegistrationKey{Collection: domain.CollectionName(collectionID)}
}

// acquirePlatform adds ownerGRPC as an owner of key. On 0→1 it builds
// and registers the platform mux service, then records ownership.
// Must be called with a.mu held (serializes Activate/Deactivate).
func (a *DynamicSchemaActivator) acquirePlatform(schema domain.ExtensionResourceSchema, key PlatformRegistrationKey, ownerGRPC string) error {
	if _, exists := a.Registry.GetPlatform(key); exists {
		_, err := a.Registry.AddPlatformOwner(key, ownerGRPC, ActivePlatformRegistration{})
		return err
	}

	platformVersion := a.platformAPIVersion()
	platCfg := &platformresource.Config{
		CollectionConfig: dynamicapi.CollectionConfig{
			Version:      platformVersion,
			CollectionID: schema.CollectionID,
			Singular:     schema.Singular,
			Plural:       schema.Plural,
		},
	}

	platSvc, err := platformresource.BuildService(platCfg, a.PlatformDeps)
	if err != nil {
		return fmt.Errorf("build: %w", err)
	}

	transport := ActivePlatformRegistration{
		APIVersion:      domain.APIVersion(platformVersion),
		GRPCServiceName: platCfg.GRPCServiceName(),
		HTTPPrefix:      platCfg.HTTPPrefix(),
		DescriptorPath:  string(platSvc.Descriptors.File.Path()),
	}

	if err := a.GRPCMux.RegisterDesc(platSvc.Desc); err != nil {
		return fmt.Errorf("gRPC: %w", err)
	}
	if a.HTTPMux != nil {
		handler := platformresource.BuildHTTPHandler(platSvc, a.HTTPMux.Conn(), transport.HTTPPrefix)
		if err := a.HTTPMux.RegisterPrefixHandler(transport.HTTPPrefix, handler); err != nil {
			a.GRPCMux.Deregister(transport.GRPCServiceName)
			return fmt.Errorf("HTTP: %w", err)
		}
	}
	if a.FileRegistry != nil {
		if err := a.FileRegistry.Register(platSvc.Descriptors.File); err != nil {
			a.deregisterExtensionTransport(transport.GRPCServiceName, transport.HTTPPrefix, transport.DescriptorPath)
			return fmt.Errorf("file descriptor: %w", err)
		}
	}

	if _, err := a.Registry.AddPlatformOwner(key, ownerGRPC, transport); err != nil {
		a.deregisterExtensionTransport(transport.GRPCServiceName, transport.HTTPPrefix, transport.DescriptorPath)
		return err
	}
	return nil
}

// releasePlatformOwner drops ownerGRPC's platform reference and, if it
// was the last owner, deregisters the platform mux service. Must be
// called with a.mu held.
func (a *DynamicSchemaActivator) releasePlatformOwner(key PlatformRegistrationKey, ownerGRPC string) {
	dropped, last := a.Registry.RemovePlatformOwner(key, ownerGRPC)
	if last {
		a.deregisterExtensionTransport(dropped.GRPCServiceName, dropped.HTTPPrefix, dropped.DescriptorPath)
	}
}

// resolveEntryFile determines the compiler entry file for a schema.
// If EntryFile is set, it is used directly. For single-file schemas,
// the sole file is used. Multi-file schemas without an explicit
// entry file are rejected.
func resolveEntryFile(schema domain.ExtensionResourceSchema) (string, error) {
	if schema.EntryFile != "" {
		if _, ok := schema.ProtoFiles[schema.EntryFile]; !ok {
			return "", fmt.Errorf("entry file %q not found in schema proto files for %q", schema.EntryFile, schema.ResourceType)
		}
		return schema.EntryFile, nil
	}
	if len(schema.ProtoFiles) == 1 {
		for name := range schema.ProtoFiles {
			return name, nil
		}
	}
	return "", fmt.Errorf("schema for %q has %d proto files but no EntryFile specified", schema.ResourceType, len(schema.ProtoFiles))
}

// Deactivate removes the gRPC, HTTP, and file descriptor registrations
// for the extension identified by its activation ID, and clears the
// registry entry. If this was the last extension referencing a
// platform service, the platform service is deregistered as well.
func (a *DynamicSchemaActivator) Deactivate(id application.SchemaActivationID) {
	if a.Registry == nil {
		return
	}
	serviceName := string(id)

	a.mu.Lock()
	defer a.mu.Unlock()

	_, ver, ok := a.Registry.GetByGRPCServiceName(serviceName)
	if !ok {
		return
	}

	a.GRPCMux.Deregister(ver.GRPCServiceName)
	if a.HTTPMux != nil {
		a.HTTPMux.DeregisterByPrefix(ver.HTTPPrefix)
	}
	if a.FileRegistry != nil {
		a.FileRegistry.Deregister(ver.DescriptorPath)
	}

	a.Registry.UnregisterByGRPCServiceName(serviceName)

	if platformKey, ok := a.Registry.PlatformKeyForOwner(serviceName); ok {
		a.releasePlatformOwner(platformKey, serviceName)
	}
}

// schemaContentHash returns a deterministic SHA-256 over the schema's
// transport identity and proto content. Used to detect content changes
// across reconnections.
func schemaContentHash(s domain.ExtensionResourceSchema) [32]byte {
	h := sha256.New()
	h.Write([]byte(s.ResourceType.ServiceName()))
	h.Write([]byte{0})
	h.Write([]byte(s.ProtoPackage))
	h.Write([]byte{0})
	h.Write([]byte(s.Version))
	h.Write([]byte{0})
	h.Write([]byte(s.CollectionID))
	h.Write([]byte{0})
	if s.Management != nil {
		h.Write([]byte(s.Management.SpecMessage))
	}
	h.Write([]byte{0})
	h.Write([]byte(s.Singular))
	h.Write([]byte{0})
	h.Write([]byte(s.Plural))
	h.Write([]byte{0})

	keys := make([]string, 0, len(s.ProtoFiles))
	for k := range s.ProtoFiles {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write([]byte(s.ProtoFiles[k]))
		h.Write([]byte{0})
	}

	return [32]byte(h.Sum(nil))
}

// ContentHash exposes the hash for a gRPC service name, for testing.
func (a *DynamicSchemaActivator) ContentHash(grpcServiceName string) ([32]byte, bool) {
	if a.Registry == nil {
		return [32]byte{}, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	_, ver, ok := a.Registry.GetByGRPCServiceName(grpcServiceName)
	if !ok {
		return [32]byte{}, false
	}
	return ver.ContentHash, true
}

// SchemaContentHash is exported for testing the deterministic hash.
func SchemaContentHash(s domain.ExtensionResourceSchema) string {
	h := schemaContentHash(s)
	var b strings.Builder
	for _, v := range h {
		fmt.Fprintf(&b, "%02x", v)
	}
	return b.String()
}
