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
// It keeps a content hash per service so that repeated Activate calls
// with unchanged schemas skip recompilation. When the schema content
// changes, the mux entry is atomically replaced — no deregister/register
// gap. It also tracks the prior handle per service so that if the
// transport identity changes (e.g. APIServiceName or Version), the old
// HTTP prefix and descriptor path are cleaned up.
//
// # Platform API version
//
// Platform-canonical services use the activator-selected platform API
// version (defaulting to [platformresource.APIVersion]). Extension
// [domain.ManagedResourceSchema.Version] applies only to the extension's
// own transport surface; it does not control the platform route or gRPC
// identity for the shared collection.
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

	mu     sync.Mutex
	hashes map[string][32]byte               // service name → content hash
	regs   map[string]*extensionRegistration // service name → registration state

	refCount        map[string]int                   // platform key → refcount
	platformHandles map[string]*platformRegistration // platform key → registration
	extensionKeys   map[string]string                // gRPC service name → platform key
}

// extensionRegistration tracks the transport details for one activated
// extension schema. Purely internal — the application layer only sees
// an opaque [application.SchemaRegistrationID].
type extensionRegistration struct {
	grpcServiceName string
	httpPrefix      string
	descriptorPath  string
}

// platformRegistration tracks what was registered for a platform
// service so it can be deregistered when the refcount drops to zero.
type platformRegistration struct {
	grpcServiceName string
	httpPrefix      string
	descriptorPath  string
}

var _ application.SchemaActivator = (*DynamicSchemaActivator)(nil)

// Activate compiles the schema's inline proto, builds a dynamic gRPC
// service, and registers it in the mux. If the schema is already active
// with identical content, the existing registration ID is returned
// without recompilation. If the content has changed, the mux entry is
// atomically replaced.
func (a *DynamicSchemaActivator) Activate(ctx context.Context, schema domain.ManagedResourceSchema) (application.SchemaRegistrationID, error) {
	if len(schema.ProtoFiles) == 0 {
		return "", fmt.Errorf("schema for %q has no proto files", schema.ResourceType)
	}

	// Compute registration identity and content hash before expensive
	// compilation so we can short-circuit when the schema is unchanged.
	serviceName := schema.ProtoPackage + "." + schema.Singular + "Service"
	pkgPath := strings.ReplaceAll(schema.ProtoPackage, ".", "/")
	lower := strings.ToLower(schema.Singular[:1]) + schema.Singular[1:]
	descriptorPath := fmt.Sprintf("dynamic/%s/%s_service.proto", pkgPath, lower)
	canonicalPrefix := "/apis/" + schema.APIServiceName + "/" + schema.Version + "/" + schema.CollectionID
	platformKey := platformKeyForCollection(schema.CollectionID)

	reg := &extensionRegistration{
		grpcServiceName: serviceName,
		httpPrefix:      canonicalPrefix,
		descriptorPath:  descriptorPath,
	}
	hash := schemaContentHash(schema)
	id := application.SchemaRegistrationID(serviceName)

	a.mu.Lock()
	if a.hashes == nil {
		a.hashes = make(map[string][32]byte)
	}
	if prev, ok := a.hashes[serviceName]; ok && prev == hash {
		a.mu.Unlock()
		return id, nil
	}
	a.mu.Unlock()

	entryFile, err := resolveEntryFile(schema)
	if err != nil {
		return "", err
	}

	specDesc, err := dynamicapi.CompileInline(
		ctx,
		schema.ProtoFiles,
		entryFile,
		protoreflect.FullName(schema.SpecMessage),
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
		APIServiceName: schema.APIServiceName,
		ProtoPackage:   schema.ProtoPackage,
		SpecMessage:    schema.SpecMessage,
		SpecDescriptor: specDesc.Message,
	}

	svc, err := Build(cfg, a.Deps)
	if err != nil {
		return "", fmt.Errorf("build service: %w", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Re-check after compilation in case a concurrent Activate completed
	// between our initial check and now.
	if prev, ok := a.hashes[serviceName]; ok && prev == hash {
		return id, nil
	}

	if a.regs == nil {
		a.regs = make(map[string]*extensionRegistration)
	}

	// Either new or changed — register or atomically replace.
	oldReg, alreadyRegistered := a.regs[serviceName]
	if alreadyRegistered {
		a.GRPCMux.ReplaceDesc(svc.Desc)
		if a.HTTPMux != nil {
			handler := BuildHTTPHandler(svc, a.HTTPMux.Conn(), oldReg.httpPrefix)
			a.HTTPMux.ReplacePrefixHandler(reg.httpPrefix, handler)
			if oldReg.httpPrefix != reg.httpPrefix {
				a.HTTPMux.DeregisterByPrefix(oldReg.httpPrefix)
			}
		}
		if a.FileRegistry != nil {
			if oldReg.descriptorPath != reg.descriptorPath {
				a.FileRegistry.Deregister(oldReg.descriptorPath)
			}
			a.FileRegistry.Replace(svc.Descriptors.File)
		}
	} else {
		if err := a.GRPCMux.RegisterDesc(svc.Desc); err != nil {
			return "", fmt.Errorf("register gRPC: %w", err)
		}
		if a.HTTPMux != nil {
			prefix := svc.Config.CanonicalHTTPPrefix()
			handler := BuildHTTPHandler(svc, a.HTTPMux.Conn(), prefix)
			if err := a.HTTPMux.RegisterPrefixHandler(prefix, handler); err != nil {
				a.GRPCMux.Deregister(reg.grpcServiceName)
				return "", fmt.Errorf("register HTTP: %w", err)
			}
		}
		if a.FileRegistry != nil {
			if err := a.FileRegistry.Register(svc.Descriptors.File); err != nil {
				a.GRPCMux.Deregister(reg.grpcServiceName)
				if a.HTTPMux != nil {
					a.HTTPMux.DeregisterByPrefix(reg.httpPrefix)
				}
				return "", fmt.Errorf("register file descriptor: %w", err)
			}
		}
	}

	// Platform service refcounting — always reconcile the previous
	// mapping so that transitions (including "had a key → now empty")
	// are handled correctly.
	a.initPlatformMaps()
	prevKey := a.extensionKeys[serviceName]

	if prevKey != "" && prevKey != platformKey {
		a.decrementRefCount(prevKey)
		delete(a.extensionKeys, serviceName)
	}

	if platformKey != "" && prevKey != platformKey {
		a.refCount[platformKey]++
		a.extensionKeys[serviceName] = platformKey

		if a.refCount[platformKey] == 1 {
			if err := a.registerPlatformService(schema); err != nil {
				a.refCount[platformKey]--
				delete(a.extensionKeys, serviceName)

				// Rollback the extension registration regardless of
				// whether this was a new registration or a replacement.
				// For replacements we can't restore the old compiled
				// service, so we deregister and clear cached state so
				// the next Activate rebuilds from scratch.
				a.GRPCMux.Deregister(reg.grpcServiceName)
				if a.HTTPMux != nil {
					a.HTTPMux.DeregisterByPrefix(reg.httpPrefix)
				}
				if a.FileRegistry != nil {
					a.FileRegistry.Deregister(reg.descriptorPath)
				}
				delete(a.hashes, serviceName)
				delete(a.regs, serviceName)
				return "", fmt.Errorf("register platform service: %w", err)
			}
		}
	}

	a.hashes[serviceName] = hash
	a.regs[serviceName] = reg
	return id, nil
}

func (a *DynamicSchemaActivator) platformAPIVersion() string {
	if a.PlatformVersion != "" {
		return a.PlatformVersion
	}
	return platformresource.APIVersion
}

// platformKeyForCollection returns the refcounting key for a
// collection's platform service, or "" if the collection lacks the
// required identity field.
func platformKeyForCollection(collectionID string) string {
	if collectionID != "" {
		return platformresource.ServiceName + "/" + collectionID
	}
	return ""
}

func (a *DynamicSchemaActivator) initPlatformMaps() {
	if a.refCount == nil {
		a.refCount = make(map[string]int)
	}
	if a.platformHandles == nil {
		a.platformHandles = make(map[string]*platformRegistration)
	}
	if a.extensionKeys == nil {
		a.extensionKeys = make(map[string]string)
	}
}

// registerPlatformService builds and registers the platform-canonical
// service for the schema's collection. Must be called with a.mu held.
func (a *DynamicSchemaActivator) registerPlatformService(schema domain.ManagedResourceSchema) error {
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

	reg := &platformRegistration{
		grpcServiceName: platCfg.GRPCServiceName(),
		httpPrefix:      platCfg.HTTPPrefix(),
		descriptorPath:  string(platSvc.Descriptors.File.Path()),
	}

	if err := a.GRPCMux.RegisterDesc(platSvc.Desc); err != nil {
		return fmt.Errorf("gRPC: %w", err)
	}
	if a.HTTPMux != nil {
		prefix := platCfg.HTTPPrefix()
		handler := platformresource.BuildHTTPHandler(platSvc, a.HTTPMux.Conn(), prefix)
		if err := a.HTTPMux.RegisterPrefixHandler(prefix, handler); err != nil {
			a.GRPCMux.Deregister(reg.grpcServiceName)
			return fmt.Errorf("HTTP: %w", err)
		}
	}
	if a.FileRegistry != nil {
		if err := a.FileRegistry.Register(platSvc.Descriptors.File); err != nil {
			a.GRPCMux.Deregister(reg.grpcServiceName)
			if a.HTTPMux != nil {
				a.HTTPMux.DeregisterByPrefix(reg.httpPrefix)
			}
			return fmt.Errorf("file descriptor: %w", err)
		}
	}

	platformKey := platformKeyForCollection(schema.CollectionID)
	a.platformHandles[platformKey] = reg
	return nil
}

// decrementRefCount reduces the refcount for a platform key and
// deregisters the platform service when it reaches zero. Must be
// called with a.mu held.
func (a *DynamicSchemaActivator) decrementRefCount(platformKey string) {
	a.refCount[platformKey]--
	if a.refCount[platformKey] <= 0 {
		delete(a.refCount, platformKey)
		if reg, ok := a.platformHandles[platformKey]; ok {
			a.GRPCMux.Deregister(reg.grpcServiceName)
			if a.HTTPMux != nil {
				a.HTTPMux.DeregisterByPrefix(reg.httpPrefix)
			}
			if a.FileRegistry != nil {
				a.FileRegistry.Deregister(reg.descriptorPath)
			}
			delete(a.platformHandles, platformKey)
		}
	}
}

// resolveEntryFile determines the compiler entry file for a schema.
// If EntryFile is set, it is used directly. For single-file schemas,
// the sole file is used. Multi-file schemas without an explicit
// entry file are rejected.
func resolveEntryFile(schema domain.ManagedResourceSchema) (string, error) {
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
// for the extension identified by its registration ID, and clears the
// cached content hash. If this was the last extension referencing a
// platform service, the platform service is deregistered as well.
func (a *DynamicSchemaActivator) Deactivate(id application.SchemaRegistrationID) {
	serviceName := string(id)

	a.mu.Lock()
	defer a.mu.Unlock()

	reg, ok := a.regs[serviceName]
	if !ok {
		return
	}

	a.GRPCMux.Deregister(reg.grpcServiceName)
	if a.HTTPMux != nil {
		a.HTTPMux.DeregisterByPrefix(reg.httpPrefix)
	}
	if a.FileRegistry != nil {
		a.FileRegistry.Deregister(reg.descriptorPath)
	}

	delete(a.hashes, serviceName)
	delete(a.regs, serviceName)

	if platformKey, hasPlatform := a.extensionKeys[serviceName]; hasPlatform {
		delete(a.extensionKeys, serviceName)
		a.decrementRefCount(platformKey)
	}
}

// schemaContentHash returns a deterministic SHA-256 over the schema's
// transport identity and proto content. Used to detect content changes
// across reconnections.
func schemaContentHash(s domain.ManagedResourceSchema) [32]byte {
	h := sha256.New()
	h.Write([]byte(s.APIServiceName))
	h.Write([]byte{0})
	h.Write([]byte(s.ProtoPackage))
	h.Write([]byte{0})
	h.Write([]byte(s.Version))
	h.Write([]byte{0})
	h.Write([]byte(s.CollectionID))
	h.Write([]byte{0})
	h.Write([]byte(s.SpecMessage))
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
	a.mu.Lock()
	defer a.mu.Unlock()
	h, ok := a.hashes[grpcServiceName]
	return h, ok
}

// SchemaContentHash is exported for testing the deterministic hash.
func SchemaContentHash(s domain.ManagedResourceSchema) string {
	h := schemaContentHash(s)
	var b strings.Builder
	for _, v := range h {
		fmt.Fprintf(&b, "%02x", v)
	}
	return b.String()
}
