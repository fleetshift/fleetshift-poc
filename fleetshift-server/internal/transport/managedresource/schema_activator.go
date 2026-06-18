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
)

// DynamicSchemaActivator implements [application.SchemaActivator] by
// compiling proto from inline sources, building a dynamic gRPC service,
// and registering it in the [DynamicServiceMux], [DynamicHTTPMux], and
// [DynamicFileRegistry] (for gRPC reflection).
//
// It keeps a content hash per service so that repeated Activate calls
// with unchanged schemas skip recompilation. When the schema content
// changes, the mux entry is atomically replaced — no deregister/register
// gap. It also tracks the prior handle per service so that if the
// transport identity changes (e.g. APIServiceName or Version), the old
// HTTP prefix and descriptor path are cleaned up.
type DynamicSchemaActivator struct {
	GRPCMux      *DynamicServiceMux
	HTTPMux      *DynamicHTTPMux
	FileRegistry *DynamicFileRegistry
	Deps         Deps
	PlatformDeps PlatformDeps

	mu      sync.Mutex
	hashes  map[string][32]byte                 // gRPC service name → content hash
	handles map[string]application.SchemaHandle // gRPC service name → prior handle

	platformRefs    map[string]int                   // platform key → refcount
	platformHandles map[string]*platformRegistration // platform key → registration
	extensionKeys   map[string]string                // gRPC service name → platform key
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
// with identical content, the existing handle is returned without
// recompilation. If the content has changed, the mux entry is
// atomically replaced.
func (a *DynamicSchemaActivator) Activate(ctx context.Context, schema domain.ManagedResourceSchema) (application.SchemaHandle, error) {
	if len(schema.ProtoFiles) == 0 {
		return application.SchemaHandle{}, fmt.Errorf("schema for %q has no proto files", schema.ResourceType)
	}

	// Compute handle and content hash before expensive compilation so
	// we can short-circuit when the schema is unchanged.
	serviceName := schema.ProtoPackage + "." + schema.Singular + "Service"
	pkgPath := strings.ReplaceAll(schema.ProtoPackage, ".", "/")
	lower := strings.ToLower(schema.Singular[:1]) + schema.Singular[1:]
	descriptorPath := fmt.Sprintf("dynamic/%s/%s_service.proto", pkgPath, lower)
	canonicalPrefix := "/apis/" + schema.APIServiceName + "/" + schema.Version + "/" + schema.CollectionID
	var platformKey string
	if schema.CollectionID != "" && schema.Version != "" {
		platformKey = PlatformServiceName + "/" + schema.Version + "/" + schema.CollectionID
	}

	handle := application.SchemaHandle{
		GRPCServiceName: serviceName,
		HTTPPrefix:      canonicalPrefix,
		DescriptorPath:  descriptorPath,
	}
	hash := schemaContentHash(schema)

	a.mu.Lock()
	if a.hashes == nil {
		a.hashes = make(map[string][32]byte)
	}
	if prev, ok := a.hashes[serviceName]; ok && prev == hash {
		h := a.handles[serviceName]
		a.mu.Unlock()
		return h, nil
	}
	a.mu.Unlock()

	entryFile, err := resolveEntryFile(schema)
	if err != nil {
		return application.SchemaHandle{}, err
	}

	specDesc, err := CompileInline(
		ctx,
		schema.ProtoFiles,
		entryFile,
		protoreflect.FullName(schema.SpecMessage),
	)
	if err != nil {
		return application.SchemaHandle{}, fmt.Errorf("compile proto: %w", err)
	}

	cfg := &ResourceTypeConfig{
		CollectionConfig: CollectionConfig{
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
		return application.SchemaHandle{}, fmt.Errorf("build service: %w", err)
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Re-check after compilation in case a concurrent Activate completed
	// between our initial check and now.
	if prev, ok := a.hashes[serviceName]; ok && prev == hash {
		return a.handles[serviceName], nil
	}

	if a.handles == nil {
		a.handles = make(map[string]application.SchemaHandle)
	}

	// Either new or changed — register or atomically replace.
	oldHandle, alreadyRegistered := a.handles[serviceName]
	if alreadyRegistered {
		a.GRPCMux.Replace(svc)
		if a.HTTPMux != nil {
			a.HTTPMux.Replace(svc, oldHandle.HTTPPrefix)
		}
		if a.FileRegistry != nil {
			if oldHandle.DescriptorPath != handle.DescriptorPath {
				a.FileRegistry.Deregister(oldHandle.DescriptorPath)
			}
			a.FileRegistry.Replace(svc.Descriptors.File)
		}
	} else {
		if err := a.GRPCMux.Register(svc); err != nil {
			return application.SchemaHandle{}, fmt.Errorf("register gRPC: %w", err)
		}
		if a.HTTPMux != nil {
			if err := a.HTTPMux.Register(svc); err != nil {
				a.GRPCMux.Deregister(handle.GRPCServiceName)
				return application.SchemaHandle{}, fmt.Errorf("register HTTP: %w", err)
			}
		}
		if a.FileRegistry != nil {
			if err := a.FileRegistry.Register(svc.Descriptors.File); err != nil {
				a.GRPCMux.Deregister(handle.GRPCServiceName)
				if a.HTTPMux != nil {
					a.HTTPMux.DeregisterByPrefix(handle.HTTPPrefix)
				}
				return application.SchemaHandle{}, fmt.Errorf("register file descriptor: %w", err)
			}
		}
	}

	// Platform service refcounting.
	if platformKey != "" {
		a.initPlatformMaps()

		prevKey := a.extensionKeys[serviceName]
		needIncrement := prevKey != platformKey

		if needIncrement {
			if prevKey != "" {
				a.decrementPlatform(prevKey)
			}

			a.platformRefs[platformKey]++
			a.extensionKeys[serviceName] = platformKey

			if a.platformRefs[platformKey] == 1 {
				if err := a.registerPlatformService(schema); err != nil {
					// Rollback: undo extension registration and refcount.
					a.platformRefs[platformKey]--
					delete(a.extensionKeys, serviceName)
					if !alreadyRegistered {
						a.GRPCMux.Deregister(handle.GRPCServiceName)
						if a.HTTPMux != nil {
							a.HTTPMux.DeregisterByPrefix(handle.HTTPPrefix)
						}
						if a.FileRegistry != nil {
							a.FileRegistry.Deregister(handle.DescriptorPath)
						}
					}
					return application.SchemaHandle{}, fmt.Errorf("register platform service: %w", err)
				}
			}
		}
	}

	a.hashes[serviceName] = hash
	a.handles[serviceName] = handle
	return handle, nil
}

func (a *DynamicSchemaActivator) initPlatformMaps() {
	if a.platformRefs == nil {
		a.platformRefs = make(map[string]int)
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
	platCfg := &PlatformResourceConfig{
		CollectionConfig: CollectionConfig{
			Version:      schema.Version,
			CollectionID: schema.CollectionID,
			Singular:     schema.Singular,
			Plural:       schema.Plural,
		},
	}

	platSvc, err := BuildPlatformService(platCfg, a.PlatformDeps)
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
		handler := buildPlatformHTTPHandler(platSvc, a.HTTPMux.Conn(), prefix)
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

	platformKey := PlatformServiceName + "/" + schema.Version + "/" + schema.CollectionID
	a.platformHandles[platformKey] = reg
	return nil
}

// decrementPlatform reduces the refcount for a platform key and
// deregisters the platform service when it reaches zero. Must be
// called with a.mu held.
func (a *DynamicSchemaActivator) decrementPlatform(platformKey string) {
	a.platformRefs[platformKey]--
	if a.platformRefs[platformKey] <= 0 {
		delete(a.platformRefs, platformKey)
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
// for the extension identified by its gRPC service name, and clears the
// cached content hash and handle. If this was the last extension
// referencing a platform service, the platform service is deregistered
// as well.
//
// The activator looks up all registration details from its internal
// state — callers only need to provide the stable service name.
func (a *DynamicSchemaActivator) Deactivate(grpcServiceName string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	handle, ok := a.handles[grpcServiceName]
	if !ok {
		return
	}

	a.GRPCMux.Deregister(handle.GRPCServiceName)
	if a.HTTPMux != nil {
		a.HTTPMux.DeregisterByPrefix(handle.HTTPPrefix)
	}
	if a.FileRegistry != nil {
		a.FileRegistry.Deregister(handle.DescriptorPath)
	}

	delete(a.hashes, grpcServiceName)
	delete(a.handles, grpcServiceName)

	if platformKey, hasPlatform := a.extensionKeys[grpcServiceName]; hasPlatform {
		delete(a.extensionKeys, grpcServiceName)
		a.decrementPlatform(platformKey)
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
