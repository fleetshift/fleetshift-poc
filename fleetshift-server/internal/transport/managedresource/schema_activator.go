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
// gap.
type DynamicSchemaActivator struct {
	GRPCMux      *DynamicServiceMux
	HTTPMux      *DynamicHTTPMux
	FileRegistry *DynamicFileRegistry
	GRPCAddr     string
	Deps         Deps

	mu     sync.Mutex
	hashes map[string][32]byte // service name → content hash
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
	serviceName := "fleetshift.v1." + schema.Singular + "Service"
	handle := application.SchemaHandle{
		ServiceName: serviceName,
		Plural:      schema.Plural,
	}
	hash := schemaContentHash(schema)

	a.mu.Lock()
	if a.hashes == nil {
		a.hashes = make(map[string][32]byte)
	}
	if prev, ok := a.hashes[serviceName]; ok && prev == hash {
		a.mu.Unlock()
		return handle, nil
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
		ResourceType:   schema.ResourceType,
		Singular:       schema.Singular,
		Plural:         schema.Plural,
		ProtoPackage:   "fleetshift.v1",
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
		return handle, nil
	}

	// Either new or changed — register or atomically replace.
	_, alreadyRegistered := a.hashes[serviceName]
	if alreadyRegistered {
		a.GRPCMux.Replace(svc)
		if a.HTTPMux != nil && a.GRPCAddr != "" {
			if err := a.HTTPMux.Replace(svc, a.GRPCAddr); err != nil {
				// TODO: roll back gRPC replace for atomicity.
				return application.SchemaHandle{}, fmt.Errorf("replace HTTP: %w", err)
			}
		}
		if a.FileRegistry != nil {
			a.FileRegistry.Replace(svc.Descriptors.File)
		}
	} else {
		if err := a.GRPCMux.Register(svc); err != nil {
			return application.SchemaHandle{}, fmt.Errorf("register gRPC: %w", err)
		}
		if a.HTTPMux != nil && a.GRPCAddr != "" {
			if err := a.HTTPMux.Register(svc, a.GRPCAddr); err != nil {
				a.GRPCMux.Deregister(handle.ServiceName)
				return application.SchemaHandle{}, fmt.Errorf("register HTTP: %w", err)
			}
		}
		if a.FileRegistry != nil {
			if err := a.FileRegistry.Register(svc.Descriptors.File); err != nil {
				a.GRPCMux.Deregister(handle.ServiceName)
				if a.HTTPMux != nil {
					a.HTTPMux.Deregister(handle.Plural)
				}
				return application.SchemaHandle{}, fmt.Errorf("register file descriptor: %w", err)
			}
		}
	}

	a.hashes[serviceName] = hash
	return handle, nil
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
// for the schema and clears the cached content hash.
func (a *DynamicSchemaActivator) Deactivate(handle application.SchemaHandle) {
	a.GRPCMux.Deregister(handle.ServiceName)
	if a.HTTPMux != nil {
		a.HTTPMux.Deregister(handle.Plural)
	}
	if a.FileRegistry != nil {
		a.FileRegistry.Deregister(dynamicFilePath(handle.ServiceName))
	}
	a.mu.Lock()
	delete(a.hashes, handle.ServiceName)
	a.mu.Unlock()
}

// dynamicFilePath derives the synthesized proto file path from a
// service name. Must match the path used in [BuildServiceDescriptors].
func dynamicFilePath(serviceName string) string {
	const prefix = "fleetshift.v1."
	const suffix = "Service"
	singular := serviceName[len(prefix) : len(serviceName)-len(suffix)]
	lower := strings.ToLower(singular[:1]) + singular[1:]
	return "dynamic/" + lower + "_service.proto"
}

// schemaContentHash returns a deterministic SHA-256 over the schema's
// proto files and spec message. Used to detect content changes across
// reconnections.
func schemaContentHash(s domain.ManagedResourceSchema) [32]byte {
	h := sha256.New()
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

// ContentHash exposes the hash for a service, for testing.
func (a *DynamicSchemaActivator) ContentHash(serviceName string) ([32]byte, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	h, ok := a.hashes[serviceName]
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
