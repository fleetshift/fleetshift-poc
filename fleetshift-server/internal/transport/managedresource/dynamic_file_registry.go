package managedresource

import (
	"sync"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// DynamicFileRegistry is a thread-safe registry of
// protoreflect.FileDescriptor instances for dynamically compiled managed
// resource services. It supports add, replace, and remove — unlike
// protoregistry.GlobalFiles which is append-only.
//
// Internally it maintains a map of file path → descriptor and rebuilds
// a *protoregistry.Files on each mutation. The set of dynamic files is
// small (typically single digits), so rebuild cost is negligible.
//
// DynamicFileRegistry implements protodesc.Resolver (FindFileByPath +
// FindDescriptorByName) for use as a gRPC reflection DescriptorResolver.
type DynamicFileRegistry struct {
	mu    sync.RWMutex
	files map[string]protoreflect.FileDescriptor
	reg   *protoregistry.Files
}

// NewDynamicFileRegistry creates an empty registry.
func NewDynamicFileRegistry() *DynamicFileRegistry {
	return &DynamicFileRegistry{
		files: make(map[string]protoreflect.FileDescriptor),
		reg:   new(protoregistry.Files),
	}
}

// Register adds a file descriptor and its transitive imports (that
// aren't already tracked) to the registry. Returns an error if the
// file path is already registered — use [Replace] for atomic updates.
func (r *DynamicFileRegistry) Register(fd protoreflect.FileDescriptor) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	path := string(fd.Path())
	if _, exists := r.files[path]; exists {
		return &alreadyRegisteredError{path: path}
	}

	r.addWithDeps(fd)
	r.rebuild()
	return nil
}

// Replace atomically swaps a file descriptor. If the path is not
// currently registered, it is added (same as [Register]).
func (r *DynamicFileRegistry) Replace(fd protoreflect.FileDescriptor) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.addWithDeps(fd)
	r.rebuild()
}

// Deregister removes a file descriptor by path. No-op if the path is
// not registered. Dependencies shared with other registered files are
// retained.
func (r *DynamicFileRegistry) Deregister(path string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.files[path]; !exists {
		return
	}
	delete(r.files, path)

	// Prune deps that are no longer needed by any remaining file.
	needed := make(map[string]bool)
	for _, fd := range r.files {
		r.collectDeps(fd, needed)
	}
	for p := range r.files {
		if !needed[p] {
			delete(r.files, p)
		}
	}

	r.rebuild()
}

// FindFileByPath satisfies protodesc.Resolver.
func (r *DynamicFileRegistry) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.reg.FindFileByPath(path)
}

// FindDescriptorByName satisfies protodesc.Resolver.
func (r *DynamicFileRegistry) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.reg.FindDescriptorByName(name)
}

// addWithDeps adds a file and its transitive imports to the files map.
// Skips files already tracked or already in GlobalFiles (those are
// resolved by the composite fallback). Must be called with mu held.
func (r *DynamicFileRegistry) addWithDeps(fd protoreflect.FileDescriptor) {
	path := string(fd.Path())
	r.files[path] = fd

	for i := range fd.Imports().Len() {
		dep := fd.Imports().Get(i).FileDescriptor
		depPath := string(dep.Path())
		if _, tracked := r.files[depPath]; tracked {
			continue
		}
		if _, err := protoregistry.GlobalFiles.FindFileByPath(depPath); err == nil {
			continue
		}
		r.addWithDeps(dep)
	}
}

// collectDeps records the paths of fd and all its transitive imports.
func (r *DynamicFileRegistry) collectDeps(fd protoreflect.FileDescriptor, out map[string]bool) {
	path := string(fd.Path())
	if out[path] {
		return
	}
	out[path] = true
	for i := range fd.Imports().Len() {
		dep := fd.Imports().Get(i).FileDescriptor
		r.collectDeps(dep, out)
	}
}

// rebuild constructs a fresh *protoregistry.Files from the current
// files map. Must be called with mu held.
func (r *DynamicFileRegistry) rebuild() {
	reg := new(protoregistry.Files)
	// Register files in dependency order: deps before dependents.
	registered := make(map[string]bool)
	for _, fd := range r.files {
		r.registerOrdered(reg, fd, registered)
	}
	r.reg = reg
}

// registerOrdered registers fd and its deps in topological order.
func (r *DynamicFileRegistry) registerOrdered(reg *protoregistry.Files, fd protoreflect.FileDescriptor, registered map[string]bool) {
	path := string(fd.Path())
	if registered[path] {
		return
	}
	for i := range fd.Imports().Len() {
		dep := fd.Imports().Get(i).FileDescriptor
		depPath := string(dep.Path())
		if _, inMap := r.files[depPath]; inMap {
			r.registerOrdered(reg, dep, registered)
		}
	}
	_ = reg.RegisterFile(fd)
	registered[path] = true
}

type alreadyRegisteredError struct {
	path string
}

func (e *alreadyRegisteredError) Error() string {
	return "dynamic file registry: " + e.path + " already registered"
}
