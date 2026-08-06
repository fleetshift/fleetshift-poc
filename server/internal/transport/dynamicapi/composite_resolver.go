package dynamicapi

import (
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// CompositeDescriptorResolver tries the dynamic file registry first,
// then falls back to a static resolver (typically
// protoregistry.GlobalFiles). This allows the gRPC reflection server
// to resolve both statically compiled descriptors and dynamically
// synthesized managed resource descriptors.
type CompositeDescriptorResolver struct {
	Dynamic  *DynamicFileRegistry
	Fallback protodesc.Resolver
}

var _ protodesc.Resolver = (*CompositeDescriptorResolver)(nil)

// FindFileByPath resolves a file descriptor by path, checking the
// dynamic registry first.
func (r *CompositeDescriptorResolver) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	if fd, err := r.Dynamic.FindFileByPath(path); err == nil {
		return fd, nil
	}
	return r.Fallback.FindFileByPath(path)
}

// FindDescriptorByName resolves a descriptor by fully-qualified name,
// checking the dynamic registry first.
func (r *CompositeDescriptorResolver) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	if d, err := r.Dynamic.FindDescriptorByName(name); err == nil {
		return d, nil
	}
	return r.Fallback.FindDescriptorByName(name)
}

// NewCompositeDescriptorResolver creates a resolver that checks the
// dynamic file registry first, then falls back to GlobalFiles.
func NewCompositeDescriptorResolver(dynamic *DynamicFileRegistry) *CompositeDescriptorResolver {
	return &CompositeDescriptorResolver{
		Dynamic:  dynamic,
		Fallback: protoregistry.GlobalFiles,
	}
}
