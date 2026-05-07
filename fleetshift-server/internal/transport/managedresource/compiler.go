// Package managedtype provides in-process proto compilation and dynamic
// gRPC service registration for addon-defined managed resource types.
// It enables the platform to host typed, AIP-compliant gRPC services
// without requiring compile-time Go stub generation for each addon type.
package managedresource

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/reporter"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
)

// SpecDescriptor holds the compiled descriptor for an addon-defined spec
// message along with metadata needed to build a resource service.
type SpecDescriptor struct {
	// File is the compiled file descriptor containing the spec message.
	File protoreflect.FileDescriptor

	// Message is the specific spec message descriptor (e.g. ClusterSpec).
	Message protoreflect.MessageDescriptor
}

// CompileSpec compiles a .proto file from the filesystem and returns the
// descriptor for the named spec message. The importPaths are used to
// resolve proto imports from the filesystem. Imports that are not found on
// the filesystem (such as buf.validate or other well-known dependencies)
// are resolved from the Go process's global proto registry, which contains
// descriptors for all transitively imported proto Go packages.
//
// For the addon lifecycle's production path, prefer [CompileInline] which
// compiles from inline proto content provided by the addon workload at
// connect time.
func CompileSpec(ctx context.Context, in CompileInput) (*SpecDescriptor, error) {
	resolver := protocompile.CompositeResolver{
		protocompile.WithStandardImports(
			&protocompile.SourceResolver{
				ImportPaths: in.ImportPaths,
			},
		),
		globalRegistryResolver{},
	}

	compiler := &protocompile.Compiler{
		Resolver:       resolver,
		SourceInfoMode: protocompile.SourceInfoStandard,
		Reporter:       reporter.NewReporter(nil, nil),
	}

	files, err := compiler.Compile(ctx, in.SourceFile)
	if err != nil {
		return nil, fmt.Errorf("compile %s: %w", in.SourceFile, err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no files compiled from %s", in.SourceFile)
	}

	fd := files[0]
	msgDesc := fd.Messages().ByName(protoreflect.Name(in.MessageName))
	if msgDesc == nil {
		msgDesc = findMessageByFullName(fd, protoreflect.FullName(in.MessageName))
	}
	if msgDesc == nil {
		return nil, fmt.Errorf("message %q not found in %s", in.MessageName, in.SourceFile)
	}

	return &SpecDescriptor{
		File:    fd,
		Message: msgDesc,
	}, nil
}

// globalRegistryResolver resolves proto imports from the Go process's global
// proto file registry. This allows protocompile to satisfy imports for
// dependencies that are already compiled into the binary (e.g. buf.validate,
// google.api annotations) without needing their .proto source on the filesystem.
type globalRegistryResolver struct{}

func (globalRegistryResolver) FindFileByPath(path string) (protocompile.SearchResult, error) {
	fd, err := protoregistry.GlobalFiles.FindFileByPath(path)
	if err != nil {
		return protocompile.SearchResult{}, err
	}
	return protocompile.SearchResult{Desc: fd}, nil
}

// CompileInput holds the parameters for [CompileSpec].
type CompileInput struct {
	// SourceFile is the proto file path relative to one of the ImportPaths.
	SourceFile string

	// MessageName is the fully-qualified or simple name of the spec message.
	MessageName string

	// ImportPaths are filesystem directories to search for imported protos.
	ImportPaths []string
}

// CompileInline compiles proto definitions provided as inline content
// (a map of virtual filename to proto source). The entryFile must be a
// key in the map. Well-known imports (google/protobuf/*, buf.validate/*)
// are resolved from the global proto registry.
//
// This is the compilation path used when an addon workload provides its
// schema at connect time — the proto content is transmitted inline,
// not read from the filesystem.
func CompileInline(ctx context.Context, protoFiles map[string]string, entryFile string, messageName protoreflect.FullName) (*SpecDescriptor, error) {
	resolver := protocompile.CompositeResolver{
		protocompile.WithStandardImports(
			inlineResolver(protoFiles),
		),
		globalRegistryResolver{},
	}

	compiler := &protocompile.Compiler{
		Resolver:       resolver,
		SourceInfoMode: protocompile.SourceInfoStandard,
		Reporter:       reporter.NewReporter(nil, nil),
	}

	files, err := compiler.Compile(ctx, entryFile)
	if err != nil {
		return nil, fmt.Errorf("compile inline %s: %w", entryFile, err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("no files compiled from inline %s", entryFile)
	}

	fd := files[0]
	msgDesc := fd.Messages().ByName(protoreflect.Name(messageName.Name()))
	if msgDesc == nil {
		msgDesc = findMessageByFullName(fd, messageName)
	}
	if msgDesc == nil {
		return nil, fmt.Errorf("message %q not found in inline %s", messageName, entryFile)
	}

	return &SpecDescriptor{
		File:    fd,
		Message: msgDesc,
	}, nil
}

// inlineResolver resolves proto imports from a map of virtual filenames
// to proto source content.
type inlineResolver map[string]string

func (r inlineResolver) FindFileByPath(path string) (protocompile.SearchResult, error) {
	content, ok := r[path]
	if !ok {
		return protocompile.SearchResult{}, os.ErrNotExist
	}
	return protocompile.SearchResult{
		Source: strings.NewReader(content),
	}, nil
}

// CompileFromDescriptorSet loads a pre-compiled FileDescriptorSet
// (e.g. from buf build -o) and extracts the named spec message.
// This avoids needing proto source on the filesystem at runtime.
func CompileFromDescriptorSet(fds *descriptorpb.FileDescriptorSet, messageName protoreflect.FullName) (*SpecDescriptor, error) {
	fdFiles, err := protodesc.NewFiles(fds)
	if err != nil {
		return nil, fmt.Errorf("create file registry: %w", err)
	}

	desc, err := fdFiles.FindDescriptorByName(messageName)
	if err != nil {
		return nil, fmt.Errorf("find message %s: %w", messageName, err)
	}

	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, fmt.Errorf("%s is not a message (got %T)", messageName, desc)
	}

	return &SpecDescriptor{
		File:    msgDesc.ParentFile(),
		Message: msgDesc,
	}, nil
}

// CompileFromGlobalRegistry extracts a spec descriptor from the global
// proto registry (for compiled-in types like our ClusterSpec).
func CompileFromGlobalRegistry(messageName protoreflect.FullName) (*SpecDescriptor, error) {
	msgType, err := protoregistry.GlobalTypes.FindMessageByName(messageName)
	if err != nil {
		return nil, fmt.Errorf("find message %s in global registry: %w", messageName, err)
	}

	msgDesc := msgType.Descriptor()
	return &SpecDescriptor{
		File:    msgDesc.ParentFile(),
		Message: msgDesc,
	}, nil
}

func findMessageByFullName(fd protoreflect.FileDescriptor, fullName protoreflect.FullName) protoreflect.MessageDescriptor {
	msgs := fd.Messages()
	for i := range msgs.Len() {
		msg := msgs.Get(i)
		if msg.FullName() == fullName {
			return msg
		}
	}
	return nil
}

// FindImportPaths returns the standard set of import paths needed
// to compile addon spec protos that use buf.validate and well-known types.
// It checks for the proto sources directory at the given root.
func FindImportPaths(protoRoot string) ([]string, error) {
	paths := []string{protoRoot}

	// Check for well-known types in the proto root
	_, err := fs.Stat(os.DirFS(protoRoot), "google/protobuf/any.proto")
	if err != nil {
		// Try GOPATH-based locations for well-known types
		gopath := os.Getenv("GOPATH")
		if gopath == "" {
			gopath = os.Getenv("HOME") + "/go"
		}
		// protocompile's WithStandardImports handles WKT automatically,
		// but buf.validate needs to be found on the import path.
		_ = gopath
	}

	// buf.validate protos are typically available via the buf module cache
	// or bundled with the binary. For now, we rely on protocompile's
	// standard imports + the proto root containing all needed imports.
	if info, err := os.Stat(protoRoot); err != nil || !info.IsDir() {
		return nil, fmt.Errorf("proto root %q does not exist or is not a directory", protoRoot)
	}

	return paths, nil
}

// SpecMessageName extracts the simple message name from a fully-qualified name.
func SpecMessageName(fullName protoreflect.FullName) protoreflect.Name {
	s := string(fullName)
	if idx := strings.LastIndex(s, "."); idx >= 0 {
		return protoreflect.Name(s[idx+1:])
	}
	return protoreflect.Name(s)
}
