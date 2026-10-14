package dynamicapi

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/reporter"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// SpecDescriptor holds the compiled descriptor for an addon-defined spec
// message along with metadata needed to build a resource service.
type SpecDescriptor struct {
	// File is the compiled file descriptor containing the spec message.
	File protoreflect.FileDescriptor

	// Message is the specific spec message descriptor (e.g. ClusterSpec).
	Message protoreflect.MessageDescriptor
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
