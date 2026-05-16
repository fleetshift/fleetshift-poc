package managedresource

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"google.golang.org/protobuf/proto"
)

// ServiceDescriptors holds the compiled descriptors for a dynamically-built
// managed resource service. These are used to create dynamic messages for
// gRPC request/response marshaling.
type ServiceDescriptors struct {
	// File is the synthesized file descriptor containing all messages and service.
	File protoreflect.FileDescriptor

	// Service is the service descriptor (e.g. ClusterService).
	Service protoreflect.ServiceDescriptor

	// Resource is the resource message descriptor (e.g. Cluster).
	Resource protoreflect.MessageDescriptor

	// CreateRequest is the create request message descriptor.
	CreateRequest protoreflect.MessageDescriptor

	// GetRequest is the get request message descriptor.
	GetRequest protoreflect.MessageDescriptor

	// ListRequest is the list request message descriptor.
	ListRequest protoreflect.MessageDescriptor

	// ListResponse is the list response message descriptor.
	ListResponse protoreflect.MessageDescriptor

	// DeleteRequest is the delete request message descriptor.
	DeleteRequest protoreflect.MessageDescriptor

	// Spec is the addon spec message descriptor.
	Spec protoreflect.MessageDescriptor
}

// BuildServiceDescriptors programmatically constructs the full set of proto
// descriptors for an AIP-compliant resource service. Given a resource type
// config and the addon's spec message descriptor, it builds:
//   - The resource message (envelope + spec)
//   - Create/Get/List/Delete request and response messages
//   - The service definition with all methods
//
// The resulting descriptors are used to instantiate dynamicpb.Message
// instances for gRPC marshaling at runtime.
func BuildServiceDescriptors(cfg *ResourceTypeConfig, specDesc protoreflect.MessageDescriptor) (*ServiceDescriptors, error) {
	if cfg == nil {
		return nil, fmt.Errorf("resource type config is required")
	}
	if cfg.Singular == "" || cfg.Plural == "" || cfg.ProtoPackage == "" {
		return nil, fmt.Errorf("singular, plural, and proto package are required")
	}
	if cfg.Singular[0] < 'A' || cfg.Singular[0] > 'Z' {
		return nil, fmt.Errorf("singular %q must start with an uppercase letter (PascalCase)", cfg.Singular)
	}
	if cfg.Plural[0] < 'A' || cfg.Plural[0] > 'Z' {
		return nil, fmt.Errorf("plural %q must start with an uppercase letter (PascalCase)", cfg.Plural)
	}
	if specDesc == nil {
		return nil, fmt.Errorf("spec descriptor is required")
	}

	singular := cfg.Singular
	lower := strings.ToLower(singular[:1]) + singular[1:]
	plural := cfg.Plural
	collectionID := cfg.CollectionID()
	resourceStateEnumName := singular + "State"

	specFullName := string(specDesc.FullName())
	specFile := specDesc.ParentFile()

	pkg := cfg.ProtoPackage
	// fqn builds fully-qualified names within the package (e.g. "fleetshift.v1.Cluster")
	fqn := func(name string) string { return pkg + "." + name }

	fdp := &descriptorpb.FileDescriptorProto{
		Name:       proto.String(fmt.Sprintf("dynamic/%s_service.proto", lower)),
		Package:    proto.String(pkg),
		Syntax:     proto.String("proto3"),
		Dependency: []string{string(specFile.Path()), "google/protobuf/timestamp.proto"},
		MessageType: []*descriptorpb.DescriptorProto{
			buildResourceMessage(singular, pkg, specFullName, resourceStateEnumName),
			buildCreateRequest(singular, lower, fqn(singular)),
			buildGetRequest(singular),
			buildListRequest(plural),
			buildListResponse(singular, plural, collectionID, fqn(singular)),
			buildDeleteRequest(singular),
		},
		Service: []*descriptorpb.ServiceDescriptorProto{
			buildService(singular, plural, pkg),
		},
	}

	// Build a file registry containing the spec's file and its dependencies
	// so protodesc can resolve cross-file references.
	files := new(protoregistry.Files)
	if err := registerFileAndDeps(files, specFile); err != nil {
		return nil, fmt.Errorf("register spec file deps: %w", err)
	}

	// Register google/protobuf/timestamp.proto from the global registry.
	tsFile, err := protoregistry.GlobalFiles.FindFileByPath("google/protobuf/timestamp.proto")
	if err != nil {
		return nil, fmt.Errorf("find timestamp.proto: %w", err)
	}
	if err := registerFileAndDeps(files, tsFile); err != nil {
		return nil, fmt.Errorf("register timestamp deps: %w", err)
	}

	fd, err := protodesc.NewFile(fdp, files)
	if err != nil {
		return nil, fmt.Errorf("build file descriptor: %w", err)
	}

	svc := fd.Services().ByName(protoreflect.Name(singular + "Service"))
	if svc == nil {
		return nil, fmt.Errorf("service %sService not found in built descriptor", singular)
	}

	return &ServiceDescriptors{
		File:          fd,
		Service:       svc,
		Resource:      fd.Messages().ByName(protoreflect.Name(singular)),
		CreateRequest: fd.Messages().ByName(protoreflect.Name("Create" + singular + "Request")),
		GetRequest:    fd.Messages().ByName(protoreflect.Name("Get" + singular + "Request")),
		ListRequest:   fd.Messages().ByName(protoreflect.Name("List" + plural + "Request")),
		ListResponse:  fd.Messages().ByName(protoreflect.Name("List" + plural + "Response")),
		DeleteRequest: fd.Messages().ByName(protoreflect.Name("Delete" + singular + "Request")),
		Spec:          specDesc,
	}, nil
}

func buildResourceMessage(singular, pkg, specFullName, resourceStateEnumName string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String(singular),
		EnumType: []*descriptorpb.EnumDescriptorProto{
			buildResourceStateEnum(resourceStateEnumName),
		},
		Field: []*descriptorpb.FieldDescriptorProto{
			stringField("name", 1),
			stringField("uid", 2),
			messageField("spec", 3, specFullName),
			int64Field("intent_version", 4),
			enumField("state", 5, pkg+"."+singular+"."+resourceStateEnumName),
			boolField("reconciling", 6),
			messageField("create_time", 7, "google.protobuf.Timestamp"),
			messageField("update_time", 8, "google.protobuf.Timestamp"),
			messageField("delete_time", 9, "google.protobuf.Timestamp"),
			stringField("etag", 10),
		},
	}
}

func buildCreateRequest(singular, lower, resourceFQN string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("Create" + singular + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			stringField(lower+"_id", 1),
			messageField(lower, 2, resourceFQN),
			bytesField("user_signature", 3),
			messageField("valid_until", 4, "google.protobuf.Timestamp"),
			int64Field("expected_generation", 5),
		},
	}
}

func buildGetRequest(singular string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("Get" + singular + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			stringField("name", 1),
		},
	}
}

func buildListRequest(plural string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("List" + plural + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			int32Field("page_size", 1),
			stringField("page_token", 2),
		},
	}
}

func buildListResponse(singular, plural, collectionID, resourceFQN string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("List" + plural + "Response"),
		Field: []*descriptorpb.FieldDescriptorProto{
			repeatedMessageField(collectionID, 1, resourceFQN),
			stringField("next_page_token", 2),
		},
	}
}

func buildDeleteRequest(singular string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("Delete" + singular + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			stringField("name", 1),
		},
	}
}

func buildService(singular, plural, pkg string) *descriptorpb.ServiceDescriptorProto {
	fqnPrefix := "." + pkg + "."
	return &descriptorpb.ServiceDescriptorProto{
		Name: proto.String(singular + "Service"),
		Method: []*descriptorpb.MethodDescriptorProto{
			{
				Name:       proto.String("Create" + singular),
				InputType:  proto.String(fqnPrefix + "Create" + singular + "Request"),
				OutputType: proto.String(fqnPrefix + singular),
			},
			{
				Name:       proto.String("Get" + singular),
				InputType:  proto.String(fqnPrefix + "Get" + singular + "Request"),
				OutputType: proto.String(fqnPrefix + singular),
			},
			{
				Name:       proto.String("List" + plural),
				InputType:  proto.String(fqnPrefix + "List" + plural + "Request"),
				OutputType: proto.String(fqnPrefix + "List" + plural + "Response"),
			},
			{
				Name:       proto.String("Delete" + singular),
				InputType:  proto.String(fqnPrefix + "Delete" + singular + "Request"),
				OutputType: proto.String(fqnPrefix + singular),
			},
		},
	}
}

// --- enum helpers ---

func buildResourceStateEnum(name string) *descriptorpb.EnumDescriptorProto {
	return &descriptorpb.EnumDescriptorProto{
		Name: proto.String(name),
		Value: []*descriptorpb.EnumValueDescriptorProto{
			{Name: proto.String("STATE_UNSPECIFIED"), Number: proto.Int32(0)},
			{Name: proto.String("CREATING"), Number: proto.Int32(1)},
			{Name: proto.String("ACTIVE"), Number: proto.Int32(2)},
			{Name: proto.String("DELETING"), Number: proto.Int32(3)},
			{Name: proto.String("FAILED"), Number: proto.Int32(4)},
			{Name: proto.String("PAUSED_AUTH"), Number: proto.Int32(5)},
		},
	}
}

func enumField(name string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
	fqn := typeName
	if !strings.HasPrefix(fqn, ".") {
		fqn = "." + fqn
	}
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(number),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(),
		TypeName: proto.String(fqn),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

// --- field builder helpers ---

func stringField(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

func bytesField(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

func int32Field(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

func int64Field(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

func boolField(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

func messageField(name string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
	fqn := typeName
	if !strings.HasPrefix(fqn, ".") {
		fqn = "." + fqn
	}
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(number),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
		TypeName: proto.String(fqn),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

func repeatedMessageField(name string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
	fqn := typeName
	if !strings.HasPrefix(fqn, ".") {
		fqn = "." + fqn
	}
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(number),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
		TypeName: proto.String(fqn),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
	}
}

func registerFileAndDeps(files *protoregistry.Files, fd protoreflect.FileDescriptor) error {
	// Already registered?
	if _, err := files.FindFileByPath(string(fd.Path())); err == nil {
		return nil
	}
	// Register dependencies first.
	for i := range fd.Imports().Len() {
		dep := fd.Imports().Get(i).FileDescriptor
		if err := registerFileAndDeps(files, dep); err != nil {
			return err
		}
	}
	return files.RegisterFile(fd)
}
