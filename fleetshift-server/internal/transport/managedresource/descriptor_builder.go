package managedresource

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	// Blank import ensures attestation.proto is registered in
	// protoregistry.GlobalFiles so the dynamic descriptor builder
	// can resolve the Provenance message type.
	_ "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
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

	// ResumeRequest is the resume request message descriptor.
	ResumeRequest protoreflect.MessageDescriptor

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
	if cfg.Singular == "" || cfg.Plural == "" || cfg.ProtoPackage == "" || cfg.CollectionID == "" {
		return nil, fmt.Errorf("singular, plural, proto package, and collection ID are required")
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
	collectionID := cfg.CollectionID
	resourceStateEnumName := singular + "State"

	specFullName := string(specDesc.FullName())
	specFile := specDesc.ParentFile()

	pkg := cfg.ProtoPackage
	// fqn builds fully-qualified names within the package (e.g. "kind.fleetshift.v1.Cluster")
	fqn := func(name string) string { return pkg + "." + name }

	pkgPath := strings.ReplaceAll(pkg, ".", "/")
	fdp := &descriptorpb.FileDescriptorProto{
		Name:       proto.String(fmt.Sprintf("dynamic/%s/%s_service.proto", pkgPath, lower)),
		Package:    proto.String(pkg),
		Syntax:     proto.String("proto3"),
		Dependency: []string{string(specFile.Path()), "google/protobuf/timestamp.proto", "fleetshift/v1/attestation.proto"},
		MessageType: []*descriptorpb.DescriptorProto{
			buildResourceMessage(singular, pkg, specFullName, resourceStateEnumName),
			buildCreateRequest(singular, lower, fqn(singular)),
			buildGetRequest(singular),
			buildListRequest(plural),
			buildListResponse(singular, plural, collectionID, fqn(singular)),
			buildDeleteRequest(singular),
			buildResumeRequest(singular),
		},
		Service: []*descriptorpb.ServiceDescriptorProto{
			buildService(singular, plural, pkg),
		},
	}

	// Build a file registry containing the spec's file and its dependencies
	// so protodesc can resolve cross-file references.
	files := new(protoregistry.Files)
	if err := dynamicapi.RegisterFileAndDeps(files, specFile); err != nil {
		return nil, fmt.Errorf("register spec file deps: %w", err)
	}

	// Register google/protobuf/timestamp.proto from the global registry.
	tsFile, err := protoregistry.GlobalFiles.FindFileByPath("google/protobuf/timestamp.proto")
	if err != nil {
		return nil, fmt.Errorf("find timestamp.proto: %w", err)
	}
	if err := dynamicapi.RegisterFileAndDeps(files, tsFile); err != nil {
		return nil, fmt.Errorf("register timestamp deps: %w", err)
	}

	// Register fleetshift/v1/attestation.proto for the Provenance field.
	attestFile, err := protoregistry.GlobalFiles.FindFileByPath("fleetshift/v1/attestation.proto")
	if err != nil {
		return nil, fmt.Errorf("find attestation.proto: %w", err)
	}
	if err := dynamicapi.RegisterFileAndDeps(files, attestFile); err != nil {
		return nil, fmt.Errorf("register attestation deps: %w", err)
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
		ResumeRequest: fd.Messages().ByName(protoreflect.Name("Resume" + singular + "Request")),
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
			dynamicapi.StringField("name", 1),
			dynamicapi.StringField("uid", 2),
			dynamicapi.MessageField("spec", 3, specFullName),
			int64Field("intent_version", 4),
			enumField("state", 5, pkg+"."+singular+"."+resourceStateEnumName),
			boolField("reconciling", 6),
			dynamicapi.MessageField("create_time", 7, "google.protobuf.Timestamp"),
			dynamicapi.MessageField("update_time", 8, "google.protobuf.Timestamp"),
			dynamicapi.MessageField("delete_time", 9, "google.protobuf.Timestamp"),
			dynamicapi.StringField("etag", 10),
			dynamicapi.MessageField("provenance", 11, "fleetshift.v1.Provenance"),
			int64Field("generation", 12),
			dynamicapi.StringField("pause_reason", 13),
		},
	}
}

func buildCreateRequest(singular, lower, resourceFQN string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("Create" + singular + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField(lower+"_id", 1),
			dynamicapi.MessageField(lower, 2, resourceFQN),
			bytesField("user_signature", 3),
			dynamicapi.MessageField("valid_until", 4, "google.protobuf.Timestamp"),
		},
	}
}

func buildGetRequest(singular string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("Get" + singular + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField("name", 1),
		},
	}
}

func buildListRequest(plural string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("List" + plural + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.Int32Field("page_size", 1),
			dynamicapi.StringField("page_token", 2),
		},
	}
}

func buildListResponse(singular, plural, collectionID, resourceFQN string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("List" + plural + "Response"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.RepeatedMessageField(collectionID, 1, resourceFQN),
			dynamicapi.StringField("next_page_token", 2),
		},
	}
}

func buildDeleteRequest(singular string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("Delete" + singular + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField("name", 1),
		},
	}
}

func buildResumeRequest(singular string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("Resume" + singular + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField("name", 1),
			bytesField("user_signature", 2),
			dynamicapi.MessageField("valid_until", 3, "google.protobuf.Timestamp"),
			dynamicapi.StringField("etag", 4),
			int64Field("expected_generation", 5),
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
			{
				Name:       proto.String("Resume" + singular),
				InputType:  proto.String(fqnPrefix + "Resume" + singular + "Request"),
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

// --- extension-only field builder helpers ---

func bytesField(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(),
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
