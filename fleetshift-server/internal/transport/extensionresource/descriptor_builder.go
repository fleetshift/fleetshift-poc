package extensionresource

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

// ExtensionServiceDescriptors holds the compiled descriptors for a dynamically-built
// extension resource service. These are used to create dynamic messages for
// gRPC request/response marshaling.
//
// Management request descriptors (CreateRequest, DeleteRequest, ResumeRequest)
// and Spec are nil when the type has no management capability.
type ExtensionServiceDescriptors struct {
	// File is the synthesized file descriptor containing all messages and service.
	File protoreflect.FileDescriptor

	// Service is the service descriptor (e.g. ClusterService).
	Service protoreflect.ServiceDescriptor

	// Resource is the resource message descriptor (e.g. Cluster).
	Resource protoreflect.MessageDescriptor

	// CreateRequest is the create request message descriptor.
	// Nil when the type has no management capability.
	CreateRequest protoreflect.MessageDescriptor

	// GetRequest is the get request message descriptor.
	GetRequest protoreflect.MessageDescriptor

	// ListRequest is the list request message descriptor.
	ListRequest protoreflect.MessageDescriptor

	// ListResponse is the list response message descriptor.
	ListResponse protoreflect.MessageDescriptor

	// DeleteRequest is the delete request message descriptor.
	// Nil when the type has no management capability.
	DeleteRequest protoreflect.MessageDescriptor

	// ResumeRequest is the resume request message descriptor.
	// Nil when the type has no management capability.
	ResumeRequest protoreflect.MessageDescriptor

	// Spec is the addon spec message descriptor.
	// Nil when the type has no management capability.
	Spec protoreflect.MessageDescriptor
}

// BuildExtensionServiceDescriptors programmatically constructs the full set of proto
// descriptors for an AIP-compliant extension resource service. Given a
// resource type config and (when management is present) the addon's spec
// message descriptor, it builds:
//   - The resource message (common envelope + capability fields)
//   - Request/response messages for the methods the type supports
//   - The service definition with those methods
//
// The resulting descriptors are used to instantiate dynamicpb.Message
// instances for gRPC marshaling at runtime.
func BuildExtensionServiceDescriptors(cfg *ResourceTypeConfig, specDesc protoreflect.MessageDescriptor) (*ExtensionServiceDescriptors, error) {
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
	hasManagement := cfg.Capabilities.Management != nil
	hasInventory := cfg.Capabilities.Inventory != nil
	if !hasManagement && !hasInventory {
		return nil, fmt.Errorf("at least one of management or inventory capability is required")
	}
	if hasManagement && specDesc == nil {
		return nil, fmt.Errorf("spec descriptor is required when management capability is set")
	}

	singular := cfg.Singular
	lower := strings.ToLower(singular[:1]) + singular[1:]
	plural := cfg.Plural
	collectionID := cfg.CollectionID
	resourceStateEnumName := singular + "State"

	pkg := cfg.ProtoPackage
	fqn := func(name string) string { return pkg + "." + name }
	resourceFQN := fqn(singular)

	deps := []string{"google/protobuf/timestamp.proto"}
	files := new(protoregistry.Files)

	tsFile, err := protoregistry.GlobalFiles.FindFileByPath("google/protobuf/timestamp.proto")
	if err != nil {
		return nil, fmt.Errorf("find timestamp.proto: %w", err)
	}
	if err := dynamicapi.RegisterFileAndDeps(files, tsFile); err != nil {
		return nil, fmt.Errorf("register timestamp deps: %w", err)
	}

	var specFullName string
	if hasManagement {
		specFullName = string(specDesc.FullName())
		specFile := specDesc.ParentFile()
		deps = append([]string{string(specFile.Path())}, deps...)
		if err := dynamicapi.RegisterFileAndDeps(files, specFile); err != nil {
			return nil, fmt.Errorf("register spec file deps: %w", err)
		}
		deps = append(deps, "fleetshift/v1/attestation.proto")
		attestFile, err := protoregistry.GlobalFiles.FindFileByPath("fleetshift/v1/attestation.proto")
		if err != nil {
			return nil, fmt.Errorf("find attestation.proto: %w", err)
		}
		if err := dynamicapi.RegisterFileAndDeps(files, attestFile); err != nil {
			return nil, fmt.Errorf("register attestation deps: %w", err)
		}
	}
	if hasInventory {
		deps = append(deps, "google/protobuf/struct.proto")
		structFile, err := protoregistry.GlobalFiles.FindFileByPath("google/protobuf/struct.proto")
		if err != nil {
			return nil, fmt.Errorf("find struct.proto: %w", err)
		}
		if err := dynamicapi.RegisterFileAndDeps(files, structFile); err != nil {
			return nil, fmt.Errorf("register struct deps: %w", err)
		}
	}

	messages := make([]*descriptorpb.DescriptorProto, 0, 8)
	resourceMsg := buildResourceMessage(cfg, singular, pkg, resourceFQN, specFullName, resourceStateEnumName)
	messages = append(messages, resourceMsg)

	if hasInventory {
		// Condition is a file-level sibling so map entries can reference it.
		messages = append([]*descriptorpb.DescriptorProto{buildConditionMessage()}, messages...)
	}

	if hasManagement {
		messages = append(messages, buildCreateRequest(singular, lower, resourceFQN))
	}
	messages = append(messages,
		buildGetRequest(singular),
		buildListRequest(plural),
		buildListResponse(singular, plural, collectionID, resourceFQN),
	)
	if hasManagement {
		messages = append(messages,
			buildDeleteRequest(singular),
			buildResumeRequest(singular),
		)
	}

	pkgPath := strings.ReplaceAll(pkg, ".", "/")
	fdp := &descriptorpb.FileDescriptorProto{
		Name:        proto.String(fmt.Sprintf("dynamic/%s/%s_service.proto", pkgPath, lower)),
		Package:     proto.String(pkg),
		Syntax:      proto.String("proto3"),
		Dependency:  uniqueStrings(deps),
		MessageType: messages,
		Service: []*descriptorpb.ServiceDescriptorProto{
			buildService(singular, plural, pkg, hasManagement),
		},
	}

	fd, err := protodesc.NewFile(fdp, files)
	if err != nil {
		return nil, fmt.Errorf("build file descriptor: %w", err)
	}

	svc := fd.Services().ByName(protoreflect.Name(singular + "Service"))
	if svc == nil {
		return nil, fmt.Errorf("service %sService not found in built descriptor", singular)
	}

	out := &ExtensionServiceDescriptors{
		File:         fd,
		Service:      svc,
		Resource:     fd.Messages().ByName(protoreflect.Name(singular)),
		GetRequest:   fd.Messages().ByName(protoreflect.Name("Get" + singular + "Request")),
		ListRequest:  fd.Messages().ByName(protoreflect.Name("List" + plural + "Request")),
		ListResponse: fd.Messages().ByName(protoreflect.Name("List" + plural + "Response")),
	}
	if hasManagement {
		out.CreateRequest = fd.Messages().ByName(protoreflect.Name("Create" + singular + "Request"))
		out.DeleteRequest = fd.Messages().ByName(protoreflect.Name("Delete" + singular + "Request"))
		out.ResumeRequest = fd.Messages().ByName(protoreflect.Name("Resume" + singular + "Request"))
		out.Spec = specDesc
	}
	return out, nil
}

func buildResourceMessage(cfg *ResourceTypeConfig, singular, pkg, resourceFQN, specFullName, resourceStateEnumName string) *descriptorpb.DescriptorProto {
	msg := &descriptorpb.DescriptorProto{
		Name: proto.String(singular),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField("name", 1),
			dynamicapi.StringField("uid", 2),
		},
	}
	dynamicapi.AppendMapField(msg, dynamicapi.MapField(
		resourceFQN, "labels", 3,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		"",
	))
	msg.Field = append(msg.Field,
		dynamicapi.MessageField("create_time", 4, "google.protobuf.Timestamp"),
		dynamicapi.MessageField("update_time", 5, "google.protobuf.Timestamp"),
		dynamicapi.StringField("etag", 6),
	)

	if cfg.Capabilities.Management != nil {
		msg.EnumType = []*descriptorpb.EnumDescriptorProto{
			buildResourceStateEnum(resourceStateEnumName),
		}
		msg.Field = append(msg.Field,
			dynamicapi.MessageField("spec", 20, specFullName),
			dynamicapi.Int64Field("intent_version", 21),
			dynamicapi.EnumField("state", 22, pkg+"."+singular+"."+resourceStateEnumName),
			dynamicapi.BoolField("reconciling", 23),
			dynamicapi.StringField("pause_reason", 24),
			dynamicapi.Int64Field("generation", 25),
			dynamicapi.MessageField("provenance", 26, "fleetshift.v1.Provenance"),
			dynamicapi.MessageField("delete_time", 27, "google.protobuf.Timestamp"),
		)
	}

	if cfg.Capabilities.Inventory != nil {
		dynamicapi.AppendMapField(msg, dynamicapi.MapField(
			resourceFQN, "local_labels", 40,
			descriptorpb.FieldDescriptorProto_TYPE_STRING,
			descriptorpb.FieldDescriptorProto_TYPE_STRING,
			"",
		))
		dynamicapi.AppendMapField(msg, dynamicapi.MapField(
			resourceFQN, "conditions", 41,
			descriptorpb.FieldDescriptorProto_TYPE_STRING,
			descriptorpb.FieldDescriptorProto_TYPE_MESSAGE,
			"."+pkg+".Condition",
		))
		msg.Field = append(msg.Field,
			dynamicapi.MessageField("observation", 42, "google.protobuf.Struct"),
			dynamicapi.MessageField("local_update_time", 43, "google.protobuf.Timestamp"),
			dynamicapi.MessageField("index_update_time", 44, "google.protobuf.Timestamp"),
		)
	}

	return msg
}

// buildConditionMessage synthesizes the wire Condition message used as the
// value type of the conditions map. type is the map key, not a field here.
func buildConditionMessage() *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("Condition"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField("status", 1),
			dynamicapi.StringField("reason", 2),
			dynamicapi.StringField("message", 3),
			dynamicapi.MessageField("last_transition_time", 4, "google.protobuf.Timestamp"),
		},
	}
}

func buildCreateRequest(singular, lower, resourceFQN string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("Create" + singular + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField(lower+"_id", 1),
			dynamicapi.MessageField(lower, 2, resourceFQN),
			dynamicapi.BytesField("user_signature", 3),
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
			dynamicapi.BytesField("user_signature", 2),
			dynamicapi.MessageField("valid_until", 3, "google.protobuf.Timestamp"),
			dynamicapi.StringField("etag", 4),
			dynamicapi.Int64Field("expected_generation", 5),
		},
	}
}

func buildService(singular, plural, pkg string, hasManagement bool) *descriptorpb.ServiceDescriptorProto {
	fqnPrefix := "." + pkg + "."
	methods := make([]*descriptorpb.MethodDescriptorProto, 0, 5)
	if hasManagement {
		methods = append(methods, &descriptorpb.MethodDescriptorProto{
			Name:       proto.String("Create" + singular),
			InputType:  proto.String(fqnPrefix + "Create" + singular + "Request"),
			OutputType: proto.String(fqnPrefix + singular),
		})
	}
	methods = append(methods,
		&descriptorpb.MethodDescriptorProto{
			Name:       proto.String("Get" + singular),
			InputType:  proto.String(fqnPrefix + "Get" + singular + "Request"),
			OutputType: proto.String(fqnPrefix + singular),
		},
		&descriptorpb.MethodDescriptorProto{
			Name:       proto.String("List" + plural),
			InputType:  proto.String(fqnPrefix + "List" + plural + "Request"),
			OutputType: proto.String(fqnPrefix + "List" + plural + "Response"),
		},
	)
	if hasManagement {
		methods = append(methods,
			&descriptorpb.MethodDescriptorProto{
				Name:       proto.String("Delete" + singular),
				InputType:  proto.String(fqnPrefix + "Delete" + singular + "Request"),
				OutputType: proto.String(fqnPrefix + singular),
			},
			&descriptorpb.MethodDescriptorProto{
				Name:       proto.String("Resume" + singular),
				InputType:  proto.String(fqnPrefix + "Resume" + singular + "Request"),
				OutputType: proto.String(fqnPrefix + singular),
			},
		)
	}
	return &descriptorpb.ServiceDescriptorProto{
		Name:   proto.String(singular + "Service"),
		Method: methods,
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

func uniqueStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
