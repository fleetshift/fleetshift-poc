package platformresource

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

// ServiceDescriptors holds the compiled descriptors for a
// dynamically-built platform-canonical resource service. Unlike
// extension [managedresource.ServiceDescriptors] (which targets
// extension APIs with a spec envelope), these descriptors model the
// platform's generic resource shape — identity, labels,
// representations, aliases, and relationships.
type ServiceDescriptors struct {
	// File is the synthesized file descriptor containing all messages and service.
	File protoreflect.FileDescriptor

	// Service is the service descriptor (e.g. PlatformClusterService).
	Service protoreflect.ServiceDescriptor

	// Resource is the resource message descriptor (e.g. PlatformCluster).
	Resource protoreflect.MessageDescriptor

	// CreateRequest is the create request message descriptor.
	CreateRequest protoreflect.MessageDescriptor

	// GetRequest is the get request message descriptor.
	GetRequest protoreflect.MessageDescriptor

	// ListRequest is the list request message descriptor.
	ListRequest protoreflect.MessageDescriptor

	// ListResponse is the list response message descriptor.
	ListResponse protoreflect.MessageDescriptor
}

// BuildServiceDescriptors programmatically constructs the full
// set of proto descriptors for a platform-canonical AIP-compliant
// resource service. Given a [Config], it builds:
//   - The platform resource message (identity, labels, representations, aliases, relationships)
//   - Helper messages (representation, alias, relationship)
//   - Create/Get/List request and response messages
//   - The service definition with all methods
//
// The resulting descriptors are used to instantiate dynamicpb.Message
// instances for gRPC marshaling at runtime.
func BuildServiceDescriptors(cfg *Config) (*ServiceDescriptors, error) {
	if cfg == nil {
		return nil, fmt.Errorf("platform resource config is required")
	}
	if cfg.Singular == "" || cfg.Plural == "" || cfg.CollectionID == "" {
		return nil, fmt.Errorf("singular, plural, and collection ID are required")
	}
	if cfg.Singular[0] < 'A' || cfg.Singular[0] > 'Z' {
		return nil, fmt.Errorf("singular %q must start with an uppercase letter (PascalCase)", cfg.Singular)
	}
	if cfg.Plural[0] < 'A' || cfg.Plural[0] > 'Z' {
		return nil, fmt.Errorf("plural %q must start with an uppercase letter (PascalCase)", cfg.Plural)
	}

	singular := cfg.Singular
	lower := strings.ToLower(singular[:1]) + singular[1:]
	plural := cfg.Plural
	collectionID := cfg.CollectionID
	pkg := ProtoPackage
	resourceName := "Platform" + singular

	fqn := func(name string) string { return pkg + "." + name }

	repMsg := buildRepresentationMessage(resourceName)
	aliasMsg := buildAliasMessage(resourceName)
	relMsg := buildRelationshipMessage(resourceName)

	resourceMsg := buildResourceMessage(resourceName, pkg, repMsg, aliasMsg, relMsg)

	fdp := &descriptorpb.FileDescriptorProto{
		Name:       proto.String(fmt.Sprintf("dynamic/fleetshift/v1/platform_%s_service.proto", lower)),
		Package:    proto.String(pkg),
		Syntax:     proto.String("proto3"),
		Dependency: []string{"google/protobuf/timestamp.proto"},
		MessageType: []*descriptorpb.DescriptorProto{
			resourceMsg,
			repMsg,
			aliasMsg,
			relMsg,
			buildCreateRequest(resourceName, lower, fqn(resourceName)),
			buildGetRequest(resourceName),
			buildListRequest(resourceName, plural),
			buildListResponse(resourceName, plural, collectionID, fqn(resourceName)),
		},
		Service: []*descriptorpb.ServiceDescriptorProto{
			buildService(resourceName, plural, pkg),
		},
	}

	files := new(protoregistry.Files)

	tsFile, err := protoregistry.GlobalFiles.FindFileByPath("google/protobuf/timestamp.proto")
	if err != nil {
		return nil, fmt.Errorf("find timestamp.proto: %w", err)
	}
	if err := dynamicapi.RegisterFileAndDeps(files, tsFile); err != nil {
		return nil, fmt.Errorf("register timestamp deps: %w", err)
	}

	fd, err := protodesc.NewFile(fdp, files)
	if err != nil {
		return nil, fmt.Errorf("build file descriptor: %w", err)
	}

	svcName := protoreflect.Name(resourceName + "Service")
	svc := fd.Services().ByName(svcName)
	if svc == nil {
		return nil, fmt.Errorf("service %s not found in built descriptor", svcName)
	}

	return &ServiceDescriptors{
		File:          fd,
		Service:       svc,
		Resource:      fd.Messages().ByName(protoreflect.Name(resourceName)),
		CreateRequest: fd.Messages().ByName(protoreflect.Name("Create" + resourceName + "Request")),
		GetRequest:    fd.Messages().ByName(protoreflect.Name("Get" + resourceName + "Request")),
		ListRequest:   fd.Messages().ByName(protoreflect.Name("List" + "Platform" + plural + "Request")),
		ListResponse:  fd.Messages().ByName(protoreflect.Name("List" + "Platform" + plural + "Response")),
	}, nil
}

func buildResourceMessage(
	resourceName, pkg string,
	repMsg, aliasMsg, relMsg *descriptorpb.DescriptorProto,
) *descriptorpb.DescriptorProto {
	parentFQN := pkg + "." + resourceName

	labelsField, labelsEntry := buildMapStringStringField(parentFQN, "labels", "LabelsEntry", 3)
	effectiveLabelsField, effectiveLabelsEntry := buildMapStringStringField(parentFQN, "effective_labels", "EffectiveLabelsEntry", 4)

	return &descriptorpb.DescriptorProto{
		Name: proto.String(resourceName),
		NestedType: []*descriptorpb.DescriptorProto{
			labelsEntry,
			effectiveLabelsEntry,
		},
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField("name", 1),
			dynamicapi.StringField("uid", 2),
			labelsField,
			effectiveLabelsField,
			dynamicapi.RepeatedMessageField("representations", 5, pkg+"."+repMsg.GetName()),
			dynamicapi.RepeatedMessageField("aliases", 6, pkg+"."+aliasMsg.GetName()),
			dynamicapi.RepeatedMessageField("relationships", 7, pkg+"."+relMsg.GetName()),
			dynamicapi.MessageField("create_time", 8, "google.protobuf.Timestamp"),
			dynamicapi.MessageField("update_time", 9, "google.protobuf.Timestamp"),
		},
	}
}

func buildRepresentationMessage(resourceName string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String(resourceName + "Representation"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField("service_name", 1),
			dynamicapi.StringField("version", 2),
			dynamicapi.StringField("full_resource_name", 3),
			repeatedStringField("roles", 4),
			dynamicapi.MessageField("create_time", 5, "google.protobuf.Timestamp"),
			dynamicapi.MessageField("update_time", 6, "google.protobuf.Timestamp"),
		},
	}
}

func buildAliasMessage(resourceName string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String(resourceName + "Alias"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField("namespace", 1),
			dynamicapi.StringField("key", 2),
			dynamicapi.StringField("value", 3),
		},
	}
}

func buildRelationshipMessage(resourceName string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String(resourceName + "Relationship"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField("type", 1),
			dynamicapi.StringField("target_uid", 2),
			dynamicapi.StringField("source_service", 3),
			dynamicapi.MessageField("create_time", 4, "google.protobuf.Timestamp"),
		},
	}
}

func buildCreateRequest(resourceName, lower, resourceFQN string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("Create" + resourceName + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField(lower+"_id", 1),
			dynamicapi.MessageField("platform_"+lower, 2, resourceFQN),
		},
	}
}

func buildGetRequest(resourceName string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("Get" + resourceName + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField("name", 1),
		},
	}
}

func buildListRequest(resourceName, plural string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("List" + "Platform" + plural + "Request"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.Int32Field("page_size", 1),
			dynamicapi.StringField("page_token", 2),
		},
	}
}

func buildListResponse(resourceName, plural, collectionID, resourceFQN string) *descriptorpb.DescriptorProto {
	return &descriptorpb.DescriptorProto{
		Name: proto.String("List" + "Platform" + plural + "Response"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.RepeatedMessageField(collectionID, 1, resourceFQN),
			dynamicapi.StringField("next_page_token", 2),
		},
	}
}

func buildService(resourceName, plural, pkg string) *descriptorpb.ServiceDescriptorProto {
	fqnPrefix := "." + pkg + "."
	return &descriptorpb.ServiceDescriptorProto{
		Name: proto.String(resourceName + "Service"),
		Method: []*descriptorpb.MethodDescriptorProto{
			{
				Name:       proto.String("Create" + resourceName),
				InputType:  proto.String(fqnPrefix + "Create" + resourceName + "Request"),
				OutputType: proto.String(fqnPrefix + resourceName),
			},
			{
				Name:       proto.String("Get" + resourceName),
				InputType:  proto.String(fqnPrefix + "Get" + resourceName + "Request"),
				OutputType: proto.String(fqnPrefix + resourceName),
			},
			{
				Name:       proto.String("List" + "Platform" + plural),
				InputType:  proto.String(fqnPrefix + "List" + "Platform" + plural + "Request"),
				OutputType: proto.String(fqnPrefix + "List" + "Platform" + plural + "Response"),
			},
		},
	}
}

// --- platform-specific field helpers ---

func repeatedStringField(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
	}
}

func buildMapStringStringField(parentFQN, fieldName, entryName string, number int32) (*descriptorpb.FieldDescriptorProto, *descriptorpb.DescriptorProto) {
	entry := &descriptorpb.DescriptorProto{
		Name: proto.String(entryName),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField("key", 1),
			dynamicapi.StringField("value", 2),
		},
		Options: &descriptorpb.MessageOptions{
			MapEntry: proto.Bool(true),
		},
	}
	field := &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(fieldName),
		Number:   proto.Int32(number),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
		TypeName: proto.String("." + parentFQN + "." + entryName),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
	}
	return field, entry
}
