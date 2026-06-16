// Package dynamic provides a reflection-based gRPC client for
// interacting with dynamically registered managed resource services.
// It uses gRPC server reflection to discover available resource types
// and construct messages at runtime without compiled stubs.
package dynamic

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// Known static services that should be excluded from resource type
// discovery. These are core platform services, not addon-provided
// managed resource services.
var staticServices = map[string]bool{
	"fleetshift.v1.DeploymentService":          true,
	"fleetshift.v1.AuthMethodService":          true,
	"fleetshift.v1.SignerEnrollmentService":    true,
	"grpc.reflection.v1.ServerReflection":      true,
	"grpc.reflection.v1alpha.ServerReflection": true,
	"grpc.health.v1.Health":                    true,
}

// ResourceType holds metadata for a discovered managed resource service.
type ResourceType struct {
	ServiceName  string
	Singular     string
	Plural       string
	ProtoPackage string // versioned proto package (e.g. "kind.fleetshift.v1")
	CollectionID string // AIP collection identifier (e.g. "clusters")
}

// ResourceCollection returns the resource name prefix (e.g. "clusters/").
func (rt ResourceType) ResourceCollection() string {
	return rt.CollectionID + "/"
}

// Client wraps a gRPC connection and provides reflection-based
// discovery and dynamic invocation for managed resource services.
type Client struct {
	conn *grpc.ClientConn
}

// NewClient creates a dynamic client for the given connection.
func NewClient(conn *grpc.ClientConn) *Client {
	return &Client{conn: conn}
}

// ListResourceTypes discovers available managed resource services via
// gRPC reflection. It filters out known static services and detects
// dynamic resource services by descriptor shape: the service name ends
// in "Service" and has Create/Get/List/Delete methods following AIP
// naming conventions.
func (c *Client) ListResourceTypes(ctx context.Context) ([]ResourceType, error) {
	services, err := c.listServices(ctx)
	if err != nil {
		return nil, err
	}

	var types []ResourceType
	for _, svcName := range services {
		if staticServices[svcName] {
			continue
		}
		if !strings.HasSuffix(svcName, "Service") {
			continue
		}

		rt, err := c.probeResourceService(ctx, svcName)
		if err != nil {
			continue
		}
		if rt == nil {
			continue
		}
		types = append(types, *rt)
	}
	return types, nil
}

// probeResourceService uses reflection to determine whether a gRPC
// service is a managed resource service by checking its method shape.
// Returns nil if the service does not match the expected pattern.
func (c *Client) probeResourceService(ctx context.Context, svcName string) (*ResourceType, error) {
	descs, err := c.resolveServiceDescriptors(ctx, svcName)
	if err != nil {
		return nil, err
	}

	svcDesc := findService(descs, svcName)
	if svcDesc == nil {
		return nil, nil
	}

	// Extract proto package and singular from the service full name.
	lastDot := strings.LastIndex(svcName, ".")
	if lastDot < 0 {
		return nil, nil
	}
	protoPackage := svcName[:lastDot]
	localName := svcName[lastDot+1:]
	if !strings.HasSuffix(localName, "Service") {
		return nil, nil
	}
	singular := localName[:len(localName)-len("Service")]

	// Verify the service has the expected CRUD methods.
	methods := make(map[string]bool, svcDesc.Methods().Len())
	for i := range svcDesc.Methods().Len() {
		methods[string(svcDesc.Methods().Get(i).Name())] = true
	}

	if !methods["Create"+singular] || !methods["Get"+singular] || !methods["Delete"+singular] {
		return nil, nil
	}

	// Find the List method to derive Plural and CollectionID.
	var plural, collectionID string
	for name := range methods {
		if strings.HasPrefix(name, "List") && name != "List" {
			plural = name[len("List"):]
			break
		}
	}
	if plural == "" {
		return nil, nil
	}

	// Derive CollectionID from the List response's repeated field.
	listRespName := protoPackage + ".List" + plural + "Response"
	listRespDesc := findMessage(descs, listRespName)
	if listRespDesc != nil {
		for i := range listRespDesc.Fields().Len() {
			f := listRespDesc.Fields().Get(i)
			if f.IsList() && f.Message() != nil {
				collectionID = string(f.Name())
				break
			}
		}
	}
	if collectionID == "" {
		collectionID = strings.ToLower(plural[:1]) + plural[1:]
	}

	// Verify the resource message has a spec field.
	resourceMsgDesc := findMessage(descs, protoPackage+"."+singular)
	if resourceMsgDesc == nil {
		return nil, nil
	}
	if resourceMsgDesc.Fields().ByName("spec") == nil {
		return nil, nil
	}

	return &ResourceType{
		ServiceName:  svcName,
		Singular:     singular,
		Plural:       plural,
		ProtoPackage: protoPackage,
		CollectionID: collectionID,
	}, nil
}

// FieldSchema describes a single field in a protobuf message,
// including nested message structure recursively.
type FieldSchema struct {
	Name     string
	Type     string
	Number   int
	Repeated bool
	Optional bool
	MapKey   *FieldSchema
	MapValue *FieldSchema
	Fields   []FieldSchema // populated for message-typed fields
}

// MessageSchema describes a protobuf message with its full nested
// field tree.
type MessageSchema struct {
	FullName string
	Fields   []FieldSchema
}

// SchemaInfo holds the described schema for a managed resource type.
type SchemaInfo struct {
	ResourceType ResourceType
	Spec         *MessageSchema
	Methods      []string
}

// Describe returns the schema information for a resource type,
// including the full recursive spec message schema and available
// RPC methods.
func (c *Client) Describe(ctx context.Context, rt ResourceType) (*SchemaInfo, error) {
	descs, err := c.resolveServiceDescriptors(ctx, rt.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("resolve descriptors: %w", err)
	}

	svcDesc := findService(descs, rt.ServiceName)
	if svcDesc == nil {
		return nil, fmt.Errorf("service %s not found in descriptors", rt.ServiceName)
	}

	info := &SchemaInfo{ResourceType: rt}

	for i := range svcDesc.Methods().Len() {
		info.Methods = append(info.Methods, string(svcDesc.Methods().Get(i).Name()))
	}

	resourceMsgDesc := findMessage(descs, rt.ProtoPackage+"."+rt.Singular)
	if resourceMsgDesc == nil {
		return info, nil
	}

	specField := resourceMsgDesc.Fields().ByName("spec")
	if specField == nil || specField.Message() == nil {
		return info, nil
	}

	info.Spec = describeMessage(specField.Message(), nil)
	return info, nil
}

func describeMessage(md protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) *MessageSchema {
	if seen == nil {
		seen = make(map[protoreflect.FullName]bool)
	}
	if seen[md.FullName()] {
		return &MessageSchema{FullName: string(md.FullName())}
	}
	seen[md.FullName()] = true

	schema := &MessageSchema{FullName: string(md.FullName())}
	for i := range md.Fields().Len() {
		f := md.Fields().Get(i)
		schema.Fields = append(schema.Fields, describeField(f, seen))
	}
	return schema
}

func describeField(f protoreflect.FieldDescriptor, seen map[protoreflect.FullName]bool) FieldSchema {
	fs := FieldSchema{
		Name:     string(f.Name()),
		Type:     fieldTypeName(f),
		Number:   int(f.Number()),
		Repeated: f.IsList(),
		Optional: f.HasOptionalKeyword(),
	}

	if f.IsMap() {
		fs.Repeated = false
		keyField := f.MapKey()
		valField := f.MapValue()
		key := describeField(keyField, seen)
		val := describeField(valField, seen)
		fs.MapKey = &key
		fs.MapValue = &val
		fs.Type = fmt.Sprintf("map<%s, %s>", key.Type, val.Type)
		return fs
	}

	if f.Message() != nil && !isWellKnown(f.Message().FullName()) {
		nested := describeMessage(f.Message(), seen)
		fs.Fields = nested.Fields
	}

	return fs
}

func fieldTypeName(f protoreflect.FieldDescriptor) string {
	if f.Message() != nil {
		return string(f.Message().FullName())
	}
	if f.Enum() != nil {
		return string(f.Enum().FullName())
	}
	return f.Kind().String()
}

// isWellKnown returns true for standard protobuf wrapper and utility
// types that should be shown as leaf types rather than recursed into.
func isWellKnown(name protoreflect.FullName) bool {
	switch name {
	case "google.protobuf.Timestamp",
		"google.protobuf.Duration",
		"google.protobuf.Any",
		"google.protobuf.Struct",
		"google.protobuf.Value",
		"google.protobuf.ListValue",
		"google.protobuf.FieldMask",
		"google.protobuf.Empty",
		"google.protobuf.StringValue",
		"google.protobuf.BytesValue",
		"google.protobuf.Int32Value",
		"google.protobuf.Int64Value",
		"google.protobuf.UInt32Value",
		"google.protobuf.UInt64Value",
		"google.protobuf.FloatValue",
		"google.protobuf.DoubleValue",
		"google.protobuf.BoolValue":
		return true
	}
	return false
}

// Create invokes the Create RPC for the given resource type.
func (c *Client) Create(ctx context.Context, rt ResourceType, id string, specJSON []byte) (proto.Message, error) {
	descs, err := c.resolveServiceDescriptors(ctx, rt.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("resolve descriptors: %w", err)
	}

	svcDesc := findService(descs, rt.ServiceName)
	if svcDesc == nil {
		return nil, fmt.Errorf("service %s not found in descriptors", rt.ServiceName)
	}

	resourceMsgDesc := findMessage(descs, rt.ProtoPackage+"."+rt.Singular)
	if resourceMsgDesc == nil {
		return nil, fmt.Errorf("resource message %s.%s not found", rt.ProtoPackage, rt.Singular)
	}

	specField := resourceMsgDesc.Fields().ByName("spec")
	if specField == nil {
		return nil, fmt.Errorf("spec field not found in resource message")
	}
	specMsgDesc := specField.Message()

	spec := dynamicpb.NewMessage(specMsgDesc)
	if err := protojson.Unmarshal(specJSON, spec); err != nil {
		return nil, fmt.Errorf("parse spec JSON: %w", err)
	}

	resource := dynamicpb.NewMessage(resourceMsgDesc)
	resource.Set(specField, protoreflect.ValueOfMessage(spec))

	createReqName := rt.ProtoPackage + ".Create" + rt.Singular + "Request"
	createReqDesc := findMessage(descs, createReqName)
	if createReqDesc == nil {
		return nil, fmt.Errorf("create request message %s not found", createReqName)
	}

	req := dynamicpb.NewMessage(createReqDesc)
	req.Set(createReqDesc.Fields().ByNumber(1), protoreflect.ValueOfString(id))
	req.Set(createReqDesc.Fields().ByNumber(2), protoreflect.ValueOfMessage(resource))

	resp := dynamicpb.NewMessage(resourceMsgDesc)
	method := "/" + rt.ServiceName + "/Create" + rt.Singular
	if err := c.conn.Invoke(ctx, method, req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// Get invokes the Get RPC for the given resource type and id.
func (c *Client) Get(ctx context.Context, rt ResourceType, id string) (proto.Message, error) {
	descs, err := c.resolveServiceDescriptors(ctx, rt.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("resolve descriptors: %w", err)
	}

	resourceMsgDesc := findMessage(descs, rt.ProtoPackage+"."+rt.Singular)
	if resourceMsgDesc == nil {
		return nil, fmt.Errorf("resource message %s.%s not found", rt.ProtoPackage, rt.Singular)
	}

	getReqName := rt.ProtoPackage + ".Get" + rt.Singular + "Request"
	getReqDesc := findMessage(descs, getReqName)
	if getReqDesc == nil {
		return nil, fmt.Errorf("get request message %s not found", getReqName)
	}

	req := dynamicpb.NewMessage(getReqDesc)
	nameField := getReqDesc.Fields().ByName("name")
	req.Set(nameField, protoreflect.ValueOfString(rt.ResourceCollection()+id))

	resp := dynamicpb.NewMessage(resourceMsgDesc)
	method := "/" + rt.ServiceName + "/Get" + rt.Singular
	if err := c.conn.Invoke(ctx, method, req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// List invokes the List RPC for the given resource type.
func (c *Client) List(ctx context.Context, rt ResourceType, pageSize int32) ([]proto.Message, error) {
	descs, err := c.resolveServiceDescriptors(ctx, rt.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("resolve descriptors: %w", err)
	}

	listReqName := rt.ProtoPackage + ".List" + rt.Plural + "Request"
	listReqDesc := findMessage(descs, listReqName)
	if listReqDesc == nil {
		return nil, fmt.Errorf("list request message %s not found", listReqName)
	}

	listRespName := rt.ProtoPackage + ".List" + rt.Plural + "Response"
	listRespDesc := findMessage(descs, listRespName)
	if listRespDesc == nil {
		return nil, fmt.Errorf("list response message %s not found", listRespName)
	}

	req := dynamicpb.NewMessage(listReqDesc)
	if pageSize > 0 {
		if f := listReqDesc.Fields().ByName("page_size"); f != nil {
			req.Set(f, protoreflect.ValueOfInt32(pageSize))
		}
	}

	resp := dynamicpb.NewMessage(listRespDesc)
	method := "/" + rt.ServiceName + "/List" + rt.Plural
	if err := c.conn.Invoke(ctx, method, req, resp); err != nil {
		return nil, err
	}

	resourcesField := listRespDesc.Fields().ByName(protoreflect.Name(rt.CollectionID))
	if resourcesField == nil {
		return nil, fmt.Errorf("resources field %q not found in list response", rt.CollectionID)
	}

	list := resp.Get(resourcesField).List()
	msgs := make([]proto.Message, list.Len())
	for i := range list.Len() {
		msgs[i] = list.Get(i).Message().Interface()
	}
	return msgs, nil
}

// Delete invokes the Delete RPC for the given resource type and id.
func (c *Client) Delete(ctx context.Context, rt ResourceType, id string) (proto.Message, error) {
	descs, err := c.resolveServiceDescriptors(ctx, rt.ServiceName)
	if err != nil {
		return nil, fmt.Errorf("resolve descriptors: %w", err)
	}

	resourceMsgDesc := findMessage(descs, rt.ProtoPackage+"."+rt.Singular)
	if resourceMsgDesc == nil {
		return nil, fmt.Errorf("resource message %s.%s not found", rt.ProtoPackage, rt.Singular)
	}

	deleteReqName := rt.ProtoPackage + ".Delete" + rt.Singular + "Request"
	deleteReqDesc := findMessage(descs, deleteReqName)
	if deleteReqDesc == nil {
		return nil, fmt.Errorf("delete request message %s not found", deleteReqName)
	}

	req := dynamicpb.NewMessage(deleteReqDesc)
	nameField := deleteReqDesc.Fields().ByName("name")
	req.Set(nameField, protoreflect.ValueOfString(rt.ResourceCollection()+id))

	resp := dynamicpb.NewMessage(resourceMsgDesc)
	method := "/" + rt.ServiceName + "/Delete" + rt.Singular
	if err := c.conn.Invoke(ctx, method, req, resp); err != nil {
		return nil, err
	}
	return resp, nil
}

// ResolveType finds a resource type by collection ID (or plural) among
// the available types. If service is non-empty, it is used to
// disambiguate when multiple services expose the same collection. If
// service is empty and the collection is ambiguous, an error listing
// the candidates is returned.
func (c *Client) ResolveType(ctx context.Context, collection string, service string) (ResourceType, error) {
	types, err := c.ListResourceTypes(ctx)
	if err != nil {
		return ResourceType{}, err
	}

	var matches []ResourceType
	for _, rt := range types {
		if strings.EqualFold(rt.CollectionID, collection) || strings.EqualFold(rt.Plural, collection) {
			if service != "" && rt.ServiceName != service {
				continue
			}
			matches = append(matches, rt)
		}
	}

	switch len(matches) {
	case 0:
		return ResourceType{}, fmt.Errorf("unknown resource type %q", collection)
	case 1:
		return matches[0], nil
	default:
		var candidates []string
		for _, rt := range matches {
			candidates = append(candidates, rt.ServiceName)
		}
		return ResourceType{}, fmt.Errorf(
			"ambiguous collection %q: multiple services match (%s); use --service to disambiguate",
			collection, strings.Join(candidates, ", "),
		)
	}
}

func (c *Client) listServices(ctx context.Context) ([]string, error) {
	client := reflectionpb.NewServerReflectionClient(c.conn)
	stream, err := client.ServerReflectionInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("reflection info: %w", err)
	}
	defer stream.CloseSend()

	if err := stream.Send(&reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{
			ListServices: "",
		},
	}); err != nil {
		return nil, fmt.Errorf("send list services: %w", err)
	}

	resp, err := stream.Recv()
	if err != nil {
		return nil, fmt.Errorf("recv list services: %w", err)
	}

	listResp := resp.GetListServicesResponse()
	if listResp == nil {
		return nil, fmt.Errorf("unexpected response type: %T", resp.GetMessageResponse())
	}

	var names []string
	for _, svc := range listResp.GetService() {
		names = append(names, svc.GetName())
	}
	return names, nil
}

// resolveServiceDescriptors fetches the file descriptor(s) for a
// service via reflection and builds a local file registry.
func (c *Client) resolveServiceDescriptors(ctx context.Context, serviceName string) (*protoregistry.Files, error) {
	client := reflectionpb.NewServerReflectionClient(c.conn)
	stream, err := client.ServerReflectionInfo(ctx)
	if err != nil {
		return nil, err
	}
	defer stream.CloseSend()

	if err := stream.Send(&reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_FileContainingSymbol{
			FileContainingSymbol: serviceName,
		},
	}); err != nil {
		return nil, err
	}

	resp, err := stream.Recv()
	if err != nil {
		return nil, err
	}

	fdResp := resp.GetFileDescriptorResponse()
	if fdResp == nil {
		if errResp := resp.GetErrorResponse(); errResp != nil {
			return nil, fmt.Errorf("reflection error: %s", errResp.GetErrorMessage())
		}
		return nil, fmt.Errorf("unexpected response type: %T", resp.GetMessageResponse())
	}

	return buildFileRegistry(fdResp.GetFileDescriptorProto())
}

func buildFileRegistry(rawFDs [][]byte) (*protoregistry.Files, error) {
	reg := new(protoregistry.Files)

	// Parse all raw descriptors first.
	var fdps []*descriptorpb.FileDescriptorProto
	for _, raw := range rawFDs {
		fdp := new(descriptorpb.FileDescriptorProto)
		if err := proto.Unmarshal(raw, fdp); err != nil {
			return nil, fmt.Errorf("unmarshal file descriptor: %w", err)
		}
		fdps = append(fdps, fdp)
	}

	// Register iteratively: some files depend on others in the same
	// batch. Loop until no more progress can be made. This handles
	// arbitrary ordering of the descriptors.
	registered := make(map[string]bool, len(fdps))
	resolver := compositeResolver{local: reg}

	for range len(fdps) + 1 {
		progress := false
		for _, fdp := range fdps {
			path := fdp.GetName()
			if registered[path] {
				continue
			}
			if _, err := protoregistry.GlobalFiles.FindFileByPath(path); err == nil {
				registered[path] = true
				progress = true
				continue
			}
			if _, err := reg.FindFileByPath(path); err == nil {
				registered[path] = true
				progress = true
				continue
			}

			fd, err := protodesc.NewFile(fdp, resolver)
			if err != nil {
				// Dependency not yet available — will retry next pass.
				continue
			}
			if err := reg.RegisterFile(fd); err != nil {
				continue
			}
			registered[path] = true
			progress = true
		}
		if !progress {
			break
		}
	}

	return reg, nil
}

type compositeResolver struct {
	local *protoregistry.Files
}

func (r compositeResolver) FindFileByPath(path string) (protoreflect.FileDescriptor, error) {
	if fd, err := r.local.FindFileByPath(path); err == nil {
		return fd, nil
	}
	return protoregistry.GlobalFiles.FindFileByPath(path)
}

func (r compositeResolver) FindDescriptorByName(name protoreflect.FullName) (protoreflect.Descriptor, error) {
	if d, err := r.local.FindDescriptorByName(name); err == nil {
		return d, nil
	}
	return protoregistry.GlobalFiles.FindDescriptorByName(name)
}

func findService(files *protoregistry.Files, fullName string) protoreflect.ServiceDescriptor {
	var found protoreflect.ServiceDescriptor
	files.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		for i := range fd.Services().Len() {
			svc := fd.Services().Get(i)
			if string(svc.FullName()) == fullName {
				found = svc
				return false
			}
		}
		return true
	})
	return found
}

func findMessage(files *protoregistry.Files, fullName string) protoreflect.MessageDescriptor {
	desc, err := files.FindDescriptorByName(protoreflect.FullName(fullName))
	if err != nil {
		return nil
	}
	msgDesc, ok := desc.(protoreflect.MessageDescriptor)
	if !ok {
		return nil
	}
	return msgDesc
}
