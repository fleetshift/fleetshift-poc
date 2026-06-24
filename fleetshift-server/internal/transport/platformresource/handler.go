package platformresource

import (
	"context"
	"fmt"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

// RegisteredService is a fully built dynamic gRPC service for
// the platform-canonical API, ready to be registered on a
// [dynamicapi.DynamicServiceMux]. It parallels
// [managedresource.RegisteredService] for extension APIs.
type RegisteredService struct {
	Desc        *grpc.ServiceDesc
	Descriptors *ServiceDescriptors
	Config      *Config
}

// Deps holds the shared dependencies injected into platform
// service handlers.
type Deps struct {
	Resources *application.PlatformResourceService
}

// BuildService constructs a dynamic gRPC service for the
// platform-canonical resource API. It does not register the service —
// the caller is responsible for wiring it into the mux.
func BuildService(cfg *Config, deps Deps) (*RegisteredService, error) {
	descs, err := BuildServiceDescriptors(cfg)
	if err != nil {
		return nil, fmt.Errorf("build platform descriptors for %s: %w", cfg.Singular, err)
	}

	handler := &platformHandler{
		cfg:       cfg,
		descs:     descs,
		resources: deps.Resources,
	}

	singular := cfg.Singular
	plural := cfg.Plural
	prefix := "Platform" + singular

	grpcDesc := &grpc.ServiceDesc{
		ServiceName: cfg.GRPCServiceName(),
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Create" + prefix,
				Handler:    handler.handleCreate,
			},
			{
				MethodName: "Get" + prefix,
				Handler:    handler.handleGet,
			},
			{
				MethodName: "ListPlatform" + plural,
				Handler:    handler.handleList,
			},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "dynamic/fleetshift/v1/platform_" + strings.ToLower(singular[:1]) + singular[1:] + "_service.proto",
	}

	return &RegisteredService{
		Desc:        grpcDesc,
		Descriptors: descs,
		Config:      cfg,
	}, nil
}

// platformHandler implements the gRPC method handler closures for the
// platform-canonical resource API.
type platformHandler struct {
	cfg       *Config
	descs     *ServiceDescriptors
	resources *application.PlatformResourceService
}

func (h *platformHandler) handleCreate(
	_ any,
	ctx context.Context,
	dec func(any) error,
	interceptor grpc.UnaryServerInterceptor,
) (any, error) {
	req := dynamicpb.NewMessage(h.descs.CreateRequest)
	if err := dec(req); err != nil {
		return nil, err
	}

	if interceptor != nil {
		info := &grpc.UnaryServerInfo{
			FullMethod: "/" + h.cfg.GRPCServiceName() + "/CreatePlatform" + h.cfg.Singular,
		}
		return interceptor(ctx, req, info, func(ctx context.Context, r any) (any, error) {
			return h.doCreate(ctx, r.(proto.Message))
		})
	}
	return h.doCreate(ctx, req)
}

func (h *platformHandler) doCreate(ctx context.Context, req proto.Message) (proto.Message, error) {
	reqMsg := req.ProtoReflect()

	idField := h.descs.CreateRequest.Fields().ByNumber(1)
	id := reqMsg.Get(idField).String()
	if id == "" {
		return nil, status.Errorf(codes.InvalidArgument, "%s is required", idField.Name())
	}

	var labels map[string]string
	resourceField := h.descs.CreateRequest.Fields().ByNumber(2)
	if reqMsg.Has(resourceField) {
		resourceMsg := reqMsg.Get(resourceField).Message()
		labelsField := h.descs.Resource.Fields().ByName("labels")
		if labelsField != nil {
			labels = extractMapStringString(resourceMsg, labelsField)
		}
	}

	resourceName, err := domain.NewResourceName(
		domain.NewCollectionName(domain.CollectionID(h.cfg.CollectionID)),
		domain.ResourceID(id),
	)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid resource name: %v", err)
	}

	pr, err := h.resources.Create(ctx, application.CreatePlatformResourceInput{
		Name:   resourceName,
		Labels: labels,
	})
	if err != nil {
		return nil, dynamicapi.ToStatusError(err)
	}

	return h.resourceToMessage(pr)
}

func (h *platformHandler) handleGet(
	_ any,
	ctx context.Context,
	dec func(any) error,
	interceptor grpc.UnaryServerInterceptor,
) (any, error) {
	req := dynamicpb.NewMessage(h.descs.GetRequest)
	if err := dec(req); err != nil {
		return nil, err
	}

	if interceptor != nil {
		info := &grpc.UnaryServerInfo{
			FullMethod: "/" + h.cfg.GRPCServiceName() + "/GetPlatform" + h.cfg.Singular,
		}
		return interceptor(ctx, req, info, func(ctx context.Context, r any) (any, error) {
			return h.doGet(ctx, r.(proto.Message))
		})
	}
	return h.doGet(ctx, req)
}

func (h *platformHandler) doGet(ctx context.Context, req proto.Message) (proto.Message, error) {
	nameField := h.descs.GetRequest.Fields().ByName("name")
	name := req.ProtoReflect().Get(nameField).String()

	resourceName, err := h.parseResourceName(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid name: %v", err)
	}

	pr, err := h.resources.Get(ctx, resourceName)
	if err != nil {
		return nil, dynamicapi.ToStatusError(err)
	}

	return h.resourceToMessage(pr)
}

func (h *platformHandler) handleList(
	_ any,
	ctx context.Context,
	dec func(any) error,
	interceptor grpc.UnaryServerInterceptor,
) (any, error) {
	req := dynamicpb.NewMessage(h.descs.ListRequest)
	if err := dec(req); err != nil {
		return nil, err
	}

	if interceptor != nil {
		info := &grpc.UnaryServerInfo{
			FullMethod: "/" + h.cfg.GRPCServiceName() + "/ListPlatform" + h.cfg.Plural,
		}
		return interceptor(ctx, req, info, func(ctx context.Context, r any) (any, error) {
			return h.doList(ctx, r.(proto.Message))
		})
	}
	return h.doList(ctx, req)
}

func (h *platformHandler) doList(ctx context.Context, _ proto.Message) (proto.Message, error) {
	collection := domain.NewCollectionName(domain.CollectionID(h.cfg.CollectionID))

	resources, err := h.resources.List(ctx, collection)
	if err != nil {
		return nil, dynamicapi.ToStatusError(err)
	}

	resp := dynamicpb.NewMessage(h.descs.ListResponse)
	resourcesField := h.descs.ListResponse.Fields().ByNumber(1)
	list := resp.Mutable(resourcesField).List()
	for _, pr := range resources {
		msg, err := h.resourceToMessage(pr)
		if err != nil {
			return nil, err
		}
		list.Append(protoreflect.ValueOfMessage(msg.ProtoReflect()))
	}

	return resp, nil
}

func (h *platformHandler) parseResourceName(name string) (domain.ResourceName, error) {
	collection := h.cfg.Collection()
	id, ok := strings.CutPrefix(name, collection)
	if !ok || id == "" {
		return "", fmt.Errorf("name must have format %s{id}", collection)
	}
	return domain.NewResourceName(
		domain.NewCollectionName(domain.CollectionID(h.cfg.CollectionID)),
		domain.ResourceID(id),
	)
}

// resourceToMessage converts a domain PlatformResource to a dynamic
// proto message. This is deliberately separate from the extension
// viewToResource — the field sets and semantics are different enough
// that sharing would obscure bugs.
func (h *platformHandler) resourceToMessage(pr *domain.PlatformResource) (proto.Message, error) {
	msg := dynamicpb.NewMessage(h.descs.Resource)

	nameField := h.descs.Resource.Fields().ByName("name")
	msg.Set(nameField, protoreflect.ValueOfString(string(pr.Name())))

	uidField := h.descs.Resource.Fields().ByName("uid")
	msg.Set(uidField, protoreflect.ValueOfString(pr.UID().String()))

	labelsField := h.descs.Resource.Fields().ByName("labels")
	setMapStringString(msg, labelsField, pr.Labels())

	effectiveLabelsField := h.descs.Resource.Fields().ByName("effective_labels")
	setMapStringString(msg, effectiveLabelsField, pr.EffectiveLabels())

	repsField := h.descs.Resource.Fields().ByName("representations")
	repList := msg.Mutable(repsField).List()
	repDesc := repsField.Message()
	for _, rep := range pr.Representations() {
		repMsg := dynamicpb.NewMessage(repDesc)
		repMsg.Set(repDesc.Fields().ByName("service_name"), protoreflect.ValueOfString(string(rep.ServiceName())))
		repMsg.Set(repDesc.Fields().ByName("version"), protoreflect.ValueOfString(string(rep.Version())))
		repMsg.Set(repDesc.Fields().ByName("full_resource_name"), protoreflect.ValueOfString(string(rep.FullResourceName())))
		rolesField := repDesc.Fields().ByName("roles")
		rolesList := repMsg.Mutable(rolesField).List()
		for _, role := range rep.Roles() {
			rolesList.Append(protoreflect.ValueOfString(string(role)))
		}
		if !rep.CreatedAt().IsZero() {
			if tsVal, err := dynamicapi.MarshalTimestamp(repDesc.Fields().ByName("create_time"), rep.CreatedAt()); err == nil {
				repMsg.Set(repDesc.Fields().ByName("create_time"), tsVal)
			}
		}
		if !rep.UpdatedAt().IsZero() {
			if tsVal, err := dynamicapi.MarshalTimestamp(repDesc.Fields().ByName("update_time"), rep.UpdatedAt()); err == nil {
				repMsg.Set(repDesc.Fields().ByName("update_time"), tsVal)
			}
		}
		repList.Append(protoreflect.ValueOfMessage(repMsg))
	}

	aliasesField := h.descs.Resource.Fields().ByName("aliases")
	aliasList := msg.Mutable(aliasesField).List()
	aliasDesc := aliasesField.Message()
	for _, alias := range pr.Aliases() {
		aliasMsg := dynamicpb.NewMessage(aliasDesc)
		aliasMsg.Set(aliasDesc.Fields().ByName("namespace"), protoreflect.ValueOfString(string(alias.Namespace)))
		aliasMsg.Set(aliasDesc.Fields().ByName("key"), protoreflect.ValueOfString(string(alias.Key)))
		aliasMsg.Set(aliasDesc.Fields().ByName("value"), protoreflect.ValueOfString(string(alias.Value)))
		aliasList.Append(protoreflect.ValueOfMessage(aliasMsg))
	}

	relsField := h.descs.Resource.Fields().ByName("relationships")
	relList := msg.Mutable(relsField).List()
	relDesc := relsField.Message()
	for _, rel := range pr.Relationships() {
		relMsg := dynamicpb.NewMessage(relDesc)
		relMsg.Set(relDesc.Fields().ByName("type"), protoreflect.ValueOfString(string(rel.Type())))
		relMsg.Set(relDesc.Fields().ByName("target_uid"), protoreflect.ValueOfString(rel.TargetUID().String()))
		relMsg.Set(relDesc.Fields().ByName("source_service"), protoreflect.ValueOfString(string(rel.SourceService())))
		if !rel.CreatedAt().IsZero() {
			if tsVal, err := dynamicapi.MarshalTimestamp(relDesc.Fields().ByName("create_time"), rel.CreatedAt()); err == nil {
				relMsg.Set(relDesc.Fields().ByName("create_time"), tsVal)
			}
		}
		relList.Append(protoreflect.ValueOfMessage(relMsg))
	}

	if !pr.CreatedAt().IsZero() {
		if tsVal, err := dynamicapi.MarshalTimestamp(h.descs.Resource.Fields().ByName("create_time"), pr.CreatedAt()); err == nil {
			msg.Set(h.descs.Resource.Fields().ByName("create_time"), tsVal)
		}
	}
	if !pr.UpdatedAt().IsZero() {
		if tsVal, err := dynamicapi.MarshalTimestamp(h.descs.Resource.Fields().ByName("update_time"), pr.UpdatedAt()); err == nil {
			msg.Set(h.descs.Resource.Fields().ByName("update_time"), tsVal)
		}
	}

	return msg, nil
}

// extractMapStringString reads a proto3 map<string,string> field from a
// dynamic message.
func extractMapStringString(msg protoreflect.Message, field protoreflect.FieldDescriptor) map[string]string {
	if !msg.Has(field) {
		return nil
	}
	m := msg.Get(field).Map()
	result := make(map[string]string, m.Len())
	m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		result[k.String()] = v.String()
		return true
	})
	return result
}

// setMapStringString writes a Go map into a proto3 map<string,string>
// field on a dynamic message.
func setMapStringString(msg *dynamicpb.Message, field protoreflect.FieldDescriptor, m map[string]string) {
	if len(m) == 0 {
		return
	}
	mapField := msg.Mutable(field).Map()
	for k, v := range m {
		mapField.Set(
			protoreflect.ValueOfString(k).MapKey(),
			protoreflect.ValueOfString(v),
		)
	}
}
