package managedresource

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	fleetshiftv1 "github.com/fleetshift/fleetshift-poc/fleetshift-server/gen/fleetshift/v1"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/application"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// RegisteredService is a fully built dynamic gRPC service ready to be
// registered on a gRPC server. It is the output of [BuildAndRegister].
type RegisteredService struct {
	// Desc is the gRPC service descriptor used for registration.
	Desc *grpc.ServiceDesc

	// ServiceDescriptors holds the proto descriptors for building messages.
	Descriptors *ServiceDescriptors

	// Config is the original type config.
	Config *ResourceTypeConfig
}

// Deps holds the shared dependencies injected into all dynamic services.
type Deps struct {
	Resources *application.ManagedResourceService
	Validator protovalidate.Validator
}

// BuildAndRegister constructs a dynamic gRPC service for the given
// resource type configuration and registers it on the server. This is
// the main entry point for the dynamic service pipeline:
//
//  1. Resolve the spec message descriptor (from global registry)
//  2. Build the service descriptors (resource, requests, responses)
//  3. Build the grpc.ServiceDesc with dynamic method handlers
//  4. Register on the gRPC server
func BuildAndRegister(server *grpc.Server, cfg *ResourceTypeConfig, deps Deps) (*RegisteredService, error) {
	svc, err := Build(cfg, deps)
	if err != nil {
		return nil, err
	}
	server.RegisterService(svc.Desc, nil)
	return svc, nil
}

// Build constructs a dynamic gRPC service without registering it.
// Useful for testing or deferred registration.
func Build(cfg *ResourceTypeConfig, deps Deps) (*RegisteredService, error) {
	specDesc, err := resolveSpecDescriptor(cfg)
	if err != nil {
		return nil, fmt.Errorf("resolve spec %s: %w", cfg.SpecMessage, err)
	}

	svcDescs, err := BuildServiceDescriptors(cfg, specDesc)
	if err != nil {
		return nil, fmt.Errorf("build descriptors for %s: %w", cfg.Singular, err)
	}

	handler := &dynamicHandler{
		cfg:        cfg,
		descs:      svcDescs,
		resources:  deps.Resources,
		validator:  deps.Validator,
		collection: cfg.Collection(),
	}

	grpcDesc := &grpc.ServiceDesc{
		ServiceName: cfg.ServiceName(),
		HandlerType: (*any)(nil),
		Methods: []grpc.MethodDesc{
			{
				MethodName: "Create" + cfg.Singular,
				Handler:    handler.handleCreate,
			},
			{
				MethodName: "Get" + cfg.Singular,
				Handler:    handler.handleGet,
			},
			{
				MethodName: "List" + cfg.Plural,
				Handler:    handler.handleList,
			},
			{
				MethodName: "Delete" + cfg.Singular,
				Handler:    handler.handleDelete,
			},
			{
				MethodName: "Resume" + cfg.Singular,
				Handler:    handler.handleResume,
			},
		},
		Streams:  []grpc.StreamDesc{},
		Metadata: "dynamic/" + strings.ToLower(cfg.Singular[:1]) + cfg.Singular[1:] + "_service.proto",
	}

	return &RegisteredService{
		Desc:        grpcDesc,
		Descriptors: svcDescs,
		Config:      cfg,
	}, nil
}

func resolveSpecDescriptor(cfg *ResourceTypeConfig) (protoreflect.MessageDescriptor, error) {
	if cfg.SpecDescriptor != nil {
		return cfg.SpecDescriptor, nil
	}
	desc, err := CompileFromGlobalRegistry(cfg.SpecMessage)
	if err != nil {
		return nil, err
	}
	return desc.Message, nil
}

// dynamicHandler implements the gRPC method handler closures.
type dynamicHandler struct {
	cfg        *ResourceTypeConfig
	descs      *ServiceDescriptors
	resources  *application.ManagedResourceService
	validator  protovalidate.Validator
	collection string
}

func (h *dynamicHandler) handleCreate(
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
			FullMethod: "/" + h.cfg.ServiceName() + "/Create" + h.cfg.Singular,
		}
		return interceptor(ctx, req, info, func(ctx context.Context, r any) (any, error) {
			return h.doCreate(ctx, r.(proto.Message))
		})
	}
	return h.doCreate(ctx, req)
}

func (h *dynamicHandler) doCreate(ctx context.Context, req proto.Message) (proto.Message, error) {
	reqMsg := req.ProtoReflect()

	// Field 1: {resource}_id
	idField := h.descs.CreateRequest.Fields().ByNumber(1)
	id := reqMsg.Get(idField).String()
	if id == "" {
		return nil, status.Errorf(codes.InvalidArgument, "%s is required", idField.Name())
	}

	// Field 2: {resource} message containing spec
	resourceField := h.descs.CreateRequest.Fields().ByNumber(2)
	if !reqMsg.Has(resourceField) {
		return nil, status.Errorf(codes.InvalidArgument, "%s is required", resourceField.Name())
	}
	resourceMsg := reqMsg.Get(resourceField).Message()

	// Extract spec from the resource message
	specField := h.descs.Resource.Fields().ByName("spec")
	if !resourceMsg.Has(specField) {
		return nil, status.Error(codes.InvalidArgument, "spec is required")
	}
	specMsg := resourceMsg.Get(specField).Message()

	// Validate spec directly — the spec field's message descriptor is the
	// original addon descriptor (with buf.validate annotations), so
	// protovalidate can evaluate constraints without a copy.
	if err := h.validator.Validate(specMsg.Interface()); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "spec validation failed: %v", err)
	}

	// Marshal to JSON for downstream persistence (the domain/repo store JSON).
	specJSON, err := protojson.Marshal(specMsg.Interface())
	if err != nil {
		return nil, status.Errorf(codes.Internal, "marshal spec: %v", err)
	}

	in := application.CreateManagedResourceInput{
		ResourceType: h.cfg.ResourceType,
		Name:         domain.ResourceName(id),
		Spec:         json.RawMessage(specJSON),
	}

	// Field 3: user_signature (optional)
	sigField := h.descs.CreateRequest.Fields().ByNumber(3)
	if sigField != nil && reqMsg.Has(sigField) {
		in.UserSignature = reqMsg.Get(sigField).Bytes()
	}

	// Field 4: valid_until (optional)
	validUntilField := h.descs.CreateRequest.Fields().ByNumber(4)
	if validUntilField != nil && reqMsg.Has(validUntilField) {
		tsMsg := reqMsg.Get(validUntilField).Message()
		ts := &timestamppb.Timestamp{}
		b, err := proto.Marshal(tsMsg.Interface())
		if err != nil {
			return nil, status.Errorf(codes.Internal, "marshal valid_until: %v", err)
		}
		if err := proto.Unmarshal(b, ts); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshal valid_until: %v", err)
		}
		in.ValidUntil = ts.AsTime()
	}

	// Field 5: expected_generation (optional)
	genField := h.descs.CreateRequest.Fields().ByNumber(5)
	if genField != nil {
		in.ExpectedGeneration = domain.Generation(reqMsg.Get(genField).Int())
	}

	view, err := h.resources.Create(ctx, in)
	if err != nil {
		return nil, toDomainError(err)
	}

	return h.viewToResource(view)
}

func (h *dynamicHandler) handleGet(
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
			FullMethod: "/" + h.cfg.ServiceName() + "/Get" + h.cfg.Singular,
		}
		return interceptor(ctx, req, info, func(ctx context.Context, r any) (any, error) {
			return h.doGet(ctx, r.(proto.Message))
		})
	}
	return h.doGet(ctx, req)
}

func (h *dynamicHandler) doGet(ctx context.Context, req proto.Message) (proto.Message, error) {
	nameField := h.descs.GetRequest.Fields().ByName("name")
	name := req.ProtoReflect().Get(nameField).String()

	resourceName, err := h.parseName(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid name: %v", err)
	}

	view, err := h.resources.Get(ctx, h.cfg.ResourceType, resourceName)
	if err != nil {
		return nil, toDomainError(err)
	}

	return h.viewToResource(view)
}

func (h *dynamicHandler) handleList(
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
			FullMethod: "/" + h.cfg.ServiceName() + "/List" + h.cfg.Plural,
		}
		return interceptor(ctx, req, info, func(ctx context.Context, r any) (any, error) {
			return h.doList(ctx, r.(proto.Message))
		})
	}
	return h.doList(ctx, req)
}

func (h *dynamicHandler) doList(ctx context.Context, _ proto.Message) (proto.Message, error) {
	views, err := h.resources.List(ctx, h.cfg.ResourceType)
	if err != nil {
		return nil, toDomainError(err)
	}

	resp := dynamicpb.NewMessage(h.descs.ListResponse)
	resourcesField := h.descs.ListResponse.Fields().ByNumber(1)

	list := resp.Mutable(resourcesField).List()
	for _, v := range views {
		resource, err := h.viewToResource(v)
		if err != nil {
			return nil, err
		}
		list.Append(protoreflect.ValueOfMessage(resource.ProtoReflect()))
	}

	return resp, nil
}

func (h *dynamicHandler) handleDelete(
	_ any,
	ctx context.Context,
	dec func(any) error,
	interceptor grpc.UnaryServerInterceptor,
) (any, error) {
	req := dynamicpb.NewMessage(h.descs.DeleteRequest)
	if err := dec(req); err != nil {
		return nil, err
	}

	if interceptor != nil {
		info := &grpc.UnaryServerInfo{
			FullMethod: "/" + h.cfg.ServiceName() + "/Delete" + h.cfg.Singular,
		}
		return interceptor(ctx, req, info, func(ctx context.Context, r any) (any, error) {
			return h.doDelete(ctx, r.(proto.Message))
		})
	}
	return h.doDelete(ctx, req)
}

func (h *dynamicHandler) doDelete(ctx context.Context, req proto.Message) (proto.Message, error) {
	nameField := h.descs.DeleteRequest.Fields().ByName("name")
	name := req.ProtoReflect().Get(nameField).String()

	resourceName, err := h.parseName(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid name: %v", err)
	}

	view, err := h.resources.Delete(ctx, h.cfg.ResourceType, resourceName)
	if err != nil {
		return nil, toDomainError(err)
	}

	return h.viewToResource(view)
}

func (h *dynamicHandler) handleResume(
	_ any,
	ctx context.Context,
	dec func(any) error,
	interceptor grpc.UnaryServerInterceptor,
) (any, error) {
	req := dynamicpb.NewMessage(h.descs.ResumeRequest)
	if err := dec(req); err != nil {
		return nil, err
	}

	if interceptor != nil {
		info := &grpc.UnaryServerInfo{
			FullMethod: "/" + h.cfg.ServiceName() + "/Resume" + h.cfg.Singular,
		}
		return interceptor(ctx, req, info, func(ctx context.Context, r any) (any, error) {
			return h.doResume(ctx, r.(proto.Message))
		})
	}
	return h.doResume(ctx, req)
}

func (h *dynamicHandler) doResume(ctx context.Context, req proto.Message) (proto.Message, error) {
	reqMsg := req.ProtoReflect()

	nameField := h.descs.ResumeRequest.Fields().ByName("name")
	name := reqMsg.Get(nameField).String()
	resourceName, err := h.parseName(name)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid name: %v", err)
	}

	in := application.ResumeManagedResourceInput{
		ResourceType: h.cfg.ResourceType,
		Name:         resourceName,
	}

	// Field 2: user_signature (optional)
	sigField := h.descs.ResumeRequest.Fields().ByNumber(2)
	if sigField != nil && reqMsg.Has(sigField) {
		in.UserSignature = reqMsg.Get(sigField).Bytes()
	}

	// Field 3: valid_until (optional)
	validUntilField := h.descs.ResumeRequest.Fields().ByNumber(3)
	if validUntilField != nil && reqMsg.Has(validUntilField) {
		tsMsg := reqMsg.Get(validUntilField).Message()
		ts := &timestamppb.Timestamp{}
		b, mErr := proto.Marshal(tsMsg.Interface())
		if mErr != nil {
			return nil, status.Errorf(codes.Internal, "marshal valid_until: %v", mErr)
		}
		if mErr := proto.Unmarshal(b, ts); mErr != nil {
			return nil, status.Errorf(codes.Internal, "unmarshal valid_until: %v", mErr)
		}
		in.ValidUntil = ts.AsTime()
	}

	view, err := h.resources.Resume(ctx, in)
	if err != nil {
		return nil, toDomainError(err)
	}

	return h.viewToResource(view)
}

func (h *dynamicHandler) parseName(name string) (domain.ResourceName, error) {
	id, ok := strings.CutPrefix(name, h.collection)
	if !ok || id == "" {
		return "", fmt.Errorf("name must have format %s{id}", h.collection)
	}
	return domain.ResourceName(id), nil
}

// viewToResource converts a domain ManagedResourceView into a dynamic
// resource message populated with the platform envelope and addon spec.
func (h *dynamicHandler) viewToResource(v domain.ManagedResourceView) (proto.Message, error) {
	mr := v.ManagedResource
	f := v.Fulfillment

	resource := dynamicpb.NewMessage(h.descs.Resource)

	// name
	nameField := h.descs.Resource.Fields().ByName("name")
	resource.Set(nameField, protoreflect.ValueOfString(h.collection+string(mr.Name)))

	// uid
	uidField := h.descs.Resource.Fields().ByName("uid")
	resource.Set(uidField, protoreflect.ValueOfString(mr.UID))

	// spec
	specField := h.descs.Resource.Fields().ByName("spec")
	specMsg := dynamicpb.NewMessage(h.descs.Spec)
	if len(v.Intent.Spec) > 0 {
		if err := protojson.Unmarshal(v.Intent.Spec, specMsg); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshal spec: %v", err)
		}
	}
	resource.Set(specField, protoreflect.ValueOfMessage(specMsg))

	// intent_version
	versionField := h.descs.Resource.Fields().ByName("intent_version")
	resource.Set(versionField, protoreflect.ValueOfInt64(int64(mr.CurrentVersion)))

	// state
	stateField := h.descs.Resource.Fields().ByName("state")
	stateNum := int32(stateFromFulfillment(f.State))
	resource.Set(stateField, protoreflect.ValueOfEnum(protoreflect.EnumNumber(stateNum)))

	// reconciling
	reconcilingField := h.descs.Resource.Fields().ByName("reconciling")
	isReconciling := stateNum == 1 || stateNum == 3 || stateNum == 5
	resource.Set(reconcilingField, protoreflect.ValueOfBool(isReconciling))

	// create_time
	if !mr.CreatedAt.IsZero() {
		createTimeField := h.descs.Resource.Fields().ByName("create_time")
		if tsVal, err := marshalTimestamp(createTimeField, mr.CreatedAt); err != nil {
			return nil, err
		} else {
			resource.Set(createTimeField, tsVal)
		}
	}

	// update_time
	if !mr.UpdatedAt.IsZero() {
		updateTimeField := h.descs.Resource.Fields().ByName("update_time")
		if tsVal, err := marshalTimestamp(updateTimeField, mr.UpdatedAt); err != nil {
			return nil, err
		} else {
			resource.Set(updateTimeField, tsVal)
		}
	}

	// delete_time
	if mr.DeletedAt != nil {
		deleteTimeField := h.descs.Resource.Fields().ByName("delete_time")
		if tsVal, err := marshalTimestamp(deleteTimeField, *mr.DeletedAt); err != nil {
			return nil, err
		} else {
			resource.Set(deleteTimeField, tsVal)
		}
	}

	// etag
	etagField := h.descs.Resource.Fields().ByName("etag")
	resource.Set(etagField, protoreflect.ValueOfString(mr.UID))

	// provenance
	if f.Provenance != nil {
		provField := h.descs.Resource.Fields().ByName("provenance")
		if provVal, err := marshalProvenance(provField, f.Provenance); err != nil {
			return nil, err
		} else {
			resource.Set(provField, provVal)
		}
	}

	return resource, nil
}

func stateFromFulfillment(s domain.FulfillmentState) protoreflect.EnumNumber {
	switch s {
	case domain.FulfillmentStateCreating:
		return 1
	case domain.FulfillmentStateActive:
		return 2
	case domain.FulfillmentStateDeleting:
		return 3
	case domain.FulfillmentStateFailed:
		return 4
	case domain.FulfillmentStatePausedAuth:
		return 5
	default:
		return 0
	}
}

// marshalTimestamp converts a time.Time to a protoreflect.Value suitable
// for setting on a dynamic Timestamp field.
func marshalTimestamp(field protoreflect.FieldDescriptor, t time.Time) (protoreflect.Value, error) {
	ts := timestamppb.New(t)
	tsMsg := dynamicpb.NewMessage(field.Message())
	b, err := proto.Marshal(ts)
	if err != nil {
		return protoreflect.Value{}, fmt.Errorf("marshal %s: %w", field.Name(), err)
	}
	if err := proto.Unmarshal(b, tsMsg); err != nil {
		return protoreflect.Value{}, fmt.Errorf("unmarshal %s: %w", field.Name(), err)
	}
	return protoreflect.ValueOfMessage(tsMsg), nil
}

// marshalProvenance converts a domain Provenance to a protoreflect.Value
// suitable for setting on the dynamic resource message's provenance field.
func marshalProvenance(field protoreflect.FieldDescriptor, p *domain.Provenance) (protoreflect.Value, error) {
	pb := &fleetshiftv1.Provenance{
		Signature: &fleetshiftv1.Signature{
			Signer: &fleetshiftv1.FederatedIdentity{
				Subject: string(p.Sig.Signer.Subject),
				Issuer:  string(p.Sig.Signer.Issuer),
			},
			ContentHash:    p.Sig.ContentHash,
			SignatureBytes: p.Sig.SignatureBytes,
		},
		ExpectedGeneration: int64(p.ExpectedGeneration),
	}
	if !p.ValidUntil.IsZero() {
		pb.ValidUntil = timestamppb.New(p.ValidUntil)
	}
	for _, oc := range p.OutputConstraints {
		pb.OutputConstraints = append(pb.OutputConstraints, &fleetshiftv1.OutputConstraint{
			Name:       oc.Name,
			Expression: oc.Expression,
		})
	}

	b, err := proto.Marshal(pb)
	if err != nil {
		return protoreflect.Value{}, fmt.Errorf("marshal provenance: %w", err)
	}
	dynMsg := dynamicpb.NewMessage(field.Message())
	if err := proto.Unmarshal(b, dynMsg); err != nil {
		return protoreflect.Value{}, fmt.Errorf("unmarshal provenance: %w", err)
	}
	return protoreflect.ValueOfMessage(dynMsg), nil
}

func toDomainError(err error) error {
	switch {
	case errors.Is(err, domain.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, domain.ErrAlreadyExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, domain.ErrInvalidArgument):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, "internal error")
	}
}
