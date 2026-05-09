package managedresource_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/managedresource"
)

// benchEnv holds pre-built descriptors for benchmarking the dynamic handler
// hot paths in isolation (no gRPC, no DB).
type benchEnv struct {
	svc       *managedresource.RegisteredService
	specDesc  protoreflect.MessageDescriptor
	validator protovalidate.Validator
}

func setupBench(b *testing.B) *benchEnv {
	b.Helper()

	schema := kindaddon.Schema()
	var entryFile string
	for name := range schema.ProtoFiles {
		entryFile = name
		break
	}
	desc, err := managedresource.CompileInline(
		context.Background(),
		schema.ProtoFiles,
		entryFile,
		protoreflect.FullName(schema.SpecMessage),
	)
	if err != nil {
		b.Fatalf("CompileInline: %v", err)
	}

	validator, err := protovalidate.New()
	if err != nil {
		b.Fatalf("protovalidate.New: %v", err)
	}

	cfg := &managedresource.ResourceTypeConfig{
		ResourceType:   kindaddon.ClusterResourceType,
		Singular:       schema.Singular,
		Plural:         schema.Plural,
		ProtoPackage:   "fleetshift.v1",
		SpecMessage:    schema.SpecMessage,
		SpecDescriptor: desc.Message,
	}

	svc, err := managedresource.Build(cfg, managedresource.Deps{
		Validator: validator,
	})
	if err != nil {
		b.Fatalf("Build: %v", err)
	}

	return &benchEnv{
		svc:       svc,
		specDesc:  desc.Message,
		validator: validator,
	}
}

// BenchmarkDynamicMessage_NewMessage measures the cost of allocating a new
// dynamic protobuf message (the resource envelope).
func BenchmarkDynamicMessage_NewMessage(b *testing.B) {
	env := setupBench(b)
	resourceDesc := env.svc.Descriptors.Resource

	b.ResetTimer()
	for range b.N {
		msg := dynamicpb.NewMessage(resourceDesc)
		_ = msg
	}
}

// BenchmarkDynamicMessage_SetFields measures building a full resource
// response by setting fields on a dynamic message (simulates viewToResource).
func BenchmarkDynamicMessage_SetFields(b *testing.B) {
	env := setupBench(b)
	resourceDesc := env.svc.Descriptors.Resource

	nameField := resourceDesc.Fields().ByName("name")
	uidField := resourceDesc.Fields().ByName("uid")
	versionField := resourceDesc.Fields().ByName("intent_version")
	stateField := resourceDesc.Fields().ByName("state")
	reconcilingField := resourceDesc.Fields().ByName("reconciling")
	etagField := resourceDesc.Fields().ByName("etag")

	b.ResetTimer()
	for range b.N {
		msg := dynamicpb.NewMessage(resourceDesc)
		msg.Set(nameField, protoreflect.ValueOfString("kindClusters/prod-us-east-1"))
		msg.Set(uidField, protoreflect.ValueOfString("550e8400-e29b-41d4-a716-446655440000"))
		msg.Set(versionField, protoreflect.ValueOfInt64(3))
		msg.Set(stateField, protoreflect.ValueOfInt32(2))
		msg.Set(reconcilingField, protoreflect.ValueOfBool(false))
		msg.Set(etagField, protoreflect.ValueOfString("550e8400-e29b-41d4-a716-446655440000"))
	}
}

// BenchmarkDynamicMessage_SetFieldsWithSpec measures building a full resource
// response including unmarshaling the spec from JSON into the dynamic message.
func BenchmarkDynamicMessage_SetFieldsWithSpec(b *testing.B) {
	env := setupBench(b)
	resourceDesc := env.svc.Descriptors.Resource
	specJSON := json.RawMessage(`{"name":"prod-us-east-1"}`)

	nameField := resourceDesc.Fields().ByName("name")
	uidField := resourceDesc.Fields().ByName("uid")
	specField := resourceDesc.Fields().ByName("spec")
	versionField := resourceDesc.Fields().ByName("intent_version")
	stateField := resourceDesc.Fields().ByName("state")
	reconcilingField := resourceDesc.Fields().ByName("reconciling")
	etagField := resourceDesc.Fields().ByName("etag")

	b.ResetTimer()
	for range b.N {
		msg := dynamicpb.NewMessage(resourceDesc)
		msg.Set(nameField, protoreflect.ValueOfString("kindClusters/prod-us-east-1"))
		msg.Set(uidField, protoreflect.ValueOfString("550e8400-e29b-41d4-a716-446655440000"))

		specMsg := dynamicpb.NewMessage(env.specDesc)
		_ = protojson.Unmarshal(specJSON, specMsg)
		msg.Set(specField, protoreflect.ValueOfMessage(specMsg))

		msg.Set(versionField, protoreflect.ValueOfInt64(3))
		msg.Set(stateField, protoreflect.ValueOfInt32(2))
		msg.Set(reconcilingField, protoreflect.ValueOfBool(false))
		msg.Set(etagField, protoreflect.ValueOfString("550e8400-e29b-41d4-a716-446655440000"))
	}
}

// BenchmarkSpecValidation_Direct measures the cost of direct protovalidate
// validation (current approach — no JSON roundtrip needed since the spec
// descriptor carries annotations through the synthesized file).
func BenchmarkSpecValidation_Direct(b *testing.B) {
	env := setupBench(b)

	specMsg := dynamicpb.NewMessage(env.specDesc)
	specMsg.Set(env.specDesc.Fields().ByName("name"), protoreflect.ValueOfString("prod-us-east-1"))

	b.ResetTimer()
	for range b.N {
		_ = env.validator.Validate(specMsg)
	}
}

// BenchmarkSpecValidation_WithJSONMarshal measures validation + the single
// JSON marshal needed for downstream persistence.
func BenchmarkSpecValidation_WithJSONMarshal(b *testing.B) {
	env := setupBench(b)

	specMsg := dynamicpb.NewMessage(env.specDesc)
	specMsg.Set(env.specDesc.Fields().ByName("name"), protoreflect.ValueOfString("prod-us-east-1"))

	b.ResetTimer()
	for range b.N {
		_ = env.validator.Validate(specMsg)
		_, _ = protojson.Marshal(specMsg)
	}
}

// BenchmarkResponseMarshal measures the cost of marshaling a fully-built
// dynamic resource message to wire format (what gRPC does before sending).
func BenchmarkResponseMarshal(b *testing.B) {
	env := setupBench(b)
	resourceDesc := env.svc.Descriptors.Resource

	msg := dynamicpb.NewMessage(resourceDesc)
	msg.Set(resourceDesc.Fields().ByName("name"), protoreflect.ValueOfString("kindClusters/prod-us-east-1"))
	msg.Set(resourceDesc.Fields().ByName("uid"), protoreflect.ValueOfString("550e8400-e29b-41d4-a716-446655440000"))
	msg.Set(resourceDesc.Fields().ByName("intent_version"), protoreflect.ValueOfInt64(3))
	msg.Set(resourceDesc.Fields().ByName("state"), protoreflect.ValueOfInt32(2))
	msg.Set(resourceDesc.Fields().ByName("reconciling"), protoreflect.ValueOfBool(false))
	msg.Set(resourceDesc.Fields().ByName("etag"), protoreflect.ValueOfString("550e8400-e29b-41d4-a716-446655440000"))

	specMsg := dynamicpb.NewMessage(env.specDesc)
	specMsg.Set(env.specDesc.Fields().ByName("name"), protoreflect.ValueOfString("prod-us-east-1"))
	msg.Set(resourceDesc.Fields().ByName("spec"), protoreflect.ValueOfMessage(specMsg))

	b.ResetTimer()
	for range b.N {
		out, _ := proto.Marshal(msg)
		_ = out
	}
}

// BenchmarkRequestUnmarshal measures the cost of unmarshaling a proto-encoded
// CreateRequest into a dynamic message (what gRPC does on receive).
func BenchmarkRequestUnmarshal(b *testing.B) {
	env := setupBench(b)
	createReqDesc := env.svc.Descriptors.CreateRequest

	// Build a sample request and marshal it to bytes.
	req := dynamicpb.NewMessage(createReqDesc)
	req.Set(createReqDesc.Fields().ByNumber(1), protoreflect.ValueOfString("prod-us-east-1"))

	resourceDesc := env.svc.Descriptors.Resource
	resource := dynamicpb.NewMessage(resourceDesc)
	specMsg := dynamicpb.NewMessage(env.specDesc)
	specMsg.Set(env.specDesc.Fields().ByName("name"), protoreflect.ValueOfString("prod-us-east-1"))
	resource.Set(resourceDesc.Fields().ByName("spec"), protoreflect.ValueOfMessage(specMsg))
	req.Set(createReqDesc.Fields().ByNumber(2), protoreflect.ValueOfMessage(resource))

	encoded, err := proto.Marshal(req)
	if err != nil {
		b.Fatalf("marshal request: %v", err)
	}

	b.ResetTimer()
	for range b.N {
		msg := dynamicpb.NewMessage(createReqDesc)
		_ = proto.Unmarshal(encoded, msg)
	}
}

// BenchmarkFullCreatePath measures the full create handler hot path:
// unmarshal request + extract fields + validate spec + build domain input.
// Excludes the actual DB/workflow call.
func BenchmarkFullCreatePath(b *testing.B) {
	env := setupBench(b)
	createReqDesc := env.svc.Descriptors.CreateRequest
	resourceDesc := env.svc.Descriptors.Resource

	// Build a sample encoded request.
	req := dynamicpb.NewMessage(createReqDesc)
	req.Set(createReqDesc.Fields().ByNumber(1), protoreflect.ValueOfString("prod-us-east-1"))

	resource := dynamicpb.NewMessage(resourceDesc)
	specMsg := dynamicpb.NewMessage(env.specDesc)
	specMsg.Set(env.specDesc.Fields().ByName("name"), protoreflect.ValueOfString("prod-us-east-1"))
	resource.Set(resourceDesc.Fields().ByName("spec"), protoreflect.ValueOfMessage(specMsg))
	req.Set(createReqDesc.Fields().ByNumber(2), protoreflect.ValueOfMessage(resource))

	encoded, err := proto.Marshal(req)
	if err != nil {
		b.Fatalf("marshal request: %v", err)
	}

	b.ResetTimer()
	for range b.N {
		// Unmarshal (gRPC decode step)
		incoming := dynamicpb.NewMessage(createReqDesc)
		_ = proto.Unmarshal(encoded, incoming)
		reqMsg := incoming.ProtoReflect()

		// Extract fields
		id := reqMsg.Get(createReqDesc.Fields().ByNumber(1)).String()
		resourceField := createReqDesc.Fields().ByNumber(2)
		resourceM := reqMsg.Get(resourceField).Message()
		specField := resourceDesc.Fields().ByName("spec")
		specM := resourceM.Get(specField).Message()

		// Direct validation (no roundtrip)
		_ = env.validator.Validate(specM.Interface())

		// Single JSON marshal for persistence
		specJSON, _ := protojson.Marshal(specM.Interface())

		// Build domain input
		_ = domain.ResourceName(id)
		_ = json.RawMessage(specJSON)
	}
}

// BenchmarkFullResponsePath measures the full response building hot path:
// construct dynamic message from domain view + marshal to wire format.
func BenchmarkFullResponsePath(b *testing.B) {
	env := setupBench(b)
	resourceDesc := env.svc.Descriptors.Resource

	now := time.Now()
	view := domain.ManagedResourceView{
		ManagedResource: domain.ManagedResource{
			ResourceType:   kindaddon.ClusterResourceType,
			Name:           "prod-us-east-1",
			UID:            "550e8400-e29b-41d4-a716-446655440000",
			CurrentVersion: 3,
			FulfillmentID:  "ful-123",
			CreatedAt:      now.Add(-1 * time.Hour),
			UpdatedAt:      now,
		},
		Intent: domain.ResourceIntent{
			Spec: json.RawMessage(`{"name":"prod-us-east-1"}`),
		},
		Fulfillment: domain.Fulfillment{
			State: domain.FulfillmentStateActive,
		},
	}

	nameField := resourceDesc.Fields().ByName("name")
	uidField := resourceDesc.Fields().ByName("uid")
	specField := resourceDesc.Fields().ByName("spec")
	versionField := resourceDesc.Fields().ByName("intent_version")
	stateField := resourceDesc.Fields().ByName("state")
	reconcilingField := resourceDesc.Fields().ByName("reconciling")
	etagField := resourceDesc.Fields().ByName("etag")

	b.ResetTimer()
	for range b.N {
		msg := dynamicpb.NewMessage(resourceDesc)
		msg.Set(nameField, protoreflect.ValueOfString("kindClusters/"+string(view.ManagedResource.Name)))
		msg.Set(uidField, protoreflect.ValueOfString(view.ManagedResource.UID))

		specMsg := dynamicpb.NewMessage(env.specDesc)
		_ = protojson.Unmarshal(view.Intent.Spec, specMsg)
		msg.Set(specField, protoreflect.ValueOfMessage(specMsg))

		msg.Set(versionField, protoreflect.ValueOfInt64(int64(view.ManagedResource.CurrentVersion)))
		msg.Set(stateField, protoreflect.ValueOfInt32(2))
		msg.Set(reconcilingField, protoreflect.ValueOfBool(false))
		msg.Set(etagField, protoreflect.ValueOfString(view.ManagedResource.UID))

		// Wire marshal (what gRPC does)
		out, _ := proto.Marshal(msg)
		_ = out
	}
}
