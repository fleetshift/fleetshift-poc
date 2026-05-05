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

	desc, err := managedresource.CompileSpec(context.Background(), managedresource.CompileInput{
		SourceFile:  "addons/cluster_mgmt/v1/cluster_spec.proto",
		MessageName: "addons.cluster_mgmt.v1.ClusterSpec",
		ImportPaths: []string{"../../../../proto"},
	})
	if err != nil {
		b.Fatalf("CompileSpec: %v", err)
	}

	validator, err := protovalidate.New()
	if err != nil {
		b.Fatalf("protovalidate.New: %v", err)
	}

	cfg := &managedresource.ResourceTypeConfig{
		ResourceType:   "clusters",
		Singular:       "Cluster",
		Plural:         "clusters",
		ProtoPackage:   "fleetshift.v1",
		SpecMessage:    "addons.cluster_mgmt.v1.ClusterSpec",
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
		msg.Set(nameField, protoreflect.ValueOfString("clusters/prod-us-east-1"))
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
	specJSON := json.RawMessage(`{"provider":"rosa","version":"4.15.2","region":"us-east-1"}`)

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
		msg.Set(nameField, protoreflect.ValueOfString("clusters/prod-us-east-1"))
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

// BenchmarkSpecValidation measures the cost of the spec validation roundtrip:
// marshal spec to JSON from dynamic message, unmarshal into original descriptor,
// then validate with protovalidate.
func BenchmarkSpecValidation(b *testing.B) {
	env := setupBench(b)
	resourceDesc := env.svc.Descriptors.Resource
	specField := resourceDesc.Fields().ByName("spec")
	_ = specField

	specMsg := dynamicpb.NewMessage(env.specDesc)
	specMsg.Set(env.specDesc.Fields().ByName("provider"), protoreflect.ValueOfString("rosa"))
	specMsg.Set(env.specDesc.Fields().ByName("version"), protoreflect.ValueOfString("4.15.2"))
	specMsg.Set(env.specDesc.Fields().ByName("region"), protoreflect.ValueOfString("us-east-1"))

	b.ResetTimer()
	for range b.N {
		specJSON, _ := protojson.Marshal(specMsg)
		validationMsg := dynamicpb.NewMessage(env.specDesc)
		_ = protojson.Unmarshal(specJSON, validationMsg)
		_ = env.validator.Validate(validationMsg)
	}
}

// BenchmarkSpecValidation_SkipRoundtrip measures only the protovalidate call
// (no JSON roundtrip) to show how much overhead the roundtrip adds.
func BenchmarkSpecValidation_SkipRoundtrip(b *testing.B) {
	env := setupBench(b)

	specMsg := dynamicpb.NewMessage(env.specDesc)
	specMsg.Set(env.specDesc.Fields().ByName("provider"), protoreflect.ValueOfString("rosa"))
	specMsg.Set(env.specDesc.Fields().ByName("version"), protoreflect.ValueOfString("4.15.2"))
	specMsg.Set(env.specDesc.Fields().ByName("region"), protoreflect.ValueOfString("us-east-1"))

	b.ResetTimer()
	for range b.N {
		_ = env.validator.Validate(specMsg)
	}
}

// BenchmarkResponseMarshal measures the cost of marshaling a fully-built
// dynamic resource message to wire format (what gRPC does before sending).
func BenchmarkResponseMarshal(b *testing.B) {
	env := setupBench(b)
	resourceDesc := env.svc.Descriptors.Resource

	msg := dynamicpb.NewMessage(resourceDesc)
	msg.Set(resourceDesc.Fields().ByName("name"), protoreflect.ValueOfString("clusters/prod-us-east-1"))
	msg.Set(resourceDesc.Fields().ByName("uid"), protoreflect.ValueOfString("550e8400-e29b-41d4-a716-446655440000"))
	msg.Set(resourceDesc.Fields().ByName("intent_version"), protoreflect.ValueOfInt64(3))
	msg.Set(resourceDesc.Fields().ByName("state"), protoreflect.ValueOfInt32(2))
	msg.Set(resourceDesc.Fields().ByName("reconciling"), protoreflect.ValueOfBool(false))
	msg.Set(resourceDesc.Fields().ByName("etag"), protoreflect.ValueOfString("550e8400-e29b-41d4-a716-446655440000"))

	specMsg := dynamicpb.NewMessage(env.specDesc)
	specMsg.Set(env.specDesc.Fields().ByName("provider"), protoreflect.ValueOfString("rosa"))
	specMsg.Set(env.specDesc.Fields().ByName("version"), protoreflect.ValueOfString("4.15.2"))
	specMsg.Set(env.specDesc.Fields().ByName("region"), protoreflect.ValueOfString("us-east-1"))
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
	specMsg.Set(env.specDesc.Fields().ByName("provider"), protoreflect.ValueOfString("rosa"))
	specMsg.Set(env.specDesc.Fields().ByName("version"), protoreflect.ValueOfString("4.15.2"))
	specMsg.Set(env.specDesc.Fields().ByName("region"), protoreflect.ValueOfString("us-east-1"))
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
	specMsg.Set(env.specDesc.Fields().ByName("provider"), protoreflect.ValueOfString("rosa"))
	specMsg.Set(env.specDesc.Fields().ByName("version"), protoreflect.ValueOfString("4.15.2"))
	specMsg.Set(env.specDesc.Fields().ByName("region"), protoreflect.ValueOfString("us-east-1"))
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

		// Validation roundtrip
		specJSON, _ := protojson.Marshal(specM.Interface())
		validationMsg := dynamicpb.NewMessage(env.specDesc)
		_ = protojson.Unmarshal(specJSON, validationMsg)
		_ = env.validator.Validate(validationMsg)

		// Build domain input (what we'd pass to the application layer)
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
			ResourceType:   "clusters",
			Name:           "prod-us-east-1",
			UID:            "550e8400-e29b-41d4-a716-446655440000",
			CurrentVersion: 3,
			FulfillmentID:  "ful-123",
			CreatedAt:      now.Add(-1 * time.Hour),
			UpdatedAt:      now,
		},
		Intent: domain.ResourceIntent{
			Spec: json.RawMessage(`{"provider":"rosa","version":"4.15.2","region":"us-east-1"}`),
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
		msg.Set(nameField, protoreflect.ValueOfString("clusters/"+string(view.ManagedResource.Name)))
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
