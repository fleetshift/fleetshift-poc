package dynamicapi_test

import (
	"context"
	"testing"

	_ "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

const specMessageName = "addons.kind.v1.KindClusterSpec"

func TestCompileInline(t *testing.T) {
	schema := kindaddon.Schema()
	var entryFile string
	for name := range schema.ProtoFiles {
		entryFile = name
		break
	}

	desc, err := dynamicapi.CompileInline(
		context.Background(),
		schema.ProtoFiles,
		entryFile,
		protoreflect.FullName(schema.SpecMessage),
	)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}

	if desc.Message == nil {
		t.Fatal("message descriptor is nil")
	}
	if got := string(desc.Message.FullName()); got != specMessageName {
		t.Errorf("message full name = %q, want %q", got, specMessageName)
	}

	for _, field := range []string{"name", "nodes", "networking"} {
		if desc.Message.Fields().ByName(protoreflect.Name(field)) == nil {
			t.Errorf("field %q not found", field)
		}
	}
}

func TestCompileSpec_DynamicMessageRoundTrip(t *testing.T) {
	schema := kindaddon.Schema()
	var entryFile string
	for name := range schema.ProtoFiles {
		entryFile = name
		break
	}

	desc, err := dynamicapi.CompileInline(
		context.Background(),
		schema.ProtoFiles,
		entryFile,
		protoreflect.FullName(schema.SpecMessage),
	)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}

	msg := dynamicpb.NewMessage(desc.Message)
	nameField := desc.Message.Fields().ByName("name")

	msg.Set(nameField, protoreflect.ValueOfString("test-cluster"))

	jsonBytes, err := protojson.Marshal(msg)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}

	roundTrip := dynamicpb.NewMessage(desc.Message)
	if err := protojson.Unmarshal(jsonBytes, roundTrip); err != nil {
		t.Fatalf("protojson.Unmarshal: %v", err)
	}

	if got := roundTrip.Get(nameField).String(); got != "test-cluster" {
		t.Errorf("name = %q, want %q", got, "test-cluster")
	}
}
