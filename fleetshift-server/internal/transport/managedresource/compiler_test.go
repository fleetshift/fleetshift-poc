package managedresource_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/clustermgmt"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/managedresource"
)

const specMessageName = "addons.cluster_mgmt.v1.ClusterSpec"

func TestCompileInline(t *testing.T) {
	schema := clustermgmt.Schema()
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
		t.Fatalf("CompileInline: %v", err)
	}

	if desc.Message == nil {
		t.Fatal("message descriptor is nil")
	}
	if got := string(desc.Message.FullName()); got != specMessageName {
		t.Errorf("message full name = %q, want %q", got, specMessageName)
	}

	for _, field := range []string{"provider", "version", "region", "compute_pools", "network"} {
		if desc.Message.Fields().ByName(protoreflect.Name(field)) == nil {
			t.Errorf("field %q not found", field)
		}
	}
}

func TestCompileSpec_DynamicMessageRoundTrip(t *testing.T) {
	schema := clustermgmt.Schema()
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
		t.Fatalf("CompileInline: %v", err)
	}

	msg := dynamicpb.NewMessage(desc.Message)
	providerField := desc.Message.Fields().ByName("provider")
	versionField := desc.Message.Fields().ByName("version")
	regionField := desc.Message.Fields().ByName("region")

	msg.Set(providerField, protoreflect.ValueOfString("rosa"))
	msg.Set(versionField, protoreflect.ValueOfString("4.15.2"))
	msg.Set(regionField, protoreflect.ValueOfString("us-east-1"))

	jsonBytes, err := protojson.Marshal(msg)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}

	roundTrip := dynamicpb.NewMessage(desc.Message)
	if err := protojson.Unmarshal(jsonBytes, roundTrip); err != nil {
		t.Fatalf("protojson.Unmarshal: %v", err)
	}

	if got := roundTrip.Get(providerField).String(); got != "rosa" {
		t.Errorf("provider = %q, want %q", got, "rosa")
	}
	if got := roundTrip.Get(versionField).String(); got != "4.15.2" {
		t.Errorf("version = %q, want %q", got, "4.15.2")
	}
}
