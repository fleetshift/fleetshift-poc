package managedresource_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/managedresource"
)

const specProtoFile = "addons/cluster_mgmt/v1/cluster_spec.proto"
const specMessageName = "addons.cluster_mgmt.v1.ClusterSpec"

// protoImportPaths returns the import paths that resolve both the addon spec
// proto source and its transitive dependencies on the filesystem. Dependencies
// not found on the filesystem (like buf.validate) are resolved from the global
// proto registry at runtime.
func protoImportPaths() []string {
	return []string{"../../../../proto"}
}

func TestCompileSpec_FromSource(t *testing.T) {
	desc, err := managedresource.CompileSpec(context.Background(), managedresource.CompileInput{
		SourceFile:  specProtoFile,
		MessageName: specMessageName,
		ImportPaths: protoImportPaths(),
	})
	if err != nil {
		t.Fatalf("CompileSpec: %v", err)
	}

	if desc.Message == nil {
		t.Fatal("message descriptor is nil")
	}
	if got := string(desc.Message.FullName()); got != specMessageName {
		t.Errorf("message full name = %q, want %q", got, specMessageName)
	}

	providerField := desc.Message.Fields().ByName("provider")
	if providerField == nil {
		t.Fatal("provider field not found")
	}
	versionField := desc.Message.Fields().ByName("version")
	if versionField == nil {
		t.Fatal("version field not found")
	}
	regionField := desc.Message.Fields().ByName("region")
	if regionField == nil {
		t.Fatal("region field not found")
	}
}

func TestCompileSpec_DynamicMessageRoundTrip(t *testing.T) {
	desc, err := managedresource.CompileSpec(context.Background(), managedresource.CompileInput{
		SourceFile:  specProtoFile,
		MessageName: specMessageName,
		ImportPaths: protoImportPaths(),
	})
	if err != nil {
		t.Fatalf("CompileSpec: %v", err)
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
