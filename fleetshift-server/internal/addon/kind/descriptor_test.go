package kind_test

import (
	"context"
	"testing"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/types/dynamicpb"

	kindaddon "github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/addon/kind"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

// TestKindClusterSpec_NameNotRequired verifies that the KindClusterSpec
// proto (the schema used to validate managed resource creation
// requests) does not require a "name" field. For managed resources,
// the cluster name always comes from the resource's own identity (see
// parseClusterManifest); a "name" in the spec body would be silently
// ignored, so callers must not be forced to supply one.
func TestKindClusterSpec_NameNotRequired(t *testing.T) {
	schema := kindaddon.Schema()
	desc, err := dynamicapi.CompileInline(context.Background(),
		schema.ProtoFiles, schema.EntryFile, schema.Management.SpecMessage)
	if err != nil {
		t.Fatalf("CompileInline: %v", err)
	}

	validator, err := protovalidate.New()
	if err != nil {
		t.Fatalf("protovalidate.New: %v", err)
	}

	msg := dynamicpb.NewMessage(desc.Message)
	if err := validator.Validate(msg); err != nil {
		t.Errorf("validate KindClusterSpec with no fields set (in particular, no name): %v", err)
	}
}
