package managedresource_test

import (
	"encoding/json"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/managedresource"
)

func TestViewToStruct_MatchesViewToResourceProtoJSON(t *testing.T) {
	cfg := clusterConfig(t)
	svc, err := managedresource.Build(cfg, managedresource.Deps{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	name := domain.ResourceName("clusters/parity-1")
	view := domain.ExtensionResourceView{
		Resource: *domain.NewExtensionResource(
			domain.NewExtensionResourceUID(),
			cfg.ResourceType,
			name,
			now,
		),
		Intent: &domain.ResourceIntent{
			Spec: json.RawMessage(`{"name":"parity-1"}`),
		},
	}

	dynMsg, err := managedresource.ViewToResource(svc.Descriptors, view)
	if err != nil {
		t.Fatalf("ViewToResource: %v", err)
	}
	wantJSON, err := protojson.Marshal(dynMsg)
	if err != nil {
		t.Fatalf("protojson.Marshal(ViewToResource): %v", err)
	}

	gotStruct, err := managedresource.ViewToStruct(svc.Descriptors, view)
	if err != nil {
		t.Fatalf("ViewToStruct: %v", err)
	}
	gotJSON, err := protojson.Marshal(gotStruct)
	if err != nil {
		t.Fatalf("protojson.Marshal(ViewToStruct): %v", err)
	}

	// Re-parse both as maps so key order does not matter.
	var wantMap, gotMap map[string]any
	if err := json.Unmarshal(wantJSON, &wantMap); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	if err := json.Unmarshal(gotJSON, &gotMap); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	wantNorm, _ := json.Marshal(wantMap)
	gotNorm, _ := json.Marshal(gotMap)
	if string(wantNorm) != string(gotNorm) {
		t.Fatalf("ViewToStruct JSON != ViewToResource protojson\nwant: %s\ngot:  %s", wantNorm, gotNorm)
	}

	if gotStruct.Fields["name"].GetStringValue() != string(name) {
		t.Errorf("name = %q, want %q", gotStruct.Fields["name"].GetStringValue(), name)
	}
	if gotStruct.Fields["uid"].GetStringValue() == "" {
		t.Error("uid is empty")
	}
	if gotStruct.Fields["spec"] == nil {
		t.Error("spec is missing")
	}
}

func TestViewToStruct_RequiresDescriptors(t *testing.T) {
	view := domain.ExtensionResourceView{
		Resource: *domain.NewExtensionResource(
			domain.NewExtensionResourceUID(),
			"kind.fleetshift.io/Cluster",
			"clusters/x",
			time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
		),
	}
	_, err := managedresource.ViewToStruct(nil, view)
	if err == nil {
		t.Fatal("ViewToStruct(nil): want error")
	}
}

func TestViewToResource_ReturnsDynamicMessage(t *testing.T) {
	cfg := clusterConfig(t)
	svc, err := managedresource.Build(cfg, managedresource.Deps{})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	view := domain.ExtensionResourceView{
		Resource: *domain.NewExtensionResource(
			domain.NewExtensionResourceUID(),
			cfg.ResourceType,
			"clusters/dyn",
			time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
		),
	}
	msg, err := managedresource.ViewToResource(svc.Descriptors, view)
	if err != nil {
		t.Fatalf("ViewToResource: %v", err)
	}
	if msg == nil {
		t.Fatal("ViewToResource returned nil")
	}
	_ = proto.MessageName(msg)
	if string(msg.ProtoReflect().Descriptor().Name()) == "" {
		t.Error("message descriptor name is empty")
	}
}
