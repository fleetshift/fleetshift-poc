package extensionresource

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

func TestViewToStruct_MatchesViewToResourceProtoJSON(t *testing.T) {
	cfg := managedOnlyConfig()
	descs, err := BuildExtensionServiceDescriptors(cfg, stubSpecDescriptor())
	if err != nil {
		t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
	}

	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	name := domain.ResourceName("clusters/parity-1")
	view := domain.ExtensionResourceView{
		Resource: *domain.NewExtensionResource(
			domain.NewExtensionResourceUID(),
			cfg.ResourceType,
			name,
			now,
			domain.WithExtensionLabels(map[string]string{"env": "prod"}),
		),
	}

	dynMsg, err := ViewToResource(descs, cfg, view)
	if err != nil {
		t.Fatalf("ViewToResource: %v", err)
	}
	wantJSON, err := protojson.Marshal(dynMsg)
	if err != nil {
		t.Fatalf("protojson.Marshal(ViewToResource): %v", err)
	}

	gotStruct, err := ViewToStruct(descs, cfg, view)
	if err != nil {
		t.Fatalf("ViewToStruct: %v", err)
	}
	gotJSON, err := protojson.Marshal(gotStruct)
	if err != nil {
		t.Fatalf("protojson.Marshal(ViewToStruct): %v", err)
	}

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
	if gotStruct.Fields["labels"] == nil {
		t.Error("labels missing from ViewToStruct")
	}
}

func TestViewToStruct_RequiresDescriptorsAndConfig(t *testing.T) {
	view := domain.ExtensionResourceView{
		Resource: *domain.NewExtensionResource(
			domain.NewExtensionResourceUID(),
			"kind.fleetshift.io/Cluster",
			"clusters/x",
			time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
		),
	}
	if _, err := ViewToStruct(nil, managedOnlyConfig(), view); err == nil {
		t.Fatal("ViewToStruct(nil descs): want error")
	}
	descs, err := BuildExtensionServiceDescriptors(managedOnlyConfig(), stubSpecDescriptor())
	if err != nil {
		t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
	}
	if _, err := ViewToStruct(descs, nil, view); err == nil {
		t.Fatal("ViewToStruct(nil cfg): want error")
	}
}

func TestViewToResource_ReturnsDynamicMessage(t *testing.T) {
	cfg := managedOnlyConfig()
	descs, err := BuildExtensionServiceDescriptors(cfg, stubSpecDescriptor())
	if err != nil {
		t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
	}
	view := domain.ExtensionResourceView{
		Resource: *domain.NewExtensionResource(
			domain.NewExtensionResourceUID(),
			cfg.ResourceType,
			"clusters/dyn",
			time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC),
		),
	}
	msg, err := ViewToResource(descs, cfg, view)
	if err != nil {
		t.Fatalf("ViewToResource: %v", err)
	}
	if msg == nil {
		t.Fatal("ViewToResource returned nil")
	}
	_ = proto.MessageName(msg)
}

func TestViewToResource_ManagedAndInventoryWithReport(t *testing.T) {
	cfg := managedAndInventoryConfig()
	descs, err := BuildExtensionServiceDescriptors(cfg, stubSpecDescriptor())
	if err != nil {
		t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
	}

	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	view := viewWithInventory(t, cfg.ResourceType, "clusters/both", now,
		map[string]string{"team": "platform"},
		&domain.InventoryResourceSnapshot{
			Labels:      map[string]string{"zone": "us-east-1"},
			Observation: json.RawMessage(`{"cpu":4}`),
			Conditions: []domain.ConditionSnapshot{
				{Type: "Ready", Status: domain.ConditionTrue, Reason: "OK", Message: "ready", LastTransitionTime: now},
			},
			ObservedAt: now,
			UpdatedAt:  now.Add(time.Minute),
		},
	)

	msg, err := ViewToResource(descs, cfg, view)
	if err != nil {
		t.Fatalf("ViewToResource: %v", err)
	}
	rm := msg.ProtoReflect()

	assertMapString(t, rm, "labels", "team", "platform")
	assertMapString(t, rm, "local_labels", "zone", "us-east-1")
	assertHasField(t, rm, "observation")
	assertHasField(t, rm, "local_update_time")
	assertHasField(t, rm, "index_update_time")
	assertCondition(t, rm, "Ready", "True", "OK", "ready")
	assertHasField(t, rm, "spec")
}

func TestViewToResource_ManagedAndInventoryNoReport(t *testing.T) {
	cfg := managedAndInventoryConfig()
	descs, err := BuildExtensionServiceDescriptors(cfg, stubSpecDescriptor())
	if err != nil {
		t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
	}

	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	view := domain.ExtensionResourceView{
		Resource: *domain.NewExtensionResource(
			domain.NewExtensionResourceUID(),
			cfg.ResourceType,
			"clusters/no-report",
			now,
			domain.WithExtensionLabels(map[string]string{"env": "dev"}),
			domain.WithManagedState("f-1"),
		),
	}

	msg, err := ViewToResource(descs, cfg, view)
	if err != nil {
		t.Fatalf("ViewToResource: %v", err)
	}
	rm := msg.ProtoReflect()

	assertMapString(t, rm, "labels", "env", "dev")
	assertMissingField(t, rm, "local_labels")
	assertMissingField(t, rm, "conditions")
	assertMissingField(t, rm, "observation")
	assertMissingField(t, rm, "local_update_time")
	assertMissingField(t, rm, "index_update_time")
}

func TestViewToResource_InventoryOnlyWithReport(t *testing.T) {
	cfg := inventoryOnlyConfig()
	descs, err := BuildExtensionServiceDescriptors(cfg, nil)
	if err != nil {
		t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
	}

	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	view := viewWithInventory(t, cfg.ResourceType, "clusters/inv", now,
		map[string]string{"owner": "ops"},
		&domain.InventoryResourceSnapshot{
			Labels:      map[string]string{"host": "node-1"},
			Observation: json.RawMessage(`{"ready":true}`),
			Conditions: []domain.ConditionSnapshot{
				{Type: "Healthy", Status: domain.ConditionTrue, Reason: "Nominal", Message: "ok", LastTransitionTime: now},
			},
			ObservedAt: now,
			UpdatedAt:  now,
		},
	)

	msg, err := ViewToResource(descs, cfg, view)
	if err != nil {
		t.Fatalf("ViewToResource: %v", err)
	}
	rm := msg.ProtoReflect()

	assertMapString(t, rm, "labels", "owner", "ops")
	assertMapString(t, rm, "local_labels", "host", "node-1")
	assertCondition(t, rm, "Healthy", "True", "Nominal", "ok")
	assertHasField(t, rm, "observation")
	assertMissingField(t, rm, "spec")
	assertMissingField(t, rm, "state")
}

func TestViewToResource_ManagedOnly_NoObservedFields(t *testing.T) {
	cfg := managedOnlyConfig()
	descs, err := BuildExtensionServiceDescriptors(cfg, stubSpecDescriptor())
	if err != nil {
		t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
	}

	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	view := viewWithInventory(t, cfg.ResourceType, "clusters/managed", now,
		map[string]string{"env": "prod"},
		&domain.InventoryResourceSnapshot{
			Labels:      map[string]string{"zone": "should-not-appear"},
			Observation: json.RawMessage(`{"cpu":1}`),
			ObservedAt:  now,
			UpdatedAt:   now,
		},
	)

	msg, err := ViewToResource(descs, cfg, view)
	if err != nil {
		t.Fatalf("ViewToResource: %v", err)
	}
	rm := msg.ProtoReflect()

	assertMapString(t, rm, "labels", "env", "prod")
	assertHasField(t, rm, "spec")
	// Descriptor has no observed fields for managed-only; ensure we did
	// not invent them and that labels stayed separate from inventory.
	if descs.Resource.Fields().ByName("local_labels") != nil {
		t.Fatal("managed-only descriptor unexpectedly has local_labels")
	}
}

func TestMarshalObservationStruct(t *testing.T) {
	cfg := inventoryOnlyConfig()
	descs, err := BuildExtensionServiceDescriptors(cfg, nil)
	if err != nil {
		t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
	}
	obsField := descs.Resource.Fields().ByName("observation")
	if obsField == nil {
		t.Fatal("observation field missing")
	}

	t.Run("empty raw leaves empty Struct", func(t *testing.T) {
		val, err := marshalObservationStruct(obsField, nil)
		if err != nil {
			t.Fatalf("marshalObservationStruct: %v", err)
		}
		b, err := protojson.Marshal(val.Message().Interface())
		if err != nil {
			t.Fatalf("protojson.Marshal: %v", err)
		}
		if string(b) != "{}" {
			t.Errorf("empty input: got %s, want {}", b)
		}
	})

	t.Run("object JSON populates Struct fields", func(t *testing.T) {
		val, err := marshalObservationStruct(obsField, json.RawMessage(`{"cpu":4,"ready":true}`))
		if err != nil {
			t.Fatalf("marshalObservationStruct: %v", err)
		}
		b, err := protojson.Marshal(val.Message().Interface())
		if err != nil {
			t.Fatalf("protojson.Marshal: %v", err)
		}
		var got map[string]any
		if err := json.Unmarshal(b, &got); err != nil {
			t.Fatalf("json.Unmarshal: %v", err)
		}
		if got["cpu"] != float64(4) {
			t.Errorf("cpu = %v, want 4", got["cpu"])
		}
		if got["ready"] != true {
			t.Errorf("ready = %v, want true", got["ready"])
		}
	})

	t.Run("invalid JSON is Internal", func(t *testing.T) {
		_, err := marshalObservationStruct(obsField, json.RawMessage(`{`))
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "unmarshal observation") {
			t.Errorf("err = %v, want unmarshal observation context", err)
		}
	})
}

func TestViewToResource_LabelsOnAllShapes(t *testing.T) {
	now := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		name string
		cfg  *ResourceTypeConfig
		spec protoreflect.MessageDescriptor
	}{
		{"managed", managedOnlyConfig(), stubSpecDescriptor()},
		{"inventory", inventoryOnlyConfig(), nil},
		{"both", managedAndInventoryConfig(), stubSpecDescriptor()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			descs, err := BuildExtensionServiceDescriptors(tc.cfg, tc.spec)
			if err != nil {
				t.Fatalf("BuildExtensionServiceDescriptors: %v", err)
			}
			view := domain.ExtensionResourceView{
				Resource: *domain.NewExtensionResource(
					domain.NewExtensionResourceUID(),
					tc.cfg.ResourceType,
					"clusters/labeled",
					now,
					domain.WithExtensionLabels(map[string]string{"a": "1"}),
				),
			}
			msg, err := ViewToResource(descs, tc.cfg, view)
			if err != nil {
				t.Fatalf("ViewToResource: %v", err)
			}
			assertMapString(t, msg.ProtoReflect(), "labels", "a", "1")
		})
	}
}

func viewWithInventory(
	t *testing.T,
	rt domain.ResourceType,
	name string,
	now time.Time,
	labels map[string]string,
	inv *domain.InventoryResourceSnapshot,
) domain.ExtensionResourceView {
	t.Helper()
	er := domain.ExtensionResourceFromSnapshot(domain.ExtensionResourceSnapshot{
		UID:          domain.NewExtensionResourceUID(),
		ResourceType: rt,
		Name:         domain.ResourceName(name),
		Labels:       labels,
		Managed: &domain.ManagedStateSnapshot{
			CurrentVersion: 1,
			FulfillmentID:  "f-1",
		},
		Inventory: inv,
		CreatedAt: now,
		UpdatedAt: now,
	})
	return domain.ExtensionResourceView{
		Resource: *er,
	}
}

func assertMapString(t *testing.T, msg protoreflect.Message, field, key, want string) {
	t.Helper()
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(field))
	if fd == nil {
		t.Fatalf("field %q missing from descriptor", field)
	}
	if !msg.Has(fd) {
		t.Fatalf("field %q unset", field)
	}
	got := msg.Get(fd).Map().Get(protoreflect.ValueOfString(key).MapKey()).String()
	if got != want {
		t.Errorf("%s[%s] = %q, want %q", field, key, got, want)
	}
}

func assertCondition(t *testing.T, msg protoreflect.Message, condType, status, reason, message string) {
	t.Helper()
	fd := msg.Descriptor().Fields().ByName("conditions")
	if fd == nil || !msg.Has(fd) {
		t.Fatal("conditions field missing or unset")
	}
	entry := msg.Get(fd).Map().Get(protoreflect.ValueOfString(condType).MapKey()).Message()
	if !entry.IsValid() {
		t.Fatalf("conditions[%s] missing", condType)
	}
	ed := entry.Descriptor()
	if got := entry.Get(ed.Fields().ByName("status")).String(); got != status {
		t.Errorf("conditions[%s].status = %q, want %q", condType, got, status)
	}
	if got := entry.Get(ed.Fields().ByName("reason")).String(); got != reason {
		t.Errorf("conditions[%s].reason = %q, want %q", condType, got, reason)
	}
	if got := entry.Get(ed.Fields().ByName("message")).String(); got != message {
		t.Errorf("conditions[%s].message = %q, want %q", condType, got, message)
	}
}

func assertHasField(t *testing.T, msg protoreflect.Message, name string) {
	t.Helper()
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		t.Fatalf("descriptor missing field %q", name)
	}
	if !msg.Has(fd) {
		t.Errorf("field %q unset", name)
	}
}

func assertMissingField(t *testing.T, msg protoreflect.Message, name string) {
	t.Helper()
	fd := msg.Descriptor().Fields().ByName(protoreflect.Name(name))
	if fd == nil {
		return
	}
	if msg.Has(fd) {
		t.Errorf("field %q should be unset", name)
	}
}
