package dynamicapi_test

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

func TestMapField_StringString_AppendsFieldAndEntry(t *testing.T) {
	parent := &descriptorpb.DescriptorProto{Name: proto.String("Widget")}
	parts := dynamicapi.MapField(
		"test.v1.Widget",
		"labels",
		3,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		"",
	)
	dynamicapi.AppendMapField(parent, parts)

	if len(parent.Field) != 1 {
		t.Fatalf("parent.Field len = %d, want 1", len(parent.Field))
	}
	if len(parent.NestedType) != 1 {
		t.Fatalf("parent.NestedType len = %d, want 1", len(parent.NestedType))
	}

	field := parent.Field[0]
	if field.GetName() != "labels" {
		t.Errorf("field name = %q, want labels", field.GetName())
	}
	if field.GetNumber() != 3 {
		t.Errorf("field number = %d, want 3", field.GetNumber())
	}
	if field.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_REPEATED {
		t.Errorf("field label = %v, want REPEATED", field.GetLabel())
	}
	if field.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		t.Errorf("field type = %v, want MESSAGE", field.GetType())
	}
	if field.GetTypeName() != ".test.v1.Widget.LabelsEntry" {
		t.Errorf("field type_name = %q, want .test.v1.Widget.LabelsEntry", field.GetTypeName())
	}

	entry := parent.NestedType[0]
	if entry.GetName() != "LabelsEntry" {
		t.Errorf("entry name = %q, want LabelsEntry", entry.GetName())
	}
	if entry.Options == nil || !entry.Options.GetMapEntry() {
		t.Error("entry map_entry option not set")
	}
	if len(entry.Field) != 2 {
		t.Fatalf("entry fields = %d, want 2", len(entry.Field))
	}
	if entry.Field[0].GetName() != "key" || entry.Field[0].GetNumber() != 1 {
		t.Errorf("key field = %s #%d", entry.Field[0].GetName(), entry.Field[0].GetNumber())
	}
	if entry.Field[1].GetName() != "value" || entry.Field[1].GetNumber() != 2 {
		t.Errorf("value field = %s #%d", entry.Field[1].GetName(), entry.Field[1].GetNumber())
	}
}

func TestMapField_StringMessage_ValueTypeName(t *testing.T) {
	parts := dynamicapi.MapField(
		"test.v1.Widget",
		"conditions",
		41,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		descriptorpb.FieldDescriptorProto_TYPE_MESSAGE,
		".test.v1.Condition",
	)
	if parts.Entry.GetName() != "ConditionsEntry" {
		t.Errorf("entry name = %q, want ConditionsEntry", parts.Entry.GetName())
	}
	value := parts.Entry.Field[1]
	if value.GetType() != descriptorpb.FieldDescriptorProto_TYPE_MESSAGE {
		t.Errorf("value type = %v, want MESSAGE", value.GetType())
	}
	if value.GetTypeName() != ".test.v1.Condition" {
		t.Errorf("value type_name = %q, want .test.v1.Condition", value.GetTypeName())
	}
}

func TestMapField_MessageValue_EmptyTypeNamePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for empty valueTypeName with TYPE_MESSAGE")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("panic value type = %T, want string", r)
		}
		if !strings.Contains(msg, "valueTypeName") {
			t.Errorf("panic = %q, want mention of valueTypeName", msg)
		}
	}()
	dynamicapi.MapField(
		"test.v1.Widget",
		"conditions",
		41,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		descriptorpb.FieldDescriptorProto_TYPE_MESSAGE,
		"",
	)
}

func TestMapField_EnumValue_EmptyTypeNamePanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty valueTypeName with TYPE_ENUM")
		}
	}()
	dynamicapi.MapField(
		"test.v1.Widget",
		"states",
		42,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		descriptorpb.FieldDescriptorProto_TYPE_ENUM,
		"",
	)
}

func TestMapField_MessageValue_NormalizesTypeName(t *testing.T) {
	parts := dynamicapi.MapField(
		"test.v1.Widget",
		"conditions",
		41,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		descriptorpb.FieldDescriptorProto_TYPE_MESSAGE,
		"test.v1.Condition",
	)
	value := parts.Entry.Field[1]
	if value.GetTypeName() != ".test.v1.Condition" {
		t.Errorf("value type_name = %q, want .test.v1.Condition", value.GetTypeName())
	}
}

func TestMapField_EntryName_SnakeToPascal(t *testing.T) {
	parts := dynamicapi.MapField(
		"pkg.Msg",
		"local_labels",
		40,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		"",
	)
	if parts.Entry.GetName() != "LocalLabelsEntry" {
		t.Errorf("entry name = %q, want LocalLabelsEntry", parts.Entry.GetName())
	}
	if parts.Field.GetTypeName() != ".pkg.Msg.LocalLabelsEntry" {
		t.Errorf("type_name = %q, want .pkg.Msg.LocalLabelsEntry", parts.Field.GetTypeName())
	}
}

func TestMapField_DynamicpbRoundTrip(t *testing.T) {
	fd := buildMapTestFile(t)
	msgDesc := fd.Messages().ByName("Widget")
	labelsField := msgDesc.Fields().ByName("labels")
	if labelsField == nil || !labelsField.IsMap() {
		t.Fatal("labels field missing or not a map")
	}

	msg := dynamicpb.NewMessage(msgDesc)
	m := msg.Mutable(labelsField).Map()
	m.Set(
		protoreflect.ValueOfString("env").MapKey(),
		protoreflect.ValueOfString("prod"),
	)
	m.Set(
		protoreflect.ValueOfString("team").MapKey(),
		protoreflect.ValueOfString("platform"),
	)

	got := msg.Get(labelsField).Map()
	if got.Len() != 2 {
		t.Fatalf("map len = %d, want 2", got.Len())
	}
	if v := got.Get(protoreflect.ValueOfString("env").MapKey()).String(); v != "prod" {
		t.Errorf("labels[env] = %q, want prod", v)
	}
}

func TestMapField_ProtoJSON(t *testing.T) {
	fd := buildMapTestFile(t)
	msgDesc := fd.Messages().ByName("Widget")
	labelsField := msgDesc.Fields().ByName("labels")

	msg := dynamicpb.NewMessage(msgDesc)
	m := msg.Mutable(labelsField).Map()
	m.Set(
		protoreflect.ValueOfString("a").MapKey(),
		protoreflect.ValueOfString("1"),
	)

	b, err := protojson.Marshal(msg)
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}
	if !containsAll(string(b), `"labels"`, `"a"`, `"1"`) {
		t.Errorf("protojson = %s, want labels map with a=1", b)
	}

	out := dynamicpb.NewMessage(msgDesc)
	if err := protojson.Unmarshal(b, out); err != nil {
		t.Fatalf("protojson.Unmarshal: %v", err)
	}
	got := out.Get(labelsField).Map().Get(protoreflect.ValueOfString("a").MapKey()).String()
	if got != "1" {
		t.Errorf("round-trip labels[a] = %q, want 1", got)
	}
}

func TestMapField_MapOfMessage_Mutable(t *testing.T) {
	fd := buildMapOfMessageTestFile(t)
	msgDesc := fd.Messages().ByName("Widget")
	condField := msgDesc.Fields().ByName("conditions")
	if condField == nil || !condField.IsMap() {
		t.Fatal("conditions field missing or not a map")
	}

	msg := dynamicpb.NewMessage(msgDesc)
	m := msg.Mutable(condField).Map()
	entry := m.Mutable(protoreflect.ValueOfString("Ready").MapKey()).Message()
	statusField := entry.Descriptor().Fields().ByName("status")
	entry.Set(statusField, protoreflect.ValueOfString("True"))

	got := msg.Get(condField).Map().Get(protoreflect.ValueOfString("Ready").MapKey()).Message()
	if got.Get(statusField).String() != "True" {
		t.Errorf("conditions[Ready].status = %q, want True", got.Get(statusField).String())
	}
}

func TestMapField_Deterministic(t *testing.T) {
	a := dynamicapi.MapField("pkg.M", "local_labels", 40,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		descriptorpb.FieldDescriptorProto_TYPE_STRING, "")
	b := dynamicapi.MapField("pkg.M", "local_labels", 40,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		descriptorpb.FieldDescriptorProto_TYPE_STRING, "")
	if !proto.Equal(a.Field, b.Field) {
		t.Error("Field not deterministic")
	}
	if !proto.Equal(a.Entry, b.Entry) {
		t.Error("Entry not deterministic")
	}
}

func buildMapTestFile(t *testing.T) protoreflect.FileDescriptor {
	t.Helper()
	widget := &descriptorpb.DescriptorProto{Name: proto.String("Widget")}
	dynamicapi.AppendMapField(widget, dynamicapi.MapField(
		"test.v1.Widget",
		"labels",
		1,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		"",
	))
	fdp := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("test/map_widget.proto"),
		Package:     proto.String("test.v1"),
		Syntax:      proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{widget},
	}
	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		t.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd
}

func buildMapOfMessageTestFile(t *testing.T) protoreflect.FileDescriptor {
	t.Helper()
	condition := &descriptorpb.DescriptorProto{
		Name: proto.String("Condition"),
		Field: []*descriptorpb.FieldDescriptorProto{
			dynamicapi.StringField("status", 1),
			dynamicapi.StringField("reason", 2),
			dynamicapi.StringField("message", 3),
		},
	}
	widget := &descriptorpb.DescriptorProto{Name: proto.String("Widget")}
	dynamicapi.AppendMapField(widget, dynamicapi.MapField(
		"test.v1.Widget",
		"conditions",
		1,
		descriptorpb.FieldDescriptorProto_TYPE_STRING,
		descriptorpb.FieldDescriptorProto_TYPE_MESSAGE,
		".test.v1.Condition",
	))
	fdp := &descriptorpb.FileDescriptorProto{
		Name:        proto.String("test/map_condition.proto"),
		Package:     proto.String("test.v1"),
		Syntax:      proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{condition, widget},
	}
	files := new(protoregistry.Files)
	fd, err := protodesc.NewFile(fdp, files)
	if err != nil {
		t.Fatalf("protodesc.NewFile: %v", err)
	}
	return fd
}

func containsAll(s string, parts ...string) bool {
	for _, p := range parts {
		if !strings.Contains(s, p) {
			return false
		}
	}
	return true
}
