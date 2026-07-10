package dynamicapi

import (
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RegisterFileAndDeps registers a file descriptor and all its transitive
// imports into the given Files registry, skipping files already present.
func RegisterFileAndDeps(files *protoregistry.Files, fd protoreflect.FileDescriptor) error {
	if _, err := files.FindFileByPath(string(fd.Path())); err == nil {
		return nil
	}
	for i := range fd.Imports().Len() {
		dep := fd.Imports().Get(i).FileDescriptor
		if err := RegisterFileAndDeps(files, dep); err != nil {
			return err
		}
	}
	return files.RegisterFile(fd)
}

// MarshalTimestamp converts a time.Time to a protoreflect.Value suitable
// for setting on a dynamic Timestamp field.
func MarshalTimestamp(field protoreflect.FieldDescriptor, t time.Time) (protoreflect.Value, error) {
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

// ExtractMapStringString reads a proto3 map<string,string> field from a
// dynamic message. Returns nil when the field is unset.
func ExtractMapStringString(msg protoreflect.Message, field protoreflect.FieldDescriptor) map[string]string {
	if field == nil || !msg.Has(field) {
		return nil
	}
	m := msg.Get(field).Map()
	result := make(map[string]string, m.Len())
	m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
		result[k.String()] = v.String()
		return true
	})
	return result
}

// SetMapStringString writes a Go map into a proto3 map<string,string>
// field on a dynamic message. An empty or nil map leaves the field unset.
func SetMapStringString(msg *dynamicpb.Message, field protoreflect.FieldDescriptor, m map[string]string) {
	if field == nil || len(m) == 0 {
		return
	}
	mapField := msg.Mutable(field).Map()
	for k, v := range m {
		mapField.Set(
			protoreflect.ValueOfString(k).MapKey(),
			protoreflect.ValueOfString(v),
		)
	}
}

// --- proto field builder helpers ---
//
// These construct descriptorpb.FieldDescriptorProto values used by
// both the extension and platform descriptor builders when
// programmatically synthesizing AIP-compliant proto services.

// StringField builds a proto3 string field descriptor.
func StringField(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

// Int32Field builds a proto3 int32 field descriptor.
func Int32Field(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

// Int64Field builds a proto3 int64 field descriptor.
func Int64Field(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

// BoolField builds a proto3 bool field descriptor.
func BoolField(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

// BytesField builds a proto3 bytes field descriptor.
func BytesField(name string, number int32) *descriptorpb.FieldDescriptorProto {
	return &descriptorpb.FieldDescriptorProto{
		Name:   proto.String(name),
		Number: proto.Int32(number),
		Type:   descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

// EnumField builds a proto3 enum field descriptor.
func EnumField(name string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
	fqn := typeName
	if !strings.HasPrefix(fqn, ".") {
		fqn = "." + fqn
	}
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(number),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(),
		TypeName: proto.String(fqn),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

// MessageField builds a proto3 message field descriptor.
func MessageField(name string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
	fqn := typeName
	if !strings.HasPrefix(fqn, ".") {
		fqn = "." + fqn
	}
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(number),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
		TypeName: proto.String(fqn),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
}

// RepeatedMessageField builds a proto3 repeated message field descriptor.
func RepeatedMessageField(name string, number int32, typeName string) *descriptorpb.FieldDescriptorProto {
	fqn := typeName
	if !strings.HasPrefix(fqn, ".") {
		fqn = "." + fqn
	}
	return &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(number),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
		TypeName: proto.String(fqn),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
	}
}

// MapFieldParts is both halves of a proto3 map: the repeated field and the
// nested MapEntry message that must be attached to the parent DescriptorProto.
type MapFieldParts struct {
	Field *descriptorpb.FieldDescriptorProto // LABEL_REPEATED, type = entry
	Entry *descriptorpb.DescriptorProto      // nested type, map_entry = true
}

// MapField builds both pieces of a proto3 map field. Callers must append
// Field to parent.Field and Entry to parent.NestedType (or use
// [AppendMapField]). Returning only the field is incorrect — the nested
// MapEntry message is required for protodesc / dynamicpb to treat the
// field as a map.
//
// parentFQN is the fully-qualified name of the parent message (without a
// leading dot), used to form the entry type_name
// (e.g. "kind.fleetshift.v1.Cluster" → ".kind.fleetshift.v1.Cluster.LabelsEntry").
//
// valueTypeName is empty for scalar values; for message values it is the
// fully-qualified type name (with or without a leading dot),
// e.g. ".pkg.Condition".
func MapField(
	parentFQN string,
	name string,
	number int32,
	keyType descriptorpb.FieldDescriptorProto_Type,
	valueType descriptorpb.FieldDescriptorProto_Type,
	valueTypeName string,
) MapFieldParts {
	entryName := mapEntryName(name)
	valueField := &descriptorpb.FieldDescriptorProto{
		Name:   proto.String("value"),
		Number: proto.Int32(2),
		Type:   valueType.Enum(),
		Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
	}
	if valueType == descriptorpb.FieldDescriptorProto_TYPE_MESSAGE ||
		valueType == descriptorpb.FieldDescriptorProto_TYPE_ENUM {
		fqn := valueTypeName
		if !strings.HasPrefix(fqn, ".") {
			fqn = "." + fqn
		}
		valueField.TypeName = proto.String(fqn)
	}

	entry := &descriptorpb.DescriptorProto{
		Name: proto.String(entryName),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:   proto.String("key"),
				Number: proto.Int32(1),
				Type:   keyType.Enum(),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
			},
			valueField,
		},
		Options: &descriptorpb.MessageOptions{
			MapEntry: proto.Bool(true),
		},
	}

	field := &descriptorpb.FieldDescriptorProto{
		Name:     proto.String(name),
		Number:   proto.Int32(number),
		Type:     descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum(),
		TypeName: proto.String("." + parentFQN + "." + entryName),
		Label:    descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum(),
	}
	return MapFieldParts{Field: field, Entry: entry}
}

// AppendMapField appends both halves of a [MapFieldParts] to parent.
func AppendMapField(parent *descriptorpb.DescriptorProto, parts MapFieldParts) {
	parent.Field = append(parent.Field, parts.Field)
	parent.NestedType = append(parent.NestedType, parts.Entry)
}

// mapEntryName converts a snake_case field name to a PascalCase MapEntry
// message name (e.g. "local_labels" → "LocalLabelsEntry").
func mapEntryName(fieldName string) string {
	parts := strings.Split(fieldName, "_")
	var b strings.Builder
	for _, p := range parts {
		if p == "" {
			continue
		}
		b.WriteString(strings.ToUpper(p[:1]))
		if len(p) > 1 {
			b.WriteString(p[1:])
		}
	}
	b.WriteString("Entry")
	return b.String()
}
