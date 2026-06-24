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
