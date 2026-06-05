package cli

import (
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

// buildTestResourceDesc creates a minimal FileDescriptor containing a resource
// message with name (string), state (enum), uid (string), and pause_reason (string).
func buildTestResourceDesc() protoreflect.MessageDescriptor {
	enumName := "State"
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    proto.String("test.proto"),
		Package: proto.String("test"),
		Syntax:  proto.String("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{
				Name: proto.String("TestResource"),
				EnumType: []*descriptorpb.EnumDescriptorProto{
					{
						Name: proto.String(enumName),
						Value: []*descriptorpb.EnumValueDescriptorProto{
							{Name: proto.String("STATE_UNSPECIFIED"), Number: proto.Int32(0)},
							{Name: proto.String("CREATING"), Number: proto.Int32(1)},
							{Name: proto.String("ACTIVE"), Number: proto.Int32(2)},
						},
					},
				},
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name:   proto.String("name"),
						Number: proto.Int32(1),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					},
					{
						Name:   proto.String("uid"),
						Number: proto.Int32(2),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					},
					{
						Name:     proto.String("state"),
						Number:   proto.Int32(5),
						Type:     descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum(),
						TypeName: proto.String(".test.TestResource.State"),
						Label:    descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					},
					{
						Name:   proto.String("pause_reason"),
						Number: proto.Int32(13),
						Type:   descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(),
						Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
					},
				},
			},
		},
	}

	fd, err := protodesc.NewFile(fdp, nil)
	if err != nil {
		panic(err)
	}
	return fd.Messages().ByName("TestResource")
}

func TestResourceColumns_StateShowsPaused(t *testing.T) {
	msgDesc := buildTestResourceDesc()
	cols := resourceColumns()

	stateCol := cols[1]
	if stateCol.Header != "State" {
		t.Fatalf("expected column 1 to be State, got %q", stateCol.Header)
	}

	tests := []struct {
		name        string
		state       int32
		pauseReason string
		want        string
	}{
		{
			name:        "creating without pause",
			state:       1,
			pauseReason: "",
			want:        "CREATING",
		},
		{
			name:        "creating with pause reason",
			state:       1,
			pauseReason: "delivery auth failed",
			want:        "CREATING (Paused)",
		},
		{
			name:        "active without pause",
			state:       2,
			pauseReason: "",
			want:        "ACTIVE",
		},
		{
			name:        "active with pause reason",
			state:       2,
			pauseReason: "some pause reason",
			want:        "ACTIVE (Paused)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := dynamicpb.NewMessage(msgDesc)
			msg.Set(msgDesc.Fields().ByName("state"), protoreflect.ValueOfEnum(protoreflect.EnumNumber(tt.state)))
			if tt.pauseReason != "" {
				msg.Set(msgDesc.Fields().ByName("pause_reason"), protoreflect.ValueOfString(tt.pauseReason))
			}

			got := stateCol.Value(msg)
			if got != tt.want {
				t.Errorf("got %q, want %q", got, tt.want)
			}
		})
	}
}
