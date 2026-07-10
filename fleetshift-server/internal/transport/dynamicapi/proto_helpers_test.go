package dynamicapi_test

import (
	"testing"

	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

func TestInt64Field(t *testing.T) {
	f := dynamicapi.Int64Field("generation", 25)
	if f.GetName() != "generation" {
		t.Errorf("name = %q, want generation", f.GetName())
	}
	if f.GetNumber() != 25 {
		t.Errorf("number = %d, want 25", f.GetNumber())
	}
	if f.GetType() != descriptorpb.FieldDescriptorProto_TYPE_INT64 {
		t.Errorf("type = %v, want TYPE_INT64", f.GetType())
	}
	if f.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL {
		t.Errorf("label = %v, want LABEL_OPTIONAL", f.GetLabel())
	}
}

func TestBoolField(t *testing.T) {
	f := dynamicapi.BoolField("reconciling", 23)
	if f.GetName() != "reconciling" {
		t.Errorf("name = %q, want reconciling", f.GetName())
	}
	if f.GetNumber() != 23 {
		t.Errorf("number = %d, want 23", f.GetNumber())
	}
	if f.GetType() != descriptorpb.FieldDescriptorProto_TYPE_BOOL {
		t.Errorf("type = %v, want TYPE_BOOL", f.GetType())
	}
	if f.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL {
		t.Errorf("label = %v, want LABEL_OPTIONAL", f.GetLabel())
	}
}

func TestBytesField(t *testing.T) {
	f := dynamicapi.BytesField("user_signature", 3)
	if f.GetName() != "user_signature" {
		t.Errorf("name = %q, want user_signature", f.GetName())
	}
	if f.GetNumber() != 3 {
		t.Errorf("number = %d, want 3", f.GetNumber())
	}
	if f.GetType() != descriptorpb.FieldDescriptorProto_TYPE_BYTES {
		t.Errorf("type = %v, want TYPE_BYTES", f.GetType())
	}
	if f.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL {
		t.Errorf("label = %v, want LABEL_OPTIONAL", f.GetLabel())
	}
}

func TestEnumField(t *testing.T) {
	t.Run("adds leading dot", func(t *testing.T) {
		f := dynamicapi.EnumField("state", 22, "kind.v1.Cluster.State")
		if f.GetType() != descriptorpb.FieldDescriptorProto_TYPE_ENUM {
			t.Errorf("type = %v, want TYPE_ENUM", f.GetType())
		}
		if f.GetTypeName() != ".kind.v1.Cluster.State" {
			t.Errorf("type_name = %q, want .kind.v1.Cluster.State", f.GetTypeName())
		}
		if f.GetLabel() != descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL {
			t.Errorf("label = %v, want LABEL_OPTIONAL", f.GetLabel())
		}
	})
	t.Run("preserves leading dot", func(t *testing.T) {
		f := dynamicapi.EnumField("state", 22, ".kind.v1.Cluster.State")
		if f.GetTypeName() != ".kind.v1.Cluster.State" {
			t.Errorf("type_name = %q, want .kind.v1.Cluster.State", f.GetTypeName())
		}
	})
}
