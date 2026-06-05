package cli

import (
	"fmt"
	"strings"

	"github.com/fleetshift/fleetshift-poc/fleetshift-cli/internal/output"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// resourceColumns returns table columns for the standard fields that
// every managed resource message contains (name, state, uid). Values
// are extracted via protoreflect so this works for dynamicpb messages
// without compile-time type knowledge.
func resourceColumns() []output.Column {
	return []output.Column{
		{Header: "Name", Value: reflectStringField("name")},
		{Header: "State", Value: func(m proto.Message) string {
			s := reflectEnumField("state")(m)
			fd := m.ProtoReflect().Descriptor().Fields().ByName("pause_reason")
			if fd != nil && m.ProtoReflect().Get(fd).String() != "" {
				s += " (Paused)"
			}
			return s
		}},
		{Header: "UID", Value: reflectStringField("uid")},
	}
}

func reflectStringField(name protoreflect.Name) func(proto.Message) string {
	return func(m proto.Message) string {
		fd := m.ProtoReflect().Descriptor().Fields().ByName(name)
		if fd == nil {
			return ""
		}
		return m.ProtoReflect().Get(fd).String()
	}
}

func reflectEnumField(name protoreflect.Name) func(proto.Message) string {
	return func(m proto.Message) string {
		fd := m.ProtoReflect().Descriptor().Fields().ByName(name)
		if fd == nil {
			return ""
		}
		enumNum := m.ProtoReflect().Get(fd).Enum()
		enumDesc := fd.Enum().Values().ByNumber(enumNum)
		if enumDesc == nil {
			return fmt.Sprintf("%d", enumNum)
		}
		return trimStatePrefix(string(enumDesc.Name()))
	}
}

// trimStatePrefix removes a common enum prefix (e.g. "STATE_") for
// more readable table output.
func trimStatePrefix(s string) string {
	if after, ok := strings.CutPrefix(s, "STATE_"); ok {
		return after
	}
	return s
}
