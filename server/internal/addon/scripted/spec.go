package scripted

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"buf.build/go/protovalidate"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

// Codec compiles the embedded proto schema once and reuses the cached
// descriptor and validator for every spec decode. It is safe for
// concurrent use after construction.
type Codec struct {
	desc      *dynamicapi.SpecDescriptor
	validator protovalidate.Validator
}

// NewCodec compiles the embedded scripted resource proto and builds a
// protovalidate validator. It should be called once during add-on
// assembly; failure is a pre-ready startup error.
func NewCodec(ctx context.Context) (*Codec, error) {
	desc, err := dynamicapi.CompileInline(
		ctx,
		Schema().ProtoFiles,
		specProtoPath,
		protoreflect.FullName(SpecMessage),
	)
	if err != nil {
		return nil, fmt.Errorf("scripted: compile spec proto: %w", err)
	}

	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("scripted: create validator: %w", err)
	}

	return &Codec{
		desc:      desc,
		validator: validator,
	}, nil
}

// Decode unmarshals raw JSON into a validated, normalized spec. It
// rejects unknown fields, validates buf.validate constraints, and
// applies prompt-success defaults for omitted behavior.
func (c *Codec) Decode(raw json.RawMessage) (*NormalizedSpec, error) {
	msg := dynamicpb.NewMessage(c.desc.Message)

	opts := protojson.UnmarshalOptions{
		DiscardUnknown: false,
	}
	if err := opts.Unmarshal(raw, msg); err != nil {
		return nil, fmt.Errorf("scripted: unmarshal spec: %w", err)
	}

	if err := c.validator.Validate(msg); err != nil {
		return nil, fmt.Errorf("scripted: validate spec: %w", err)
	}

	return normalizeSpec(msg, c.desc.Message)
}

// promptSuccess is the default phase behavior: zero delay, constant
// success.
var promptSuccess = PhaseBehavior{
	Latency: ConstantLatency{Duration: 0},
	Outcome: ConstantOutcome{Value: OutcomeSuccess},
}

// normalizeSpec extracts an immutable NormalizedSpec from a validated
// dynamic message.
func normalizeSpec(msg *dynamicpb.Message, desc protoreflect.MessageDescriptor) (*NormalizedSpec, error) {
	spec := &NormalizedSpec{
		Delivery: OperationSpec{
			Acknowledgement: promptSuccess,
			Completion:      promptSuccess,
		},
		Removal: OperationSpec{
			Acknowledgement: promptSuccess,
			Completion:      promptSuccess,
		},
	}

	behaviorField := desc.Fields().ByName("behavior")
	if behaviorField != nil && msg.Has(behaviorField) {
		behaviorMsg := msg.Get(behaviorField).Message()
		behaviorDesc := behaviorField.Message()

		deliveryField := behaviorDesc.Fields().ByName("delivery")
		if deliveryField != nil && behaviorMsg.Has(deliveryField) {
			op, err := normalizePhasePair(behaviorMsg.Get(deliveryField).Message(), deliveryField.Message())
			if err != nil {
				return nil, fmt.Errorf("delivery: %w", err)
			}
			spec.Delivery = op
		}

		removalField := behaviorDesc.Fields().ByName("removal")
		if removalField != nil && behaviorMsg.Has(removalField) {
			op, err := normalizePhasePair(behaviorMsg.Get(removalField).Message(), removalField.Message())
			if err != nil {
				return nil, fmt.Errorf("removal: %w", err)
			}
			spec.Removal = op
		}
	}

	inventoryField := desc.Fields().ByName("inventory")
	if inventoryField != nil && msg.Has(inventoryField) {
		inv, err := normalizeInventory(msg.Get(inventoryField).Message(), inventoryField.Message())
		if err != nil {
			return nil, fmt.Errorf("inventory: %w", err)
		}
		spec.Inventory = inv
	}

	return spec, nil
}

func normalizePhasePair(msg protoreflect.Message, desc protoreflect.MessageDescriptor) (OperationSpec, error) {
	op := OperationSpec{
		Acknowledgement: promptSuccess,
		Completion:      promptSuccess,
	}

	ackField := desc.Fields().ByName("acknowledgement")
	if ackField != nil && msg.Has(ackField) {
		pb, err := normalizePhaseSpec(msg.Get(ackField).Message(), ackField.Message())
		if err != nil {
			return op, fmt.Errorf("acknowledgement: %w", err)
		}
		op.Acknowledgement = pb
	}

	compField := desc.Fields().ByName("completion")
	if compField != nil && msg.Has(compField) {
		pb, err := normalizePhaseSpec(msg.Get(compField).Message(), compField.Message())
		if err != nil {
			return op, fmt.Errorf("completion: %w", err)
		}
		op.Completion = pb
	}

	return op, nil
}

func normalizePhaseSpec(msg protoreflect.Message, desc protoreflect.MessageDescriptor) (PhaseBehavior, error) {
	pb := promptSuccess

	latencyField := desc.Fields().ByName("latency")
	if latencyField != nil && msg.Has(latencyField) {
		l, err := normalizeLatency(msg.Get(latencyField).Message(), latencyField.Message())
		if err != nil {
			return pb, fmt.Errorf("latency: %w", err)
		}
		pb.Latency = l
	}

	outcomeField := desc.Fields().ByName("outcome")
	if outcomeField != nil && msg.Has(outcomeField) {
		o, err := normalizeOutcome(msg.Get(outcomeField).Message(), outcomeField.Message())
		if err != nil {
			return pb, fmt.Errorf("outcome: %w", err)
		}
		pb.Outcome = o
	}

	return pb, nil
}

func normalizeLatency(msg protoreflect.Message, desc protoreflect.MessageDescriptor) (LatencyDecider, error) {
	constantField := desc.Fields().ByName("constant")
	if constantField != nil && msg.Has(constantField) {
		durMsg := msg.Get(constantField).Message()
		seconds := durMsg.Get(durMsg.Descriptor().Fields().ByName("seconds")).Int()
		nanos := durMsg.Get(durMsg.Descriptor().Fields().ByName("nanos")).Int()
		d := time.Duration(seconds)*time.Second + time.Duration(nanos)*time.Nanosecond
		return ConstantLatency{Duration: d}, nil
	}
	// Validation should have caught missing oneof; defensive default.
	return ConstantLatency{Duration: 0}, nil
}

func normalizeOutcome(msg protoreflect.Message, desc protoreflect.MessageDescriptor) (OutcomeDecider, error) {
	constantField := desc.Fields().ByName("constant")
	if constantField != nil && msg.Has(constantField) {
		v := msg.Get(constantField).Enum()
		ov, err := protoEnumToOutcome(int32(v))
		if err != nil {
			return nil, err
		}
		return ConstantOutcome{Value: ov}, nil
	}

	sequenceField := desc.Fields().ByName("sequence")
	if sequenceField != nil && msg.Has(sequenceField) {
		seqMsg := msg.Get(sequenceField).Message()
		valuesField := seqMsg.Descriptor().Fields().ByName("values")
		if valuesField == nil {
			return nil, fmt.Errorf("sequence missing values field")
		}
		list := seqMsg.Get(valuesField).List()
		values := make([]OutcomeValue, list.Len())
		for i := range list.Len() {
			ov, err := protoEnumToOutcome(int32(list.Get(i).Enum()))
			if err != nil {
				return nil, fmt.Errorf("sequence[%d]: %w", i, err)
			}
			values[i] = ov
		}
		return SequenceOutcome{Values: values}, nil
	}

	// Validation should have caught missing oneof; defensive default.
	return ConstantOutcome{Value: OutcomeSuccess}, nil
}

func normalizeInventory(msg protoreflect.Message, desc protoreflect.MessageDescriptor) (InventoryProjection, error) {
	inv := InventoryProjection{}

	labelsField := desc.Fields().ByName("labels")
	if labelsField != nil && msg.Has(labelsField) {
		m := msg.Get(labelsField).Map()
		inv.Labels = make(map[string]string, m.Len())
		m.Range(func(k protoreflect.MapKey, v protoreflect.Value) bool {
			inv.Labels[k.String()] = v.String()
			return true
		})
	}

	observationField := desc.Fields().ByName("observation")
	if observationField != nil && msg.Has(observationField) {
		obsMsg := msg.Get(observationField).Message()
		// Marshal the Struct back to JSON for storage.
		dm, ok := obsMsg.Interface().(*dynamicpb.Message)
		if !ok {
			return inv, fmt.Errorf("observation: unexpected message type %T", obsMsg.Interface())
		}
		raw, err := protojson.Marshal(dm)
		if err != nil {
			return inv, fmt.Errorf("observation: marshal: %w", err)
		}
		inv.Observation = json.RawMessage(raw)
	}

	return inv, nil
}

func protoEnumToOutcome(v int32) (OutcomeValue, error) {
	switch v {
	case 1:
		return OutcomeSuccess, nil
	case 2:
		return OutcomeFailure, nil
	default:
		return 0, fmt.Errorf("invalid outcome value %d", v)
	}
}
