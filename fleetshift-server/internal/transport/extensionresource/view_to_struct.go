package extensionresource

import (
	"encoding/json"
	"fmt"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/transport/dynamicapi"
)

// ViewToResource converts a domain ExtensionResourceView into a dynamic
// extension-resource message. descs must be the activated service
// descriptors for the view's resource type. cfg drives which capability
// field sets are projected:
//
//   - common envelope (name, uid, labels, timestamps, etag) always
//   - management fields when cfg.Capabilities.Management != nil
//   - observed-state fields when cfg.Capabilities.Inventory != nil and
//     the view has a latest inventory report
func ViewToResource(descs *ExtensionServiceDescriptors, cfg *ResourceTypeConfig, v domain.ExtensionResourceView) (proto.Message, error) {
	if descs == nil || descs.Resource == nil {
		return nil, fmt.Errorf("%w: service descriptors are required to project resource body", domain.ErrInvalidArgument)
	}
	if cfg == nil {
		return nil, fmt.Errorf("%w: resource type config is required to project resource body", domain.ErrInvalidArgument)
	}

	resource := dynamicpb.NewMessage(descs.Resource)
	if err := populateCommonFields(descs, resource, v); err != nil {
		return nil, err
	}
	if cfg.Capabilities.Management != nil {
		if err := populateManagementFields(descs, resource, v); err != nil {
			return nil, err
		}
	}
	if cfg.Capabilities.Inventory != nil {
		if err := populateObservedStateFields(descs, resource, v); err != nil {
			return nil, err
		}
	}
	return resource, nil
}

func populateCommonFields(descs *ExtensionServiceDescriptors, resource *dynamicpb.Message, v domain.ExtensionResourceView) error {
	er := v.Resource

	// name — ResourceName is already collection-qualified (e.g. "widgets/widget-1")
	nameField := descs.Resource.Fields().ByName("name")
	resource.Set(nameField, protoreflect.ValueOfString(string(er.Name())))

	uidField := descs.Resource.Fields().ByName("uid")
	resource.Set(uidField, protoreflect.ValueOfString(er.UID().String()))

	labelsField := descs.Resource.Fields().ByName("labels")
	dynamicapi.SetMapStringString(resource, labelsField, er.Labels())

	if !er.CreatedAt().IsZero() {
		createTimeField := descs.Resource.Fields().ByName("create_time")
		if createTimeField != nil {
			tsVal, err := dynamicapi.MarshalTimestamp(createTimeField, er.CreatedAt())
			if err != nil {
				return err
			}
			resource.Set(createTimeField, tsVal)
		}
	}

	if !er.UpdatedAt().IsZero() {
		updateTimeField := descs.Resource.Fields().ByName("update_time")
		if updateTimeField != nil {
			tsVal, err := dynamicapi.MarshalTimestamp(updateTimeField, er.UpdatedAt())
			if err != nil {
				return err
			}
			resource.Set(updateTimeField, tsVal)
		}
	}

	if etagField := descs.Resource.Fields().ByName("etag"); etagField != nil {
		resource.Set(etagField, protoreflect.ValueOfString(string(v.Etag())))
	}
	return nil
}

func populateManagementFields(descs *ExtensionServiceDescriptors, resource *dynamicpb.Message, v domain.ExtensionResourceView) error {
	er := v.Resource

	specField := descs.Resource.Fields().ByName("spec")
	if specField != nil && descs.Spec != nil {
		specMsg := dynamicpb.NewMessage(descs.Spec)
		if v.Intent != nil && len(v.Intent.Spec) > 0 {
			if err := protojson.Unmarshal(v.Intent.Spec, specMsg); err != nil {
				return status.Errorf(codes.Internal, "unmarshal spec: %v", err)
			}
		}
		resource.Set(specField, protoreflect.ValueOfMessage(specMsg))
	}

	if versionField := descs.Resource.Fields().ByName("intent_version"); versionField != nil {
		if managed := er.Managed(); managed != nil {
			resource.Set(versionField, protoreflect.ValueOfInt64(int64(managed.CurrentVersion())))
		}
	}

	if v.Fulfillment == nil {
		return nil
	}
	f := v.Fulfillment
	if stateField := descs.Resource.Fields().ByName("state"); stateField != nil {
		stateNum := int32(stateFromFulfillment(f.State()))
		resource.Set(stateField, protoreflect.ValueOfEnum(protoreflect.EnumNumber(stateNum)))
	}

	if prField := descs.Resource.Fields().ByName("pause_reason"); prField != nil {
		resource.Set(prField, protoreflect.ValueOfString(f.PauseReason()))
	}

	if reconcilingField := descs.Resource.Fields().ByName("reconciling"); reconcilingField != nil {
		resource.Set(reconcilingField, protoreflect.ValueOfBool(f.Reconciling()))
	}

	if f.Provenance() != nil {
		if provField := descs.Resource.Fields().ByName("provenance"); provField != nil {
			provVal, err := marshalProvenance(provField, f.Provenance())
			if err != nil {
				return err
			}
			resource.Set(provField, provVal)
		}
	}

	if genField := descs.Resource.Fields().ByName("generation"); genField != nil {
		resource.Set(genField, protoreflect.ValueOfInt64(int64(f.Generation())))
	}
	return nil
}

func populateObservedStateFields(descs *ExtensionServiceDescriptors, resource *dynamicpb.Message, v domain.ExtensionResourceView) error {
	inv := v.Resource.Inventory()
	if inv == nil {
		// Inventory capability present but no report yet — leave
		// observed-state fields unset.
		return nil
	}

	localLabelsField := descs.Resource.Fields().ByName("local_labels")
	dynamicapi.SetMapStringString(resource, localLabelsField, inv.Labels())

	if err := setConditionsMap(descs, resource, inv.Conditions()); err != nil {
		return err
	}

	if obs := inv.Observation(); obs != nil {
		obsField := descs.Resource.Fields().ByName("observation")
		if obsField != nil {
			obsVal, err := marshalObservationStruct(obsField, *obs)
			if err != nil {
				return err
			}
			resource.Set(obsField, obsVal)
		}
	}

	if !inv.ObservedAt().IsZero() {
		localUpdateField := descs.Resource.Fields().ByName("local_update_time")
		if localUpdateField != nil {
			tsVal, err := dynamicapi.MarshalTimestamp(localUpdateField, inv.ObservedAt())
			if err != nil {
				return err
			}
			resource.Set(localUpdateField, tsVal)
		}
	}

	if !inv.UpdatedAt().IsZero() {
		indexUpdateField := descs.Resource.Fields().ByName("index_update_time")
		if indexUpdateField != nil {
			tsVal, err := dynamicapi.MarshalTimestamp(indexUpdateField, inv.UpdatedAt())
			if err != nil {
				return err
			}
			resource.Set(indexUpdateField, tsVal)
		}
	}
	return nil
}

func setConditionsMap(descs *ExtensionServiceDescriptors, resource *dynamicpb.Message, conditions []domain.Condition) error {
	condField := descs.Resource.Fields().ByName("conditions")
	if condField == nil || len(conditions) == 0 {
		return nil
	}

	m := resource.Mutable(condField).Map()
	seen := make(map[domain.ConditionType]struct{}, len(conditions))
	for _, c := range conditions {
		ct := c.Type()
		if _, dup := seen[ct]; dup {
			return status.Errorf(codes.Internal, "duplicate condition type %q in inventory state", ct)
		}
		seen[ct] = struct{}{}

		entry := m.Mutable(protoreflect.ValueOfString(string(ct)).MapKey()).Message()
		entryDesc := entry.Descriptor()
		if statusField := entryDesc.Fields().ByName("status"); statusField != nil {
			entry.Set(statusField, protoreflect.ValueOfString(string(c.Status())))
		}
		if reasonField := entryDesc.Fields().ByName("reason"); reasonField != nil {
			entry.Set(reasonField, protoreflect.ValueOfString(c.Reason()))
		}
		if messageField := entryDesc.Fields().ByName("message"); messageField != nil {
			entry.Set(messageField, protoreflect.ValueOfString(c.Message()))
		}
		if lttField := entryDesc.Fields().ByName("last_transition_time"); lttField != nil && !c.LastTransitionTime().IsZero() {
			tsVal, err := dynamicapi.MarshalTimestamp(lttField, c.LastTransitionTime())
			if err != nil {
				return err
			}
			entry.Set(lttField, tsVal)
		}
	}
	return nil
}

func marshalObservationStruct(field protoreflect.FieldDescriptor, raw json.RawMessage) (protoreflect.Value, error) {
	s := &structpb.Struct{}
	if len(raw) > 0 {
		if err := protojson.Unmarshal(raw, s); err != nil {
			return protoreflect.Value{}, status.Errorf(codes.Internal, "unmarshal observation: %v", err)
		}
	}
	obsMsg := dynamicpb.NewMessage(field.Message())
	b, err := proto.Marshal(s)
	if err != nil {
		return protoreflect.Value{}, status.Errorf(codes.Internal, "marshal observation: %v", err)
	}
	if err := proto.Unmarshal(b, obsMsg); err != nil {
		return protoreflect.Value{}, status.Errorf(codes.Internal, "unmarshal observation message: %v", err)
	}
	return protoreflect.ValueOfMessage(obsMsg), nil
}

// ViewToStruct projects a view into a google.protobuf.Struct with the
// same field set as the dynamic extension-resource Get/List body
// (protojson casing), including labels and observed-state fields when
// the type's capabilities declare them.
func ViewToStruct(descs *ExtensionServiceDescriptors, cfg *ResourceTypeConfig, v domain.ExtensionResourceView) (*structpb.Struct, error) {
	msg, err := ViewToResource(descs, cfg, v)
	if err != nil {
		return nil, err
	}
	b, err := protojson.Marshal(msg)
	if err != nil {
		return nil, fmt.Errorf("marshal resource to json: %w", err)
	}
	out := &structpb.Struct{}
	if err := protojson.Unmarshal(b, out); err != nil {
		return nil, fmt.Errorf("unmarshal resource json to struct: %w", err)
	}
	return out, nil
}
