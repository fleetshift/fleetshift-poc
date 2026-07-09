package managedresource

import (
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
// managed-resource message populated with the platform envelope and
// addon spec. descs must be the activated service descriptors for the
// view's resource type.
func ViewToResource(descs *ServiceDescriptors, v domain.ExtensionResourceView) (proto.Message, error) {
	if descs == nil || descs.Resource == nil || descs.Spec == nil {
		return nil, fmt.Errorf("%w: service descriptors are required to project resource body", domain.ErrInvalidArgument)
	}

	er := v.Resource
	resource := dynamicpb.NewMessage(descs.Resource)

	// name — ResourceName is already collection-qualified (e.g. "widgets/widget-1")
	nameField := descs.Resource.Fields().ByName("name")
	resource.Set(nameField, protoreflect.ValueOfString(string(er.Name())))

	// uid
	uidField := descs.Resource.Fields().ByName("uid")
	resource.Set(uidField, protoreflect.ValueOfString(er.UID().String()))

	// spec
	specField := descs.Resource.Fields().ByName("spec")
	specMsg := dynamicpb.NewMessage(descs.Spec)
	if v.Intent != nil && len(v.Intent.Spec) > 0 {
		if err := protojson.Unmarshal(v.Intent.Spec, specMsg); err != nil {
			return nil, status.Errorf(codes.Internal, "unmarshal spec: %v", err)
		}
	}
	resource.Set(specField, protoreflect.ValueOfMessage(specMsg))

	// intent_version
	versionField := descs.Resource.Fields().ByName("intent_version")
	if managed := er.Managed(); managed != nil {
		resource.Set(versionField, protoreflect.ValueOfInt64(int64(managed.CurrentVersion())))
	}

	// state and fulfillment-derived fields
	if v.Fulfillment != nil {
		f := v.Fulfillment
		stateField := descs.Resource.Fields().ByName("state")
		stateNum := int32(stateFromFulfillment(f.State()))
		resource.Set(stateField, protoreflect.ValueOfEnum(protoreflect.EnumNumber(stateNum)))

		if prField := descs.Resource.Fields().ByName("pause_reason"); prField != nil {
			resource.Set(prField, protoreflect.ValueOfString(f.PauseReason()))
		}

		reconcilingField := descs.Resource.Fields().ByName("reconciling")
		resource.Set(reconcilingField, protoreflect.ValueOfBool(f.Reconciling()))

		if f.Provenance() != nil {
			provField := descs.Resource.Fields().ByName("provenance")
			if provVal, err := marshalProvenance(provField, f.Provenance()); err != nil {
				return nil, err
			} else {
				resource.Set(provField, provVal)
			}
		}

		genField := descs.Resource.Fields().ByName("generation")
		resource.Set(genField, protoreflect.ValueOfInt64(int64(f.Generation())))
	}

	// create_time
	if !er.CreatedAt().IsZero() {
		createTimeField := descs.Resource.Fields().ByName("create_time")
		if tsVal, err := dynamicapi.MarshalTimestamp(createTimeField, er.CreatedAt()); err != nil {
			return nil, err
		} else {
			resource.Set(createTimeField, tsVal)
		}
	}

	// update_time
	if !er.UpdatedAt().IsZero() {
		updateTimeField := descs.Resource.Fields().ByName("update_time")
		if tsVal, err := dynamicapi.MarshalTimestamp(updateTimeField, er.UpdatedAt()); err != nil {
			return nil, err
		} else {
			resource.Set(updateTimeField, tsVal)
		}
	}

	// etag (weak domain-state token)
	etagField := descs.Resource.Fields().ByName("etag")
	resource.Set(etagField, protoreflect.ValueOfString(string(v.Etag())))

	return resource, nil
}

// ViewToStruct projects a view into a google.protobuf.Struct with the
// same field set as the dynamic managed-resource Get/List body
// (protojson casing). It does not include labels or inventory fields.
func ViewToStruct(descs *ServiceDescriptors, v domain.ExtensionResourceView) (*structpb.Struct, error) {
	msg, err := ViewToResource(descs, v)
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
