package domain

import (
	"encoding/json"
	"fmt"
)

// FulfillmentRelation describes how a managed resource maps to
// fulfillment strategies. It is the behavioral/structural concept —
// purely about derivation logic. The addon's cryptographic proof of
// the relation is stored separately on [ManagedResourceTypeDef].
//
// Mirrors the hybrid attestation PoC's FulfillmentRelation type union.
type FulfillmentRelation interface {
	DeriveStrategies(intent ResourceIntent) (ManifestStrategySpec, PlacementStrategySpec, *RolloutStrategySpec)
	fulfillmentRelation() // sealed
}

// RegisteredSelfTarget is a fulfillment relation where the addon is
// the delivery target itself. The addon signs this to claim: "I own
// resources of this type, and fulfillments derived from them target me
// directly."
//
// Produces: managed-resource manifests (resolved by intent reference),
// static placement to the addon target, immediate rollout.
type RegisteredSelfTarget struct {
	addonTarget  TargetID
	manifestType ManifestType
}

// NewRegisteredSelfTarget constructs a [RegisteredSelfTarget]. Both
// parameters are already-valid value objects; the type system guarantees
// non-emptiness via their respective constructors.
func NewRegisteredSelfTarget(addonTarget TargetID, manifestType ManifestType) RegisteredSelfTarget {
	return RegisteredSelfTarget{addonTarget: addonTarget, manifestType: manifestType}
}

// AddonTarget returns the target that owns resources of this type.
func (r RegisteredSelfTarget) AddonTarget() TargetID { return r.addonTarget }

// ManifestType returns the manifest type produced for fulfillments.
func (r RegisteredSelfTarget) ManifestType() ManifestType { return r.manifestType }

func (r RegisteredSelfTarget) DeriveStrategies(intent ResourceIntent) (ManifestStrategySpec, PlacementStrategySpec, *RolloutStrategySpec) {
	ms := ManifestStrategySpec{
		Type: ManifestStrategyManagedResource,
		IntentRef: IntentRef{
			ResourceType: intent.ResourceType,
			Name:         intent.Name,
			Version:      intent.Version,
			ManifestType: r.manifestType,
		},
	}
	ps := PlacementStrategySpec{
		Type:    PlacementStrategyStatic,
		Targets: []TargetID{r.addonTarget},
	}
	rs := &RolloutStrategySpec{
		Type: RolloutStrategyImmediate,
	}
	return ms, ps, rs
}

func (RegisteredSelfTarget) fulfillmentRelation() {}

// MarshalJSON implements json.Marshaler for RegisteredSelfTarget.
func (r RegisteredSelfTarget) MarshalJSON() ([]byte, error) {
	return json.Marshal(registeredSelfTargetJSON{
		AddonTarget:  string(r.addonTarget),
		ManifestType: string(r.manifestType),
	})
}

// UnmarshalJSON implements json.Unmarshaler for RegisteredSelfTarget.
// This is a deserialization boundary — it validates raw strings through
// the value object constructors before accepting them.
func (r *RegisteredSelfTarget) UnmarshalJSON(data []byte) error {
	var dto registeredSelfTargetJSON
	if err := json.Unmarshal(data, &dto); err != nil {
		return err
	}
	target, err := NewTargetID(dto.AddonTarget)
	if err != nil {
		return fmt.Errorf("registered self target: %w", err)
	}
	mt, err := NewManifestType(dto.ManifestType)
	if err != nil {
		return fmt.Errorf("registered self target: %w", err)
	}
	*r = NewRegisteredSelfTarget(target, mt)
	return nil
}

// registeredSelfTargetJSON is the serialization DTO for
// [RegisteredSelfTarget]. Fields are raw strings — validation happens
// during unmarshal via [NewTargetID] and [NewManifestType].
type registeredSelfTargetJSON struct {
	AddonTarget  string `json:"addon_target"`
	ManifestType string `json:"manifest_type"`
}

// SignedRelation is self-contained evidence for delivery-side
// verification. It bundles the resource type scope, the fulfillment
// relation, and the addon's signature over the claim. Assembled from
// a [ManagedResourceTypeDef] at delivery time.
type SignedRelation struct {
	ResourceType ResourceType        `json:"resource_type"`
	Relation     FulfillmentRelation `json:"relation"`
	Signature    Signature           `json:"signature"`
}

// MarshalJSON implements json.Marshaler for SignedRelation.
func (sr SignedRelation) MarshalJSON() ([]byte, error) {
	type alias struct {
		ResourceType ResourceType       `json:"resource_type"`
		Relation     fulfillmentRelJSON `json:"relation"`
		Signature    Signature          `json:"signature"`
	}
	rel, err := marshalFulfillmentRelation(sr.Relation)
	if err != nil {
		return nil, err
	}
	return json.Marshal(alias{
		ResourceType: sr.ResourceType,
		Relation:     rel,
		Signature:    sr.Signature,
	})
}

// UnmarshalJSON implements json.Unmarshaler for SignedRelation.
func (sr *SignedRelation) UnmarshalJSON(data []byte) error {
	type alias struct {
		ResourceType ResourceType       `json:"resource_type"`
		Relation     fulfillmentRelJSON `json:"relation"`
		Signature    Signature          `json:"signature"`
	}
	var a alias
	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}
	sr.ResourceType = a.ResourceType
	sr.Signature = a.Signature
	rel, err := unmarshalFulfillmentRelation(a.Relation)
	if err != nil {
		return err
	}
	sr.Relation = rel
	return nil
}

// fulfillmentRelJSON is the discriminated union representation for
// FulfillmentRelation serialization.
type fulfillmentRelJSON struct {
	Type                 string                    `json:"Type"`
	RegisteredSelfTarget *registeredSelfTargetJSON `json:"RegisteredSelfTarget,omitempty"`
}

func marshalFulfillmentRelation(r FulfillmentRelation) (fulfillmentRelJSON, error) {
	switch v := r.(type) {
	case RegisteredSelfTarget:
		return fulfillmentRelJSON{
			Type: "RegisteredSelfTarget",
			RegisteredSelfTarget: &registeredSelfTargetJSON{
				AddonTarget:  string(v.addonTarget),
				ManifestType: string(v.manifestType),
			},
		}, nil
	case *RegisteredSelfTarget:
		if v == nil {
			return fulfillmentRelJSON{}, fmt.Errorf("fulfillment relation: RegisteredSelfTarget is nil")
		}
		return fulfillmentRelJSON{
			Type: "RegisteredSelfTarget",
			RegisteredSelfTarget: &registeredSelfTargetJSON{
				AddonTarget:  string(v.addonTarget),
				ManifestType: string(v.manifestType),
			},
		}, nil
	case nil:
		return fulfillmentRelJSON{}, nil
	default:
		return fulfillmentRelJSON{}, fmt.Errorf("fulfillment relation: unknown type %T", r)
	}
}

func unmarshalFulfillmentRelation(j fulfillmentRelJSON) (FulfillmentRelation, error) {
	switch j.Type {
	case "RegisteredSelfTarget":
		if j.RegisteredSelfTarget == nil {
			return nil, fmt.Errorf("fulfillment relation: RegisteredSelfTarget is nil")
		}
		target, err := NewTargetID(j.RegisteredSelfTarget.AddonTarget)
		if err != nil {
			return nil, fmt.Errorf("fulfillment relation: %w", err)
		}
		mt, err := NewManifestType(j.RegisteredSelfTarget.ManifestType)
		if err != nil {
			return nil, fmt.Errorf("fulfillment relation: %w", err)
		}
		return NewRegisteredSelfTarget(target, mt), nil
	case "":
		return nil, nil
	default:
		return nil, fmt.Errorf("fulfillment relation: unknown type %q", j.Type)
	}
}

// MarshalFulfillmentRelation serializes a [FulfillmentRelation] to JSON.
// Used by repository implementations for persistence.
func MarshalFulfillmentRelation(r FulfillmentRelation) ([]byte, error) {
	j, err := marshalFulfillmentRelation(r)
	if err != nil {
		return nil, err
	}
	return json.Marshal(j)
}

// UnmarshalFulfillmentRelation deserializes a [FulfillmentRelation] from JSON.
// Used by repository implementations for hydration.
func UnmarshalFulfillmentRelation(data []byte) (FulfillmentRelation, error) {
	var j fulfillmentRelJSON
	if err := json.Unmarshal(data, &j); err != nil {
		return nil, err
	}
	return unmarshalFulfillmentRelation(j)
}
