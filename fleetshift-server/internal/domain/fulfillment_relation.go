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
	AddonTarget TargetID `json:"addon_target"`
}

func (r RegisteredSelfTarget) DeriveStrategies(intent ResourceIntent) (ManifestStrategySpec, PlacementStrategySpec, *RolloutStrategySpec) {
	ms := ManifestStrategySpec{
		Type: ManifestStrategyManagedResource,
		IntentRef: IntentRef{
			ResourceType: intent.ResourceType,
			Name:         intent.Name,
			Version:      intent.Version,
		},
	}
	ps := PlacementStrategySpec{
		Type:    PlacementStrategyStatic,
		Targets: []TargetID{r.AddonTarget},
	}
	rs := &RolloutStrategySpec{
		Type: RolloutStrategyImmediate,
	}
	return ms, ps, rs
}

func (RegisteredSelfTarget) fulfillmentRelation() {}

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
		ResourceType ResourceType         `json:"resource_type"`
		Relation     fulfillmentRelJSON   `json:"relation"`
		Signature    Signature            `json:"signature"`
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
	Type                 string               `json:"Type"`
	RegisteredSelfTarget *RegisteredSelfTarget `json:"RegisteredSelfTarget,omitempty"`
}

func marshalFulfillmentRelation(r FulfillmentRelation) (fulfillmentRelJSON, error) {
	switch v := r.(type) {
	case RegisteredSelfTarget:
		return fulfillmentRelJSON{Type: "RegisteredSelfTarget", RegisteredSelfTarget: &v}, nil
	case *RegisteredSelfTarget:
		return fulfillmentRelJSON{Type: "RegisteredSelfTarget", RegisteredSelfTarget: v}, nil
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
		return *j.RegisteredSelfTarget, nil
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
