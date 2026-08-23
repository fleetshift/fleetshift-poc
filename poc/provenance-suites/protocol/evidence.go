package protocol

import (
	"encoding/json"
	"fmt"
)

// ProvenanceType is a well-known, versioned identifier for verifier and
// protocol semantics. Authenticated configuration selects installed
// implementations; it never supplies executable code.
type ProvenanceType string

const (
	// ProvenanceTypeDirectKeyV1 is the naive retained-public-key profile
	// used by this POC. It is a well-known type, not an RM-assigned ID.
	ProvenanceTypeDirectKeyV1 ProvenanceType = "direct-key/v1"
)

// MediaType is how a blob of bytes is encoded. Encoded carries it together
// with those bytes. It is used for proof encodings, replaceable support
// material, and delivered payload items.
type MediaType string

// Encoded is media-typed bytes. TypedEvidence, SupportMaterial, and
// TypedManifest share this form and remain distinct types because their
// authority and lifecycle differ. TypedAssertion is not Encoded: a
// predicate type is purpose, not a media type.
type Encoded struct {
	MediaType MediaType `json:"media_type"`
	Bytes     []byte    `json:"bytes"`
}

// Clone returns a copy whose byte slice does not alias the original.
func (e Encoded) Clone() Encoded {
	return Encoded{
		MediaType: e.MediaType,
		Bytes:     append([]byte(nil), e.Bytes...),
	}
}

// PredicateType names the inner assertion's purpose. It is established only
// after a selected profile authenticates the evidence bytes.
type PredicateType string

const (
	// PredicateTypeDeploymentV1 is a user's deployment authorization: the
	// signed content is the fulfillment recipe as typed manifests.
	PredicateTypeDeploymentV1 PredicateType = "deployment/v1"

	// PredicateTypeManagedResourceV1 is a user's managed-resource
	// authorization: the signed content is a resource spec plus who owns
	// it. Applying it requires a verified fulfillment-relation/v1.
	PredicateTypeManagedResourceV1 PredicateType = "managed-resource/v1"

	// PredicateTypeFulfillmentRelationV1 is supporting graph evidence that
	// maps a managed-resource identity onto a payload MediaType. It is
	// not a user-intent root predicate.
	PredicateTypeFulfillmentRelationV1 PredicateType = "fulfillment-relation/v1"

	// PredicateTypeTrustConfigUpdateV1 is an authenticated trust-configuration
	// successor, as defined by the provenance design.
	PredicateTypeTrustConfigUpdateV1 PredicateType = "trust-config-update/v1"
)

// TypedEvidence is the common stored object: a provenance type plus Encoded
// proof bytes. Common storage, digesting, bounding, and routing operate on
// this envelope and do not parse the inner format.
type TypedEvidence struct {
	ProvenanceType ProvenanceType `json:"provenance_type"`
	Encoded
}

// Identity returns the domain-separated binding of provenance type, media
// type, and exact bytes. Changing any field produces a different item.
func (e TypedEvidence) Identity() (Digest, error) {
	return DigestObject(purposeTypedEvidenceIdentity, e)
}

// TypedAssertion is the inner purpose-typed statement a profile authenticates.
// Envelope encodings such as a Sigstore Bundle carry this statement inside
// TypedEvidence bytes. Verify emits it; common code does not read it from a
// parallel couriered field.
type TypedAssertion struct {
	PredicateType PredicateType `json:"predicate_type"`
	Bytes         []byte        `json:"bytes"`
}

// Digest returns the inner content digest consumed by AuthenticatedEvidence.
// It is not the outer TypedEvidence identity.
func (a TypedAssertion) Digest() (Digest, error) {
	return DigestObject(purposeContentDigest, a)
}

// ConstraintOutcome is a named result defined by the matched authenticated
// policy. It is not an unbounded facts map.
type ConstraintOutcome struct {
	Name   string `json:"name"`
	Result string `json:"result"`
}

// AuthenticatedEvidence is the deliberately small common result of profile
// verification. Configuration digests bind the result to the exact
// authenticated policy and anchors used; they are not profile selectors.
type AuthenticatedEvidence struct {
	Principal              Principal           `json:"principal"`
	MappedFleetShiftTenant TenantID            `json:"mapped_fleetshift_tenant,omitempty"`
	PredicateType          PredicateType       `json:"predicate_type"`
	ContentDigest          Digest              `json:"content_digest"`
	ProvenanceType         ProvenanceType      `json:"provenance_type"`
	AuthorityConfigDigest  Digest              `json:"authority_config_digest"`
	ProfileConfigDigest    Digest              `json:"profile_config_digest"`
	SatisfiedConstraints   []ConstraintOutcome `json:"satisfied_constraints,omitempty"`
}

// TentativeHints are parsed from untrusted evidence to locate authenticated
// authority configuration and a candidate delivery policy. They grant no
// identity or authority. PredicateType is an untrusted hint of the inner
// statement purpose; it is re-checked after verification.
type TentativeHints struct {
	Scheme          IdentityScheme  `json:"scheme"`
	Authority       Authority       `json:"authority"`
	TenantPartition TenantPartition `json:"tenant_partition,omitempty"`
	Subject         Subject         `json:"subject,omitempty"`
	PredicateType   PredicateType   `json:"predicate_type,omitempty"`
}

// SupportMaterial is replaceable, reconstructable profile proof material
// assembled by the resource manager. It uses the Encoded form; the profile
// is implied by the evidence it accompanies. It is not part of the immutable
// TypedEvidence identity and gains authority only by verifying against
// retained state and committed evidence.
type SupportMaterial Encoded

// SignedStatement is one independently authenticated assertion as couriered
// to a verifier. Evidence is the immutable proof encoding; the inner
// statement lives in those bytes. Support is replaceable material used by
// the selected profile to verify that evidence. It is not a second signed
// assertion. Root and supporting items in a delivery package are the same
// type.
type SignedStatement struct {
	Evidence TypedEvidence   `json:"evidence"`
	Support  SupportMaterial `json:"support"`
}

// DeliveryContext is the bounded context used to match a delivery policy
// before evidence is authoritative. PredicateType is filled from untrusted
// evidence hints during selection, not supplied as a separate couriered
// assertion.
type DeliveryContext struct {
	ClaimedTenant     TenantID      `json:"claimed_tenant,omitempty"`
	PredicateType     PredicateType `json:"predicate_type,omitempty"`
	RootAuthorization bool          `json:"root_authorization"`
}

const (
	ActionPut    = "put"
	ActionRemove = "remove"
)

// DeliveryScope is signed delivery-protocol identity, not a predicate. It
// is embedded in each root authorization so the resource manager cannot
// retarget tenant, placement, resource, generation, or action.
//
// FullResourceName is the AIP-122 identity of the Deployment or
// ManagedResource. TargetID is a stand-in for a placement strategy: this
// POC uses one static target rather than a selector. A later placement
// model replaces TargetID without changing the resource name, generation,
// or action.
type DeliveryScope struct {
	TenantID         TenantID         `json:"tenant_id"`
	TargetID         string           `json:"target_id"`
	FullResourceName FullResourceName `json:"name"`
	Generation       uint64           `json:"generation"`
	Action           string           `json:"action"`
}

// TypedManifest is one delivered payload item. It is Encoded: a payload
// media type and the exact bytes of that encoding.
type TypedManifest Encoded

// DeploymentAuthorization is the exact inner assertion for a deployment/v1
// predicate. The signed content is the fulfillment recipe.
type DeploymentAuthorization struct {
	DeliveryScope
	Manifests []TypedManifest `json:"manifests"`
}

// Assertion returns the purpose-typed inner content of a deployment
// authorization.
func (a DeploymentAuthorization) Assertion() (TypedAssertion, error) {
	encoded, err := MarshalCanonical(a)
	if err != nil {
		return TypedAssertion{}, err
	}
	return TypedAssertion{
		PredicateType: PredicateTypeDeploymentV1,
		Bytes:         encoded,
	}, nil
}

// ManagedResourceAuthorization is the exact inner assertion for a
// managed-resource/v1 predicate. The signed content is a resource spec;
// a verified fulfillment relation says how to fulfill it.
type ManagedResourceAuthorization struct {
	DeliveryScope
	ResourceType string          `json:"resource_type"`
	Spec         json.RawMessage `json:"spec"`
}

// Assertion returns the purpose-typed inner content of a managed-resource
// authorization.
func (a ManagedResourceAuthorization) Assertion() (TypedAssertion, error) {
	encoded, err := MarshalCanonical(a)
	if err != nil {
		return TypedAssertion{}, err
	}
	return TypedAssertion{
		PredicateType: PredicateTypeManagedResourceV1,
		Bytes:         encoded,
	}, nil
}

// FulfillmentRelation is the exact inner assertion for a
// fulfillment-relation/v1 predicate. This POC implements only
// RegisteredSelfTarget: the addon claims this resource type and fulfills
// it as a payload of MediaType.
type FulfillmentRelation struct {
	ResourceType string    `json:"resource_type"`
	MediaType    MediaType `json:"media_type"`
}

// Assertion returns the purpose-typed inner content of a fulfillment
// relation.
func (a FulfillmentRelation) Assertion() (TypedAssertion, error) {
	encoded, err := MarshalCanonical(a)
	if err != nil {
		return TypedAssertion{}, err
	}
	return TypedAssertion{
		PredicateType: PredicateTypeFulfillmentRelationV1,
		Bytes:         encoded,
	}, nil
}

// DecodeDeliveryScope extracts signed delivery-protocol identity from a
// root authorization body. Extra JSON fields are ignored so both
// deployment and managed-resource bodies decode. The resource identity is
// FullResourceName, not an RM-assigned fulfillment ID. Evidence encodings
// are profile-owned; callers first obtain the statement with
// ResourceManagerAPI.DecodeAssertion.
func DecodeDeliveryScope(assertion TypedAssertion) (DeliveryScope, error) {
	var scope DeliveryScope
	if err := json.Unmarshal(assertion.Bytes, &scope); err != nil {
		return DeliveryScope{}, fmt.Errorf("%w: decode delivery scope: %v", ErrMalformedEvidence, err)
	}
	return scope, nil
}

// DecodeDeploymentAuthorization decodes a deployment/v1 assertion body.
func DecodeDeploymentAuthorization(assertion TypedAssertion) (DeploymentAuthorization, error) {
	if assertion.PredicateType != PredicateTypeDeploymentV1 {
		return DeploymentAuthorization{}, fmt.Errorf("%w: %s", ErrUnknownPredicateType, assertion.PredicateType)
	}
	var authorization DeploymentAuthorization
	if err := json.Unmarshal(assertion.Bytes, &authorization); err != nil {
		return DeploymentAuthorization{}, fmt.Errorf("%w: decode deployment authorization: %v", ErrMalformedEvidence, err)
	}
	return authorization, nil
}

// DecodeManagedResourceAuthorization decodes a managed-resource/v1 assertion body.
func DecodeManagedResourceAuthorization(assertion TypedAssertion) (ManagedResourceAuthorization, error) {
	if assertion.PredicateType != PredicateTypeManagedResourceV1 {
		return ManagedResourceAuthorization{}, fmt.Errorf("%w: %s", ErrUnknownPredicateType, assertion.PredicateType)
	}
	var authorization ManagedResourceAuthorization
	if err := json.Unmarshal(assertion.Bytes, &authorization); err != nil {
		return ManagedResourceAuthorization{}, fmt.Errorf("%w: decode managed-resource authorization: %v", ErrMalformedEvidence, err)
	}
	return authorization, nil
}

// DecodeFulfillmentRelation decodes a fulfillment-relation/v1 assertion body.
func DecodeFulfillmentRelation(assertion TypedAssertion) (FulfillmentRelation, error) {
	if assertion.PredicateType != PredicateTypeFulfillmentRelationV1 {
		return FulfillmentRelation{}, fmt.Errorf("%w: %s", ErrUnknownPredicateType, assertion.PredicateType)
	}
	var relation FulfillmentRelation
	if err := json.Unmarshal(assertion.Bytes, &relation); err != nil {
		return FulfillmentRelation{}, fmt.Errorf("%w: decode fulfillment relation: %v", ErrMalformedEvidence, err)
	}
	return relation, nil
}
