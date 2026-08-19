package protocol

// ProvenanceType is a well-known, versioned identifier for verifier and
// protocol semantics. Authenticated configuration selects installed
// implementations; it never supplies executable code.
type ProvenanceType string

const (
	// ProvenanceTypeDirectKeyV1 is the naive retained-public-key profile
	// used by this POC. It is a well-known type, not an RM-assigned ID.
	ProvenanceTypeDirectKeyV1 ProvenanceType = "direct-key/v1"
)

// MediaType is the wire representation of TypedEvidence bytes.
type MediaType string

// ContentType names the inner assertion's purpose. It is established only
// after a selected profile authenticates the evidence bytes.
type ContentType string

const (
	// ContentTypeDeliveryAuthorizationV1 is a user's delivery authorization.
	ContentTypeDeliveryAuthorizationV1 ContentType = "delivery-authorization/v1"

	// ContentTypeTrustConfigUpdateV1 is an authenticated trust-configuration
	// successor, as defined by the provenance design.
	ContentTypeTrustConfigUpdateV1 ContentType = "trust-config-update/v1"
)

// TypedEvidence is the common stored object: a provenance type, a media
// type, and the exact immutable bytes. Common storage, digesting, bounding,
// and routing operate on this envelope and do not parse the inner format.
type TypedEvidence struct {
	ProvenanceType ProvenanceType `json:"provenance_type"`
	MediaType      MediaType      `json:"media_type"`
	Bytes          []byte         `json:"bytes"`
}

// Identity returns the domain-separated binding of provenance type, media
// type, and exact bytes. Changing any field produces a different item.
func (e TypedEvidence) Identity() (Digest, error) {
	return DigestObject(purposeTypedEvidenceIdentity, e)
}

// TypedAssertion is the inner purpose-typed content a profile authenticates.
type TypedAssertion struct {
	ContentType ContentType `json:"content_type"`
	Bytes       []byte      `json:"bytes"`
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
	ContentType            ContentType         `json:"content_type"`
	ContentDigest          Digest              `json:"content_digest"`
	ProvenanceType         ProvenanceType      `json:"provenance_type"`
	AuthorityConfigDigest  Digest              `json:"authority_config_digest"`
	ProfileConfigDigest    Digest              `json:"profile_config_digest"`
	SatisfiedConstraints   []ConstraintOutcome `json:"satisfied_constraints,omitempty"`
}

// TentativeHints are parsed from untrusted evidence to locate authenticated
// authority configuration. They grant no identity or authority.
type TentativeHints struct {
	Scheme          IdentityScheme  `json:"scheme"`
	Authority       Authority       `json:"authority"`
	TenantPartition TenantPartition `json:"tenant_partition,omitempty"`
	Subject         Subject         `json:"subject,omitempty"`
}

// SupportMaterial is replaceable, reconstructable profile proof material
// assembled by the resource manager. It is not part of the immutable
// TypedEvidence identity and gains authority only by verifying against
// retained state and committed evidence.
type SupportMaterial struct {
	ProvenanceType ProvenanceType `json:"provenance_type"`
	MediaType      MediaType      `json:"media_type"`
	Bytes          []byte         `json:"bytes,omitempty"`
}

// DeliveryContext is the bounded context used to match a delivery policy
// before evidence is authoritative.
type DeliveryContext struct {
	ClaimedTenant     TenantID    `json:"claimed_tenant,omitempty"`
	ContentType       ContentType `json:"content_type"`
	RootAuthorization bool        `json:"root_authorization"`
}

// DeliveryAuthorization is the exact inner assertion for a user's delivery
// authorization in this POC. The profile authenticates its canonical bytes;
// common delivery handling consumes the resulting AuthenticatedEvidence.
type DeliveryAuthorization struct {
	TenantID      TenantID `json:"tenant_id"`
	TargetID      string   `json:"target_id"`
	FulfillmentID string   `json:"fulfillment_id"`
	Generation    uint64   `json:"generation"`
	Action        string   `json:"action"`
	Payload       []byte   `json:"payload"`
}

const (
	ActionPut    = "put"
	ActionRemove = "remove"
)

// Assertion returns the purpose-typed inner content of a delivery
// authorization.
func (a DeliveryAuthorization) Assertion() (TypedAssertion, error) {
	encoded, err := MarshalCanonical(a)
	if err != nil {
		return TypedAssertion{}, err
	}
	return TypedAssertion{
		ContentType: ContentTypeDeliveryAuthorizationV1,
		Bytes:       encoded,
	}, nil
}
