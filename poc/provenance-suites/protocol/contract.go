package protocol

import "context"

// ClientAPI is the client or publisher side of a provenance profile.
// Common client code identifies the allowed provenance type and principal
// authority; it does not obtain an RM-maintained profile or anchor ID.
//
// Implementations own their signing ceremony and expose purpose-specific
// operations over known content. They must not return private-key bytes or a
// general signing oracle.
type ClientAPI interface {
	ProvenanceType() ProvenanceType
	CreateEvidence(ctx context.Context, assertion TypedAssertion) (TypedEvidence, error)
}

// ResourceManagerAPI is the resource-manager side of a provenance profile.
// The RM stores original immutable TypedEvidence, invokes profile-specific
// work inside the mutation's durable transaction, and assembles replaceable
// support material. RM verification is authoritative only for whether the RM
// accepts an API request; it is not target verification.
//
// DecodeAssertion is the evidence counterpart of DecodeDeliveryScope: it
// unwraps the inner statement from profile-owned evidence bytes without
// authenticating it. Common code then calls DecodeDeliveryScope on that
// statement. The RM never parses TypedEvidence.Bytes itself.
type ResourceManagerAPI interface {
	ProvenanceType() ProvenanceType
	StoreEvidence(ctx context.Context, evidence TypedEvidence) (Digest, error)
	AssembleSupportMaterial(ctx context.Context, evidence TypedEvidence) (SupportMaterial, error)
	CheckDelivery(evidence TypedEvidence) (TentativeHints, error)
	DecodeAssertion(evidence TypedEvidence) (TypedAssertion, error)
}

// TargetAPI is the target side of a provenance profile.
// ParseHints reads untrusted type-specific material only to locate
// authenticated authority configuration and a candidate predicate type.
// Verify produces AuthenticatedEvidence and the authenticated inner
// assertion extracted from the statement's evidence.
type TargetAPI interface {
	ProvenanceType() ProvenanceType
	ParseHints(evidence TypedEvidence) (TentativeHints, error)
	Verify(ctx context.Context, req VerifyRequest) (AuthenticatedEvidence, TypedAssertion, error)
}

// VerifyRequest is the authenticated policy and one couriered signed
// statement supplied to a target-side profile. Retained profile state stays
// with the TargetAPI implementation and is associated with the authenticated
// authority and profile configuration, never with an RM-supplied profile ID.
type VerifyRequest struct {
	Statement       SignedStatement
	ProfileConfig   ProfileConfig
	AuthorityConfig AuthorityConfig
	DeliveryContext DeliveryContext
}

// TargetLookup returns the installed target implementation for a provenance
// type. Implementations arrive through the verifier's trusted software supply
// chain; unknown types fail closed.
type TargetLookup func(ProvenanceType) (TargetAPI, bool)
