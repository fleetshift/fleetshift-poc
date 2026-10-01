package protocol

import "context"

// ProducerAPI is the producer side of a provenance profile.
// Common producer code identifies the allowed provenance type and principal
// authority; it does not obtain an RM-maintained profile or anchor ID.
//
// Implementations own their signing ceremony and expose purpose-specific
// operations over known content. They must not return private-key bytes or a
// general signing oracle.
type ProducerAPI interface {
	ProvenanceType() ProvenanceType
	CreateEvidence(ctx context.Context, assertion TypedAssertion) (TypedEvidence, error)
}

// ResourceManagerAPI is the resource-manager side of a provenance profile.
// Common RM code owns immutable TypedEvidence storage and evidence-log
// registration. The profile invokes work inside the mutation's durable
// transaction and assembles replaceable support material. RM verification is
// authoritative only for whether the RM accepts an API request; it is not
// target verification.
//
// DecodeAssertion is the evidence counterpart of DecodeDeliveryScope: it
// unwraps the inner statement from profile-owned evidence bytes without
// authenticating it. Common code then calls DecodeDeliveryScope on that
// statement. The RM never parses TypedEvidence.Bytes itself.
type ResourceManagerAPI interface {
	ProvenanceType() ProvenanceType
	// ParseHints locates tentative policy for registration without granting trust.
	ParseHints(evidence TypedEvidence) (TentativeHints, error)
	// RequiresEvidenceLog has the same mechanism requirement as TargetAPI.
	RequiresEvidenceLog() bool
	AssembleSupportMaterial(ctx context.Context, evidence TypedEvidence) (SupportMaterial, error)
	CheckDelivery(evidence TypedEvidence) (TentativeHints, error)
	DecodeAssertion(evidence TypedEvidence) (TypedAssertion, error)
}

// TargetAPI is the target side of a provenance profile.
// ParseHints reads untrusted type-specific material only to locate
// authenticated authority configuration and a candidate predicate type.
// RequiresEvidenceLog reports config: this suite needs FleetShift log
// positions (for example continuity key events). It is not a session
// method and does not describe the statement window.
// BeginVerification creates a single-use session for one SignedStatement.
// Owns declares the suite-owned predicates this profile applies.
// Apply updates retained profile state for those predicates. Intent and
// trust-config-update predicates are handled by the agent and are never
// owned by a suite. Unknown predicates fail closed.
type TargetAPI interface {
	ProvenanceType() ProvenanceType
	ParseHints(evidence TypedEvidence) (TentativeHints, error)
	RequiresEvidenceLog() bool
	BeginVerification(ctx context.Context, req VerifyRequest) (ProvenanceVerificationSession, error)
	Owns(predicate PredicateType) bool
	Apply(ctx context.Context, req ApplyRequest) error
}

// VerifyRequest is the authenticated policy and one couriered signed
// statement supplied to a target-side profile. Statement is a
// SignedStatement: common selection copies item.SignedStatement and must
// not copy item.EvidenceLog. Retained profile state stays with the
// TargetAPI implementation and is associated with the authenticated
// authority and profile configuration, never with an RM-supplied profile ID.
// DeliveryContext contains hint-derived source policy fields, not the tenant
// expected by a semantic consumer of the assertion.
// Common selection detaches statement and configuration buffers at this
// profile boundary so the implementation cannot change shared common values.
type VerifyRequest struct {
	Statement       SignedStatement
	ProfileConfig   ProfileConfig
	AuthorityConfig AuthorityConfig
	DeliveryContext DeliveryContext
}

// ApplyRequest is the authenticated result of SelectAndVerify plus verified
// temporal facts for this subject. Suites use it to update retained proof
// material for predicates they own. Intent predicates never reach Apply.
// The agent detaches the request's buffers at this profile boundary.
type ApplyRequest struct {
	Authenticated AuthenticatedEvidence
	Assertion     TypedAssertion
	Statement     SignedStatement
	// Temporal is the verified subject-temporal projection for this
	// statement. LogPosition is set only after occurrence verification.
	// Continuity/v3 cutoffs will need it; profiles that do not consult
	// temporal facts ignore it.
	Temporal VerifiedSubjectTemporalInfo
}

// TargetLookup returns the installed target implementation for a provenance
// type. Implementations arrive through the verifier's trusted software supply
// chain; unknown types fail closed.
type TargetLookup func(ProvenanceType) (TargetAPI, bool)

// ResourceManagerLookup returns installed courier and admission mechanisms
// from the resource manager's trusted software supply chain.
type ResourceManagerLookup func(ProvenanceType) (ResourceManagerAPI, bool)
