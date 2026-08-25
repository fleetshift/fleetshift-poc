package protocol

import "context"

// LogDomainID names an ordered-log namespace. Positions from different
// domains are not comparable.
type LogDomainID string

const (
	// LogDomainTenantEvidenceV1 is the single tenant evidence-log domain
	// used by this POC.
	LogDomainTenantEvidenceV1 LogDomainID = "tenant-evidence/v1"
)

// LogPosition is a verified index in a named log domain.
type LogPosition struct {
	Domain LogDomainID
	Index  uint64
}

// VerifiedEvidenceLogBinding is the intermediate result at the log-verifier
// boundary. Evidence is the recomputed TypedEvidence identity bound at
// Position. Common code checks Evidence against the verifier input before
// retaining only Position.
type VerifiedEvidenceLogBinding struct {
	Position LogPosition
	Evidence Digest
}

// OrderedLogEvidenceVerifier verifies that exact TypedEvidence identities
// occur in an append-only log. Implementations are bound to a verified
// checkpoint and memoize occurrence proofs; they never scan a package-wide
// digest map. Inclusion is supplied by value as the adjacent proof for this
// evidence, not looked up by digest.
type OrderedLogEvidenceVerifier interface {
	VerifyOccurrence(
		ctx context.Context,
		evidence TypedEvidence,
		inclusion EvidenceLogInclusion,
	) (VerifiedEvidenceLogBinding, error)
}
