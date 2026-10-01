package protocol

import (
	"context"
	"fmt"
	"time"
)

// LogDomainID names an ordered-log namespace. Positions from different
// domains are not comparable.
type LogDomainID string

const (
	// LogDomainTenantEvidenceV1 is the single tenant evidence-log domain
	// used by this POC.
	LogDomainTenantEvidenceV1 LogDomainID = "tenant-evidence/v1"
)

// TimestampAuthorityID names a trusted-time source. The identifier is
// retained for source policy and audit; coordinate comparison across sources
// requires policy approval and comparable UTC uncertainty intervals.
type TimestampAuthorityID string

// TimestampFormat is a versioned discriminant for a timestamp token
// encoding. Unknown values fail closed. It is not a parser.
type TimestampFormat string

const (
	// TimestampFormatRFC3161V1 identifies RFC 3161 tokens. This POC does
	// not parse or verify that encoding.
	TimestampFormatRFC3161V1 TimestampFormat = "rfc3161/v1"
)

const (
	// MaxTimestampTokenBytes is the POC cap on an unverified timestamp
	// token. Identity() rejects larger tokens before producing a digest.
	MaxTimestampTokenBytes = 16 * 1024

	// MaxTimestampMessageBytes is the POC cap on timestamped message
	// bytes. Identity() rejects larger messages before producing a digest.
	MaxTimestampMessageBytes = 64 * 1024
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

// VerifiedTimeObservation is the intermediate timestamp result passed to
// provenance Finish so the profile can authenticate the relationship
// between the timestamped message and the subject. Binding is the
// protocol-owned identity of the prepared (Format, Token, Message) triple.
type VerifiedTimeObservation struct {
	Binding   Digest
	Authority TimestampAuthorityID
	Earliest  time.Time
	Latest    time.Time
}

// VerifiedSubjectTime is a final subject time fact after the active trusted
// time source policy has admitted the observation and Finish has authenticated
// its applicability to this subject. It does not carry a binding digest.
type VerifiedSubjectTime struct {
	Authority TimestampAuthorityID
	Earliest  time.Time
	Latest    time.Time
}

// VerifiedSubjectTemporalInfo is the temporal facts common code retains for
// an authenticated subject after occurrence verification and successful
// Finish. LogPosition is set only after occurrence verification. Times are
// source-approved, Finish-endorsed timestamp observations with binding
// digests dropped. The initial POC does not implement trusted-time source
// policy or a timestamp verifier.
// Common consumers treat the position and time slice as immutable.
type VerifiedSubjectTemporalInfo struct {
	LogPosition *LogPosition
	Times       []VerifiedSubjectTime
}

// OrderedLogEvidenceVerifier verifies that exact TypedEvidence identities
// occur in an append-only log. Implementations are bound to a verified
// checkpoint and memoize occurrence proofs; they never scan a package-wide
// digest map. Inclusion is supplied by value as the adjacent proof for this
// evidence, not looked up by digest.
// Inputs are borrowed immutable values and may be retained by the verifier.
type OrderedLogEvidenceVerifier interface {
	VerifyOccurrence(
		ctx context.Context,
		evidence TypedEvidence,
		inclusion EvidenceLogInclusion,
	) (VerifiedEvidenceLogBinding, error)
}

// TimestampReceipt is a producer-side timestamp token. The provenance
// producer chooses the message bytes, invokes TimestampAuthorityClient,
// and embeds the receipt according to its own evidence format. This type
// is intent-only in this POC: there is no RFC 3161 client.
type TimestampReceipt struct {
	Format TimestampFormat
	Token  []byte
}

// TimestampAuthorityClient is the producer-side TSA capability. It does
// not create TypedEvidence, choose a provenance type, or prescribe a
// package-wide TSA field. This POC does not implement or wire it.
type TimestampAuthorityClient interface {
	Timestamp(ctx context.Context, message []byte) (TimestampReceipt, error)
}

// UnverifiedTimestampBinding is a provenance-identified timestamp token
// and the exact message bytes it purports to bind. Token and Message are
// mutable slices; callers that retain a binding must Clone it.
type UnverifiedTimestampBinding struct {
	Format  TimestampFormat
	Token   []byte
	Message []byte
}

// Clone returns a copy whose token and message slices do not alias the
// original.
func (b UnverifiedTimestampBinding) Clone() UnverifiedTimestampBinding {
	return UnverifiedTimestampBinding{
		Format:  b.Format,
		Token:   cloneBytes(b.Token),
		Message: cloneBytes(b.Message),
	}
}

// Identity returns the protocol-owned correlation digest of this exact
// (Format, Token, Message) triple. It hashes a versioned domain tag plus
// a length-delimited encoding of the canonical format discriminant and
// exact token and message bytes. It does not parse the token or hash
// JSON DigestObject encoding. Unknown or empty formats and size-bound
// violations return ErrInvalidTimestampBinding before producing a digest.
func (b UnverifiedTimestampBinding) Identity() (Digest, error) {
	if err := b.validate(); err != nil {
		return "", err
	}
	return digestLengthDelimited(purposeTimestampBindingIdentity, []byte(b.Format), b.Token, b.Message), nil
}

func (b UnverifiedTimestampBinding) validate() error {
	switch b.Format {
	case TimestampFormatRFC3161V1:
	default:
		return fmt.Errorf("%w: timestamp format %q", ErrInvalidTimestampBinding, b.Format)
	}
	if len(b.Token) > MaxTimestampTokenBytes {
		return fmt.Errorf("%w: token length %d exceeds %d", ErrInvalidTimestampBinding, len(b.Token), MaxTimestampTokenBytes)
	}
	if len(b.Message) > MaxTimestampMessageBytes {
		return fmt.Errorf("%w: message length %d exceeds %d", ErrInvalidTimestampBinding, len(b.Message), MaxTimestampMessageBytes)
	}
	return nil
}

// VerifiedTimestampResult is the trusted-time adapter's verified
// authority and observation interval. It has no Binding field: common
// code attaches the precomputed binding identity.
type VerifiedTimestampResult struct {
	Authority TimestampAuthorityID
	Earliest  time.Time
	Latest    time.Time
}

// TrustedTimeEvidenceVerifier verifies an unverified timestamp binding.
// This POC does not ship a production implementation.
type TrustedTimeEvidenceVerifier interface {
	VerifyTimestamp(
		ctx context.Context,
		binding UnverifiedTimestampBinding,
	) (VerifiedTimestampResult, error)
}

// TemporalVerificationServices bundles the log and time adapters consumed
// by common selection. A nil Log fails when inclusion is required or
// supplied. A nil Time fails if Prepare returns a timestamp binding.
type TemporalVerificationServices struct {
	Log  OrderedLogEvidenceVerifier
	Time TrustedTimeEvidenceVerifier
}

// TemporalPreparation is the untrusted timestamp bindings identified by
// Prepare. It does not carry validity boundaries or an authentication
// result.
// Common code copies profile-owned buffers at the Prepare boundary, then
// shares those bindings as immutable values during coordination.
type TemporalPreparation struct {
	Timestamps []UnverifiedTimestampBinding
}

// VerifiedProvenanceTemporalInputs is the verified forms of
// provenance-specific inputs returned by Prepare. It deliberately
// excludes FleetShift evidence-log occurrences.
type VerifiedProvenanceTemporalInputs struct {
	Timestamps []VerifiedTimeObservation
}

// EvidenceLogRequirement is the union of tenant policy and the selected
// profile's RequiresEvidenceLog. Non-empty Domain means the coordinator
// must verify inclusion for reached evidence. It is not a window
// coordinate.
type EvidenceLogRequirement struct {
	Domain LogDomainID
}

// ProvenanceAuthenticationResult is the authenticated content and every
// temporal constraint recovered by Finish.
type ProvenanceAuthenticationResult struct {
	Authenticated AuthenticatedEvidence
	Assertion     TypedAssertion
	Established   []AuthenticatedTemporalConstraint
	Retired       []AuthenticatedTemporalConstraint
}

// VerificationResult is the common selection result: authenticated
// content, the normalized validity window, digest-free subject temporal
// facts, and the authenticated authority/profile/policy that produced
// them.
// The result and all nested data are immutable within common code. Authority,
// Profile, and Policy share the selected trust configuration's backing data;
// consumers must not mutate their slices or pointers.
type VerificationResult struct {
	ProvenanceAuthenticationResult
	Validity  AuthenticatedValidity
	Temporal  VerifiedSubjectTemporalInfo
	Authority AuthorityConfig
	Profile   ProfileConfig
	Policy    DeliveryPolicy
}

// ProvenanceVerificationSession is a single-use profile session for one
// SignedStatement. Prepare is called exactly once. Finish is called at
// most once after successful Prepare. A session is never reused after
// success or failure.
type ProvenanceVerificationSession interface {
	Prepare(ctx context.Context) (TemporalPreparation, error)
	Finish(
		ctx context.Context,
		inputs VerifiedProvenanceTemporalInputs,
	) (ProvenanceAuthenticationResult, error)
}
