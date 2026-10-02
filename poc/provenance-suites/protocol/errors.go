package protocol

import "errors"

var (
	// ErrInvalidTrustConfiguration reports malformed or unresolved authenticated
	// configuration. It is not an ordinary failed profile attempt.
	ErrInvalidTrustConfiguration = errors.New("invalid trust configuration")

	// ErrUnknownProvenanceType is returned when no installed implementation
	// matches the evidence's provenance type.
	ErrUnknownProvenanceType = errors.New("unknown provenance type")

	// ErrUnknownMediaType is returned when a selected profile does not
	// permit the evidence's media type.
	ErrUnknownMediaType = errors.New("unknown media type")

	// ErrUnknownPredicateType is returned when an assertion purpose is
	// not a known predicate, or is not the predicate a decoder requires.
	ErrUnknownPredicateType = errors.New("unknown predicate type")

	// ErrUnknownAuthority is returned when tentative (scheme, authority)
	// does not match an authenticated AuthorityConfig.
	ErrUnknownAuthority = errors.New("unknown authority")

	// ErrNoMatchingPolicy is returned when no delivery policy matches.
	ErrNoMatchingPolicy = errors.New("no matching delivery policy")

	// ErrNoSuccessfulProfile is returned when no profile in the matched
	// policy's ordered any-of list fully verifies the evidence.
	ErrNoSuccessfulProfile = errors.New("no successful provenance profile")

	// ErrPolicyReevaluation is returned when authenticated identity or
	// content does not match the policy or hints used to select it.
	ErrPolicyReevaluation = errors.New("authenticated result failed policy re-evaluation")

	// ErrTenantMismatch reports an authenticated principal whose tenant identity
	// does not satisfy the relationship checked by common semantic evaluation.
	ErrTenantMismatch = errors.New("tenant identity mismatch")

	// ErrUninitializedVerifier is returned when an operation requires
	// bootstrapped trust configuration.
	ErrUninitializedVerifier = errors.New("verifier is uninitialized")

	// ErrAlreadyInitialized is returned when bootstrap is attempted on an
	// initialized verifier.
	ErrAlreadyInitialized = errors.New("verifier is already initialized")

	// ErrMalformedEvidence is returned when type-specific material cannot
	// be parsed.
	ErrMalformedEvidence = errors.New("malformed evidence")

	// ErrVerificationFailed is returned when cryptographic verification of
	// evidence fails.
	ErrVerificationFailed = errors.New("provenance verification failed")

	// ErrInvalidLogUpdate is returned when an evidence-log update does not
	// prove append-only consistency from a retained checkpoint.
	ErrInvalidLogUpdate = errors.New("invalid evidence-log update")

	// ErrInvalidLogInclusion is returned when an evidence-log inclusion
	// proof does not place the adjacent TypedEvidence identity at the
	// stated index under the given checkpoint.
	ErrInvalidLogInclusion = errors.New("invalid evidence-log inclusion")

	// ErrInvalidTimestampBinding is returned when a timestamp binding's
	// format is empty or unknown, or when token or message bytes exceed
	// the POC size bounds. Identity() returns this before producing a
	// digest so the coordinator can skip the time adapter.
	ErrInvalidTimestampBinding = errors.New("invalid timestamp binding")

	// ErrTemporalValidity is returned when a normalized validity window
	// is inverted or unsatisfiable, when subject facts fall outside that
	// window, or when a required comparable fact or log domain is missing
	// or mismatched.
	ErrTemporalValidity = errors.New("temporal validity check failed")
)
