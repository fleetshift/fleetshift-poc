package protocol

import "errors"

var (
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

	// ErrAmbiguousPolicy is returned when delivery context matches more
	// than one delivery policy.
	ErrAmbiguousPolicy = errors.New("ambiguous delivery policy")

	// ErrNoMatchingPolicy is returned when no delivery policy matches.
	ErrNoMatchingPolicy = errors.New("no matching delivery policy")

	// ErrNoSuccessfulProfile is returned when no profile in the matched
	// policy's ordered any-of list fully verifies the evidence.
	ErrNoSuccessfulProfile = errors.New("no successful provenance profile")

	// ErrPolicyReevaluation is returned when authenticated identity or
	// content does not match the policy or hints used to select it.
	ErrPolicyReevaluation = errors.New("authenticated result failed policy re-evaluation")

	// ErrTenantMismatch is returned when a claimed tenant does not match
	// the verified tenant mapping.
	ErrTenantMismatch = errors.New("tenant mapping mismatch")

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
	// prove append-only consistency or inclusion of the claimed leaf.
	ErrInvalidLogUpdate = errors.New("invalid evidence-log update")
)
