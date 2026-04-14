package domain

import "context"

// AuthnObserver is called at key points during authentication
// operations. Each method corresponds to one authentication lifecycle
// event, receives the caller's context, and returns a short-lived
// probe for that operation.
// Implementations should embed [NoOpAuthnObserver] for forward
// compatibility with new methods added to this interface.
type AuthnObserver interface {
	// Authenticate is called when a request enters the
	// authentication pipeline. The returned [AuthnProbe] tracks
	// the outcome.
	Authenticate(ctx context.Context, info AuthnRequestInfo) (context.Context, AuthnProbe)
}

// AuthnRequestInfo captures the request-level context available when
// authentication begins.
type AuthnRequestInfo struct {
	Method   string // API method being accessed
	PeerAddr string // client address
}

// AuthnProbe tracks a single authentication attempt.
// Implementations should embed [NoOpAuthnProbe] for forward
// compatibility.
type AuthnProbe interface {
	// MethodsLoaded is called after auth methods are fetched,
	// reporting how many are configured.
	MethodsLoaded(count int)

	// CredentialMissing is called when a configured auth method is
	// skipped because no matching credential was present in the
	// request (e.g. no bearer token for an OIDC method).
	CredentialMissing(methodType AuthMethodType)

	// VerifyingCredential is called just before a credential is
	// verified against a specific auth method.
	VerifyingCredential(methodID AuthMethodID, methodType AuthMethodType)

	// Authenticated is called when a credential was successfully
	// verified, identifying the subject and the method type used.
	Authenticated(methodType AuthMethodType, subject SubjectClaims)

	// Anonymous is called when no credential was presented or no
	// configured method matched.
	Anonymous()

	// Error is called when authentication fails (e.g. invalid token).
	Error(err error)

	// End signals the authentication attempt is complete (for timing).
	// Called via defer.
	End()
}

// NoOpAuthnObserver is an [AuthnObserver] that returns no-op probes.
type NoOpAuthnObserver struct{}

func (NoOpAuthnObserver) Authenticate(ctx context.Context, _ AuthnRequestInfo) (context.Context, AuthnProbe) {
	return ctx, NoOpAuthnProbe{}
}

// NoOpAuthnProbe is an [AuthnProbe] that discards all calls.
type NoOpAuthnProbe struct{}

func (NoOpAuthnProbe) MethodsLoaded(int)                                {}
func (NoOpAuthnProbe) CredentialMissing(AuthMethodType)                 {}
func (NoOpAuthnProbe) VerifyingCredential(AuthMethodID, AuthMethodType) {}
func (NoOpAuthnProbe) Authenticated(AuthMethodType, SubjectClaims)      {}
func (NoOpAuthnProbe) Anonymous()                                       {}
func (NoOpAuthnProbe) Error(error)                                      {}
func (NoOpAuthnProbe) End()                                             {}
