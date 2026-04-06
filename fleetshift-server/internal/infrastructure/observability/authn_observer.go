package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/fleetshift/fleetshift-poc/fleetshift-server/internal/domain"
)

// AuthnObserver is a [domain.AuthnObserver] that logs authentication
// lifecycle events via [slog].
type AuthnObserver struct {
	domain.NoOpAuthnObserver
	logger *slog.Logger
}

// NewAuthnObserver returns an AuthnObserver that logs to logger.
func NewAuthnObserver(logger *slog.Logger) *AuthnObserver {
	return &AuthnObserver{logger: logger.With("component", "authn")}
}

func (o *AuthnObserver) Authenticate(ctx context.Context, info domain.AuthnRequestInfo) (context.Context, domain.AuthnProbe) {
	return ctx, &authnProbe{
		logger:    o.logger,
		ctx:       ctx,
		startTime: time.Now(),
		method:    info.Method,
		peerAddr:  info.PeerAddr,
	}
}

type authnProbe struct {
	domain.NoOpAuthnProbe
	logger       *slog.Logger
	ctx          context.Context
	startTime    time.Time
	method       string
	peerAddr     string
	methodCount  int
	verifiedWith *verifiedMethod
	err          error
	outcome      authnOutcome
	subject      *domain.SubjectClaims
	authType     domain.AuthMethodType
}

type verifiedMethod struct {
	id         domain.AuthMethodID
	methodType domain.AuthMethodType
}

type authnOutcome int

const (
	authnOutcomeUnknown authnOutcome = iota
	authnOutcomeAuthenticated
	authnOutcomeAnonymous
)

func (p *authnProbe) MethodsLoaded(count int) {
	p.methodCount = count
	if p.logger.Enabled(p.ctx, slog.LevelDebug) {
		p.logger.LogAttrs(p.ctx, slog.LevelDebug, "auth methods loaded",
			slog.Int("count", count),
		)
	}
}

func (p *authnProbe) CredentialMissing(methodType domain.AuthMethodType) {
	if p.logger.Enabled(p.ctx, slog.LevelDebug) {
		p.logger.LogAttrs(p.ctx, slog.LevelDebug, "credential missing for method",
			slog.String("auth_method_type", string(methodType)),
		)
	}
}

func (p *authnProbe) VerifyingCredential(methodID domain.AuthMethodID, methodType domain.AuthMethodType) {
	p.verifiedWith = &verifiedMethod{id: methodID, methodType: methodType}
	if p.logger.Enabled(p.ctx, slog.LevelDebug) {
		p.logger.LogAttrs(p.ctx, slog.LevelDebug, "verifying credential",
			slog.String("auth_method_id", string(methodID)),
			slog.String("auth_method_type", string(methodType)),
		)
	}
}

func (p *authnProbe) Authenticated(methodType domain.AuthMethodType, subject domain.SubjectClaims) {
	p.outcome = authnOutcomeAuthenticated
	p.authType = methodType
	p.subject = &subject
}

func (p *authnProbe) Anonymous() {
	p.outcome = authnOutcomeAnonymous
}

func (p *authnProbe) Error(err error) {
	p.err = err
}

func (p *authnProbe) End() {
	duration := time.Since(p.startTime)
	attrs := []slog.Attr{
		slog.Duration("duration", duration),
		slog.String("method", p.method),
		slog.String("peer_addr", p.peerAddr),
		slog.Int("auth_methods_configured", p.methodCount),
	}

	if p.err != nil {
		if p.verifiedWith != nil {
			attrs = append(attrs,
				slog.String("auth_method_id", string(p.verifiedWith.id)),
				slog.String("auth_method_type", string(p.verifiedWith.methodType)),
			)
		}
		attrs = append(attrs, slog.String("error", p.err.Error()))
		p.logger.LogAttrs(p.ctx, slog.LevelWarn, "authentication failed", attrs...)
		return
	}

	if !p.logger.Enabled(p.ctx, slog.LevelInfo) {
		return
	}

	switch p.outcome {
	case authnOutcomeAuthenticated:
		attrs = append(attrs,
			slog.String("subject_id", string(p.subject.Subject)),
			slog.String("auth_method_type", string(p.authType)),
		)
		p.logger.LogAttrs(p.ctx, slog.LevelInfo, "authenticated", attrs...)
	case authnOutcomeAnonymous:
		p.logger.LogAttrs(p.ctx, slog.LevelInfo, "anonymous request", attrs...)
	}
}
